import { DynamodbError } from '@_/errors/DynamodbError'
import { convertToUnderscore } from '@_/helpers'
import {
    BatchWriteItemCommand,
    DescribeTableCommand,
    DynamoDBClient,
    DynamoDBClientConfig,
    GetItemCommand,
    GetItemCommandInput,
    GetItemCommandOutput,
    PutItemCommand,
    PutItemCommandOutput,
    UpdateItemCommand,
    UpdateItemCommandInput,
    UpdateItemCommandOutput,
} from '@aws-sdk/client-dynamodb'
import { marshall, marshallOptions, unmarshall } from '@aws-sdk/util-dynamodb'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import { merge, omit, pick } from 'lodash'

import { CompositeKeySchema, DynamoDbSchema } from './DynamoDbSchema'

/**
 * Represents a DynamoDB client for interacting with a DynamoDB table.
 */
export class Tynamo<PK extends string, SK extends string | undefined> {
    public client: DynamoDBClient

    tableName: string

    pk: string

    sk: string | undefined

    /**
     * Creates a new instance of the DynamoDBTable class.
     * @param tableName - The name of the DynamoDB table.
     * @param pk - The partition key of the table.
     * @param sk - The sort key of the table (optional).
     * @param options - The configuration options for the DynamoDB client (optional).
     */
    private constructor(
        tableName: string,
        pk: PK,
        sk?: SK,
        options?: DynamoDBClientConfig) {
        this.client = new DynamoDBClient(options || {
            requestHandler: new NodeHttpHandler({
                requestTimeout: 1000,
                connectionTimeout: 2000,
            }),
            maxAttempts: 6,
            retryMode: 'adaptive',
        })
        this.tableName = tableName
        this.pk = pk
        this.sk = sk
    }

    /**
     * Creates a new instance of the DynamoDB class with both a partition key and a sort key.
     * This is useful for tables that have both a partition key and a sort key.
     * 
     * @param tableName - The name of the DynamoDB table.
     * @param pk - The partition key of the table.
     * @param sk - The sort key of the table.
     * @param options - The configuration options for the DynamoDB client (optional).
     * @returns A new instance of the DynamoDB class.
     */
    static create<A extends string, B extends string>(tableName: string, pk: A, sk: B, options?: DynamoDBClientConfig) {
        return new Tynamo<A, B>(tableName, pk, sk, options)
    }

    /**
     * Creates a new instance of the DynamoDB class with only a partition key.
     * This is useful for tables that do not have a sort key.
     * 
     * @param tableName - The name of the DynamoDB table.
     * @param pk - The partition key of the table.
     * @param options - The configuration options for the DynamoDB client (optional).
     * @returns A new instance of the DynamoDB class.
     */
    static createOnlyPk<A extends string>(tableName: string, pk: A, options?: DynamoDBClientConfig) {
        return new Tynamo<A, undefined>(tableName, pk, undefined, options)
    }

    /**
     * Sends a command to the client and handles any errors.
     * 
     * @template T - The type of the command.
     * @param {T} command - The command to send.
     * @param {string} operation - The name of the operation being performed.
     * @returns {Promise<any>} - A promise that resolves to the result of the command.
     * @throws {DynamodbError} - If an error occurs while sending the command.
     */
    private async send(command: any, operation: string) {
        try {
            const resp = await this.client.send(command)
            return resp
        } catch (error) {
            const orig = error as unknown as Error
            throw new DynamodbError(orig)
        }
    }

    /**
     * Puts an item into the DynamoDB table.
     * @param record The item to be inserted.
     * @returns A promise that resolves to the result of the PutItem operation.
     */
    async putRecord<T extends CompositeKeySchema<PK, SK>>(record: T, options?: { marshallOptions?: marshallOptions }) {
        return this.send(
            new PutItemCommand({
                TableName: this.tableName,
                Item: marshall(record, options?.marshallOptions),
            }),
            'PutItem',
        ) as Promise<PutItemCommandOutput>
    }

    /**
     * Retrieves an item from the DynamoDB table based on the provided primary key (pk) and optional sort key (sk).
     * @param pk The primary key value.
     * @param sk The optional sort key value.
     * @returns A promise that resolves to the retrieved item.
     */
    async getRecord(pk: string, sk?: string): Promise<CompositeKeySchema<PK, SK> | null> {
        const keys = { [this.pk]: pk } as DynamoDbSchema
        if (this.sk && sk) keys[this.sk] = sk

        const resp = await this.send(
            new GetItemCommand({
                TableName: this.tableName,
                Key: this.keys(keys),
            }),
            'GetItem',
        ) as GetItemCommandOutput
        if (resp.Item) {
            return unmarshall(resp.Item)
        }
        return null
    }

    /**
     * Retrieves the description of the table.
     * @returns {Promise<DescribeTableOutput>} The description of the table.
     */
    async describeTable() {
        // return the table descirbe
        return this.send(
            new DescribeTableCommand({
                TableName: this.tableName,
            }),
            'DescribeTable',
        )
    }

    /**
     * Generates the keys for a DynamoDB record.
     * 
     * @param record - The DynamoDB record.
     * @returns The keys for the record.
     */
    private keys(record: CompositeKeySchema<PK, SK>) {
        const keys: DynamoDbSchema = {
            [this.pk]: record[this.pk],
        }
        if (this.sk && record[this.sk]) {
            keys[this.sk] = record[this.sk]
        }
        return marshall(keys)
    }

    async batchWriteRecord(records: CompositeKeySchema<PK, SK>[], options?: { marshallOptions?: marshallOptions }) {
        // distribute records into chunks of 25
        const chunks = []
        while (records.length > 0) {
            chunks.push(records.splice(0, 25))
        }
        const promises = chunks.map(chunk => this.send(new BatchWriteItemCommand({
            RequestItems: {
                [this.tableName]: chunk.map(p => ({
                    PutRequest: {
                        Item: marshall(p, options?.marshallOptions),
                    },
                })),
            },
        }), 'BatchWriteRecord'))
        return Promise.all(promises)
    }

    /**
     * Deletes multiple records from the DynamoDB table using batch write operation.
     * @param records - An array of composite key schemas representing the records to be deleted.
     * @returns A promise that resolves when the batch delete operation is complete.
     */
    async batchDeleteRecord(records: CompositeKeySchema<PK, SK>[]) {
        const chunks = []
        while (records.length > 0) {
            chunks.push(records.splice(0, 25))
        }
        const promises = chunks.map(chunk => this.send(new BatchWriteItemCommand({
            RequestItems: {
                [this.tableName]: chunk.map(p => ({
                    DeleteRequest: {
                        Key: this.keys(p),
                    },
                })),
            },
        }), 'BatchDeleteRecord'))

        return Promise.all(promises)
    }

    private async updateItem(params: UpdateItemCommandInput) {

        if (params.ExpressionAttributeNames) {
            params.ConditionExpression = `attribute_exists(#${this.pk})`
            params.ExpressionAttributeNames[`#${this.pk}`] = this.pk
            if (this.sk) {
                params.ExpressionAttributeNames[`#${this.sk}`] = this.sk
                params.ConditionExpression += ` and attribute_exists(#${this.sk})`
            }
        }
        const command = new UpdateItemCommand(params)
        return this.send(command, 'UpdateItemError') as Promise<UpdateItemCommandOutput>
    }

    /**
     * Updates the nested attributes of a record in a single request.
     * 
     * @template T - The type of the record that extends DynamoDbSchema.
     * @param {T} record - The record to update.
     * @returns {Promise<void>} - A promise that resolves when the update is complete.
     */
    async updateRecordNested(record: CompositeKeySchema<PK, SK>, options?: { marshallOptions?: marshallOptions }) {
        const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
        } = this.generateUpdateExpression({
            _marshallOptions: { removeUndefinedValues: true, ...options?.marshallOptions },
            exclude: [],
            include: true,
            record,
        })
        try {
            const response = await this.updateItem({
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues,
            })
            return response
        }
        catch (error) {
            const origErr = error as unknown as Error
            if (Tynamo.isNestedError(origErr)) {
                console.warn(
                    'Tynamo::UpdateNestedError::UnMatchedSchema::FetchingOriginalRecord',
                    pick(record, [this.pk, this.sk ?? '']),
                )
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, true, [])
                return this.putRecord(mergedRecord, { marshallOptions: options?.marshallOptions })
            }
            if (Tynamo.isUpdateError(origErr)) {
                console.warn('Tynamo::UpdateError::RecordNotFound', pick(record, [this.pk, this.sk ?? '']))
                return undefined
            }
            throw new DynamodbError(origErr)
        }
    }

    async upsertRecordNested(record: CompositeKeySchema<PK, SK>, options?: { marshallOptions?: marshallOptions }) {
        const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
        } = this.generateUpdateExpression({
            _marshallOptions: { removeUndefinedValues: true, ...options?.marshallOptions },
            exclude: [],
            include: true,
            record,
        })

        try {
            const resp = await this.updateItem({
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues,
                ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
            })
            return resp
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            console.warn(origErr.name, origErr.message)
            if (Tynamo.isUpdateError(origErr)) {
                console.warn('Tynamo::UpsertError::RecordNotFound::Inserting', pick(record, [this.pk, this.sk ?? '']))
                return this.putRecord(record, { marshallOptions: options?.marshallOptions })
            }
            if (Tynamo.isNestedError(origErr)) {
                console.warn(
                    'Tynamo::UpsertNestedError::UnMatchedSchema::FetchingOriginalRecord',
                    pick(record, [this.pk, this.sk ?? '']),
                )
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, true, [])
                return this.putRecord(mergedRecord, { marshallOptions: options?.marshallOptions })
            }
            throw error
        }
    }

    async upsertRecordIncluding(
        record: CompositeKeySchema<PK, SK>,
        include: string[],
        options?: { marshallOptions?: marshallOptions },
    ) {
        const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
        } = this.generateUpdateExpression({
            _marshallOptions: { removeUndefinedValues: true, ...options?.marshallOptions },
            exclude: [],
            include,
            record,
        })
        let conditionalExpression = `attribute_exists(#${this.pk})`
        ExpressionAttributeNames[`#${this.pk}`] = this.pk
        if (this.sk) {
            conditionalExpression += ` and attribute_exists(#${this.sk})`
            ExpressionAttributeNames[`#${this.sk}`] = this.sk
        }

        try {
            const resp = await this.updateItem({
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ConditionExpression: conditionalExpression,
                ExpressionAttributeValues,
                ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
            })
            return resp
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            if (origErr.name === 'ConditionalCheckFailedException') {
                console.warn('Tynamo::UpsertError::RecordNotFound::Inserting', pick(record, [this.pk, this.sk ?? '']))
                return this.putRecord(record, { marshallOptions: options?.marshallOptions })
            }
            if (Tynamo.isNestedError(origErr)) {
                console.warn(
                    'Tynamo::UpdateNestedError::UnMatchedSchema::FetchingOriginalRecord',
                    pick(record, [this.pk, this.sk ?? '']),
                )
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, true, [])
                return this.putRecord(mergedRecord, { marshallOptions: options?.marshallOptions })
            }
            throw error
        }
    }

    /**
     * Updates a record in the DynamoDB table, excluding specified attributes.
     * 
     * @template T - The type of the record being updated.
     * @param {T} record - The record to be updated.
     * @param {string[]} exclude - The attributes to be excluded from the update.
     * @returns {Promise<void>} - A promise that resolves when the update is complete.
     */
    async updateRecordExcluding(
        record: CompositeKeySchema<PK, SK>, exclude: string[], options?: {
            marshallOptions?: marshallOptions
        }) {
        const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
        } = this.generateUpdateExpression({
            _marshallOptions: { removeUndefinedValues: true, ...options?.marshallOptions },
            exclude,
            include: true,
            record,
        })
        try {
            const resp = this.updateItem({
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues,
            })
            return resp
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            if (Tynamo.isNestedError(origErr)) {
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, true, exclude)
                return this.updateItem(mergedRecord)
            }
            if (Tynamo.isUpdateError(origErr)) {
                console.warn('Tynamo::UpdateError::RecordNotFound', pick(record, [this.pk, this.sk ?? '']))
                return undefined
            }
            throw error
        }
    }

    /**
     * Updates a record in the DynamoDB table, including specified attributes.
     * 
     * @template T - The type of the record being updated.
     * @param {T} record - The record to be updated.
     * @param {string[]} include - The list of attributes to include in the update.
     * @returns {Promise<void>} - A promise that resolves when the update is complete.
     */
    async updateRecordIncluding(
        record: CompositeKeySchema<PK, SK>,
        include: string[],
        options?: { marshallOptions?: marshallOptions },
    ) {
        const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
        } = this.generateUpdateExpression({
            _marshallOptions: { removeUndefinedValues: true, ...options?.marshallOptions },
            record,
            include,
            exclude: [],
        })
        try {
            return this.updateItem({
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues,
            })
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            if (Tynamo.isNestedError(origErr)) {
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, include, [])
                return this.updateItem(mergedRecord)
            } if (Tynamo.isUpdateError(origErr)) {
                console.warn('Tynamo::UpdateError::RecordNotFound', pick(record, [this.pk, this.sk ?? '']))
                return undefined
            }
            throw error
        }
    }

    static isNestedRecord(value: unknown): value is DynamoDbSchema {
        if (value && typeof value === 'object' && Object.keys(value).length > 0) {
            return true
        }
        return false
    }

    static isNestedError(err: Error): boolean {
        return err.name === 'ValidationException' &&
            (
                err.message.includes('path provided in the update expression is invalid for update') ||
                err.message.includes('Invalid UpdateExpression')
            )
    }

    static isUpdateError(err: Error): boolean {
        return err.name === 'ConditionalCheckFailedException'
    }

    /**
     * Generates an update expression, expression attribute names, and expression attribute values
     * for updating a DynamoDB record.
     * 
     * @param record - The DynamoDB record to update.
     * @param include - Determines which attributes to include in the update expression.
     *         If `true`, includes all attributes. If an array of strings, includes only the specified attributes.
     * @param exclude - An array of attribute names to exclude from the update expression.
     * 
     * @returns An object containing  UpdateExpression, ExpressionAttributeNames, and ExpressionAttributeValues.
     */
    private generateUpdateExpression(
        { record, include, exclude, _marshallOptions }: {
            _marshallOptions?: marshallOptions;
            exclude: string[];
            include: boolean | string[];
            record: CompositeKeySchema<PK, SK>;
        },
    ) {
        const expressionAttributeNames: any = {}
        const expressionAttributeValues: any = {}
        const updateExpressions: string[] = []

        const tagify = (key: string) => `${key.split('.').map(k => `#${convertToUnderscore(k)}`).join('.')}`
        const traverseAttributes = (attributes: CompositeKeySchema<PK, SK>, parentKey?: string) => {
            for (const [key, val] of Object.entries(attributes)) {
                const currentKey = parentKey ? `${parentKey}.${key}` : key
                const recursive = Tynamo.isNestedRecord(val)
                if (
                    currentKey === this.pk ||
                    currentKey === this.sk ||
                    val === undefined ||
                    exclude.includes(currentKey) ||
                    (include === false || (Array.isArray(include) && !include.includes(currentKey)) && !recursive)
                ) {
                    // eslint-disable-next-line no-continue
                    continue
                }
                if (recursive) {
                    traverseAttributes(val, currentKey)
                } else {
                    const placeholder = tagify(currentKey)
                    const valuePlaceholder = `:value${Object.keys(expressionAttributeValues).length + 1}`

                    currentKey.split('.').forEach(k => { expressionAttributeNames[`#${convertToUnderscore(k)}`] = k })

                    expressionAttributeValues[valuePlaceholder] = val
                    updateExpressions.push(`${placeholder} = ${valuePlaceholder}`)
                }
            }
        }
        traverseAttributes(record)

        const updateExpression = `SET ${updateExpressions.join(', ')}`

        return {
            UpdateExpression: updateExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: marshall(expressionAttributeValues, _marshallOptions),
        }
    }

    /**
     * Merges two records together based on the specified criteria.
     * 
     * @param newRecord - The new record to merge.
     * @param oldRecord - The old record to merge.
     * @param include - Determines which properties to include in the merged record. 
     *                  If `true`, all properties are included. 
     *                  If an array of strings, only the specified properties are included.
     *                  Default is `true`.
     * @param exclude - An array of properties to exclude from the merged record. 
     *                  Default is an empty array.
     * @returns The merged record.
    */
    // eslint-disable-next-line class-methods-use-this
    private mergeRecords(
        newRecord: CompositeKeySchema<PK, SK>,
        oldRecord: CompositeKeySchema<PK, SK>,
        include: boolean | string[] = true,
        exclude: string[] = [],
    ): CompositeKeySchema<PK, SK> {
        const excludedNewReocrd = omit(newRecord, exclude)
        if (include === true) return merge(oldRecord, excludedNewReocrd) as CompositeKeySchema<PK, SK>
        if (Array.isArray(include)) {
            const includedNewRecord = pick(newRecord, excludedNewReocrd)
            return merge(oldRecord, includedNewRecord) as CompositeKeySchema<PK, SK>
        }
        return oldRecord
    }
}
