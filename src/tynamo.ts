import { DynamodbError } from '@_/errors/DynamodbError'
import { convertToUnderscore } from '@_/helpers'
import { Logger, LogLevel, LogName } from '@_/logger'
import {
    AttributeValue,
    BatchGetItemCommand,
    BatchGetItemCommandOutput,
    BatchWriteItemCommand,
    ConditionalCheckFailedException,
    DescribeTableCommand,
    DynamoDBClient,
    DynamoDBClientConfig,
    GetItemCommand,
    GetItemCommandOutput,
    PutItemCommand,
    PutItemCommandOutput,
    ReturnValuesOnConditionCheckFailure,
    UpdateItemCommand,
    UpdateItemCommandInput,
    UpdateItemCommandOutput,
} from '@aws-sdk/client-dynamodb'
import { marshall, marshallOptions, unmarshall } from '@aws-sdk/util-dynamodb'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import { merge, omit, pick } from 'lodash'

import { CompositeKeySchema, DynamoDbSchema } from './DynamoDbSchema'

/**
 * Options for configuring the Tynamo client.
 */
interface TynamoOptions extends DynamoDBClientConfig {
    /**
     * The log level for logging. Default is 'INFO'.
     */
    logLevel?: LogName
}

/**
 * Represents a DynamoDB client for interacting with a DynamoDB table.
 */
export class Tynamo<PK extends string, SK extends string | undefined> {
    private client: DynamoDBClient

    private logger: Logger

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
        options?: TynamoOptions) {
        this.client = new DynamoDBClient(options || {
            requestHandler: new NodeHttpHandler({
                requestTimeout: 1000,
                connectionTimeout: 2000,
            }),
            maxAttempts: 6,
            retryMode: 'adaptive',
        })
        if (options?.logLevel) {
            this.logger = new Logger(options.logLevel)
        } else {
            this.logger = new Logger('INFO')
        }
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
    static create<A extends string, B extends string>(tableName: string, pk: A, sk: B, options?: TynamoOptions) {
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
    static createOnlyPk<A extends string>(tableName: string, pk: A, options?: TynamoOptions) {
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
        for (let i = 0; i < records.length; i += 25) {
            chunks.push(records.slice(i, i + 25))
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

    async batchGetRecord(records: CompositeKeySchema<PK, SK>[]): Promise<CompositeKeySchema<PK, SK>[]> {
        const chunks = []
        if (records.length === 0) return []
        for (let i = 0; i < records.length; i += 100) {
            chunks.push(records.slice(i, i + 100))
        }
        const promises = chunks.map(chunk => this.send(new BatchGetItemCommand({
            RequestItems: {
                [this.tableName]: { Keys: chunk.map(p => this.keys(p)) },
            },
        }), 'BatchGetRecord')) as Promise<BatchGetItemCommandOutput>[]
        const responses = await Promise.all(promises)
        const results: Record<string, unknown>[] = []
        for (const response of responses) {
            results.push(
                ...(response.Responses ?
                    response.Responses[this.tableName]?.map(item => unmarshall(item)) || [] :
                    []),
            )
        }
        return results
    }

    /**
     * Deletes multiple records from the DynamoDB table using batch write operation.
     * @param records - An array of composite key schemas representing the records to be deleted.
     * @returns A promise that resolves when the batch delete operation is complete.
     */
    async batchDeleteRecord(records: CompositeKeySchema<PK, SK>[]) {
        const chunks = []
        for (let i = 0; i < records.length; i += 25) {
            chunks.push(records.slice(i, i + 25))
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

    private async updateItem(params: UpdateItemCommandInput, insert = false) {

        if (params.ExpressionAttributeNames) {
            // eslint-disable-next-line max-len
            params.ConditionExpression = `${params.ConditionExpression ? `${params.ConditionExpression} and ` : ''}attribute_exists(#${this.pk})`
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
    async updateRecordNested(record: CompositeKeySchema<PK, SK>, options?: {
        marshallOptions?: marshallOptions,
        conditionExpression?: string,
        expressionAttributeValues?: Record<string, AttributeValue>,
    }) {
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
            const params = {
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues,
                ConditionExpression: options?.conditionExpression,
            }
            if (options?.conditionExpression) {
                const { attributeNames, attributeValues } = Tynamo.parseExpression(
                    options.conditionExpression,
                    options.expressionAttributeValues,
                )
                params.ExpressionAttributeNames = { ...params.ExpressionAttributeNames, ...attributeNames }
                params.ExpressionAttributeValues = { ...params.ExpressionAttributeValues, ...attributeValues }
            }
            const response = await this.updateItem(params)
            return response
        }
        catch (error) {
            const origErr = (error as DynamodbError)?.original as Error
            if (Tynamo.isUpdateError(origErr)) {
                this.logger.log(
                    LogLevel.WARN,
                    'Tynamo::UpdateNestedError::UnMatchedSchema::FetchingOriginalRecord',
                    pick(record, [this.pk, this.sk ?? '']),
                )
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, true, [])
                return this.putRecord(mergedRecord, { marshallOptions: options?.marshallOptions })
            }
            if (Tynamo.isCheckError(origErr)) {
                this.logger.log(
                    LogLevel.WARN,
                    'Tynamo::ConditionCheck::Failed',
                    pick(record, [this.pk, this.sk ?? '']),
                )
                return undefined
            }
            throw new DynamodbError(origErr)
        }
    }

    /**
     * Updates a nested record in DynamoDB. If the update fails due to schema mismatch,
     * it will fetch the original record, merge the changes, and put the merged record.
     * 
     * @param record - The record to update with nested attributes
     * @param options - Optional parameters
     * @param options.marshallOptions - Options for marshalling the DynamoDB record - same like dynamodb package
     * @param options.insertOnly - List of attribute keys that should only be inserted, not updated.
     *                            For nested keys, use dot notation (e.g. "data.created_at").
     * @param options.conditionExpression - A condition expression to check before updating
     * @param options.expressionAttributeValues - Additional Attribute Values for any condition expression variables
     * @returns The updated record if successful, undefined if condition check fails
     * @throws {DynamodbError} If the update fails for reasons other than schema mismatch
     */
    async upsertRecordNested(record: CompositeKeySchema<PK, SK>, options?: {
        marshallOptions?: marshallOptions,
        conditionExpression?: string,
        expressionAttributeValues?: Record<string, AttributeValue>,
        insertOnly?: string[],
    }) {
        const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
        } = this.generateUpdateExpression({
            _marshallOptions: { removeUndefinedValues: true, ...options?.marshallOptions },
            exclude: [],
            include: true,
            insertOnly: options?.insertOnly,
            record,
        })

        try {
            const params: UpdateItemCommandInput = {
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues,
                ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure.ALL_OLD,
            }
            if (options?.conditionExpression) {
                const { attributeNames, attributeValues } = Tynamo.parseExpression(
                    options.conditionExpression,
                    options.expressionAttributeValues,
                )
                params.ConditionExpression = options.conditionExpression
                params.ExpressionAttributeNames = { ...params.ExpressionAttributeNames, ...attributeNames }
                params.ExpressionAttributeValues = { ...params.ExpressionAttributeValues, ...attributeValues }
            }
            const resp = await this.updateItem(params, true)
            return resp
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            if (Tynamo.isCheckError(origErr)) {
                const oldRecord = (origErr.original as ConditionalCheckFailedException)?.Item
                if (oldRecord && options?.conditionExpression) {
                    this.logger.log(
                        LogLevel.WARN,
                        'Tynamo::ConditionCheck::Failed', pick(record, [this.pk, this.sk ?? '']),
                    )
                    return undefined
                }
                if (!oldRecord) {
                    this.logger.log(
                        LogLevel.WARN,
                        'Tynamo::ConditionCheck::RecordNotFound::Inserting', pick(record, [this.pk, this.sk ?? '']),
                    )
                    return this.putRecord(record, { marshallOptions: options?.marshallOptions })
                }
            }
            if (Tynamo.isUpdateError(origErr)) {
                this.logger.log(
                    LogLevel.WARN,
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
            this.logger.log(LogLevel.INFO, 'Tynamo::UpsertRecordIncluding', resp)
            return resp
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            if (Tynamo.isCheckError(origErr)) {
                this.logger.log(
                    LogLevel.WARN,
                    'Tynamo::UpsertError::RecordNotFound::Inserting', pick(record, [this.pk, this.sk ?? '']),
                )
                return this.putRecord(record, { marshallOptions: options?.marshallOptions })
            }
            if (Tynamo.isUpdateError(origErr)) {
                this.logger.log(
                    LogLevel.WARN,
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
            this.logger.log(LogLevel.INFO, 'Tynamo::UpdateRecordExcluding', pick(record, [this.pk, this.sk ?? '']))
            return resp
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            if (Tynamo.isUpdateError(origErr)) {
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, true, exclude)
                return this.updateItem(mergedRecord)
            }
            if (Tynamo.isCheckError(origErr)) {
                this.logger.log(
                    LogLevel.WARN,
                    'Tynamo::UpdateError::RecordNotFound', pick(record, [this.pk, this.sk ?? '']))
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
            const resp = this.updateItem({
                TableName: this.tableName,
                Key: this.keys(record),
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues,
            })
            this.logger.log(LogLevel.INFO, 'Tynamo::UpdateRecordIncluding', resp)
            return resp
        } catch (error) {
            const origErr = error as unknown as DynamodbError
            if (Tynamo.isUpdateError(origErr)) {
                const originalRecord = await this.getRecord(record[this.pk], this.sk && record[this.sk])
                const mergedRecord = this.mergeRecords(record, originalRecord, include, [])
                return this.updateItem(mergedRecord)
            } if (Tynamo.isCheckError(origErr)) {
                this.logger.log(
                    LogLevel.WARN,
                    'Tynamo::UpdateError::RecordNotFound', pick(record, [this.pk, this.sk ?? '']),
                )
                return undefined
            }
            throw error
        }
    }

    static isNestedRecord(value: unknown): value is DynamoDbSchema {
        if (value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length > 0) {
            return true
        }
        return false
    }

    static isUpdateError(err: Error): boolean {
        return err.name === 'ValidationException' &&
            (
                err.message.includes('path provided in the update expression is invalid for update') ||
                err.message.includes('Invalid UpdateExpression')
            )
    }

    static isCheckError(err: Error): err is ConditionalCheckFailedException {
        return err.name === 'ConditionalCheckFailedException'
    }

    /**
     * a helper function to parse dynamodb expression and extract the name and value
     * 
     * Expression variable should start with '#' and expression value should start with ':'
     * @param expr dynamodb expression like ConditionExpression, FilterExpression, etc.
     * 
     * @example
     * ```ts
     * import { Tynamo } from '@_/tynamo'
     * import { AttributeValue } from '@aws-sdk/client-dynamodb'
     * const start = new Date().getTime();
     * let res = Tynamo.parseExpression(
     *  '#name = :name and #data.#value.#sub_val = :value and size   (#users ) > :size and begins_with(#aaa, :dd)',
     * {
     *      ':name': {S: 'Ahmed'},
     *      ':value': { S: 'Ahmed'},
     *      ':size': {N: '10'},
     *      ':dd': {S: 'Ahmed'},
     * }
     * )
     * let elapsed = new Date().getTime() - start;
     * console.log(`Elapsed Time: ${elapsed} milliseconds`)
     * console.log(`Result: `,res) 
     * expect(res.attributeNames['#data']).toBe('data')
     * expect(res.attributeNames['#value']).toBe('value')
     * expect(res.attributeNames['#sub_val']).toBe('sub_val')
     * expect(res.attributeNames['#users']).toBe('users')
     * ```
     */
    static parseExpression(expr: string, attributeValues?: Record<string, AttributeValue>): {
        attributeNames: Record<string, string>,
        attributeValues: Record<string, AttributeValue>
    } {
        const result: {
            attributeNames: Record<string, string>,
            attributeValues: Record<string, AttributeValue>
        } = {
            attributeNames: {},
            attributeValues: {},
        }

        const sAttributeName = '(?<attributeName>(#\\w+(\\.#\\w+)*))'
        const sAttributeValue = '(:(?<attributeValue>\\w+))'
        const regexes = [
            RegExp(`${sAttributeName}\\s*(=|>|<|>=|<=|<>)\\s*${sAttributeValue}`, 'g'),
            RegExp(`size\\s*\\(\\s*${sAttributeName}\\s*\\)\\s*(=|>|<|>=|<=|<>)\\s*${sAttributeValue}`, 'g'),
            // eslint-disable-next-line max-len
            RegExp(`(begins_with|attribute_exists|attribute_not_exists|attribute_type|contains)\\s*\\(\\s*${sAttributeName}\\s*(,(\\s*${sAttributeValue}\\s*))?\\)`, 'g'),
        ]

        for (const regex of regexes) {
            const matches = expr.matchAll(regex)
            for (const match of matches) {

                const { attributeName, attributeValue } = match.groups || {}
                if (attributeName) {
                    const attributeNameParts = [...attributeName.matchAll(/\w+/g)]
                    for (const part of attributeNameParts) {
                        // eslint-disable-next-line prefer-destructuring
                        result.attributeNames[`#${part[0]}`] = part[0]
                    }
                }
                if (attributeValue) {
                    if (attributeValues && attributeValues[`:${attributeValue}`]) {
                        const key = `:${attributeValue}` as string
                        result.attributeValues[key] = attributeValues[key] as AttributeValue
                    }
                    else {
                        throw new DynamodbError(
                            new Error(`\n":${attributeValue}" value not found in expression 👇\n"${expr}"\n`),
                        )
                    }
                }
            }
        }
        return result
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
        { record, include, exclude, _marshallOptions, insertOnly }: {
            _marshallOptions?: marshallOptions;
            exclude: string[];
            include: boolean | string[];
            record: CompositeKeySchema<PK, SK>;
            insertOnly?: string[];
        },
    ) {
        const expressionAttributeNames: Record<string, string> = {}
        const expressionAttributeValues: Record<string, AttributeValue> = {}
        const updateExpressions: string[] = []
        const insertKeys = new Set(insertOnly)
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
                    const valueKey = `:value${Object.keys(expressionAttributeValues).length + 1}`
                    const valuePlaceholder = insertKeys.has(currentKey) ? `if_not_exists(${placeholder}, ${valueKey})` : valueKey

                    currentKey.split('.').forEach(k => { expressionAttributeNames[`#${convertToUnderscore(k)}`] = k })

                    expressionAttributeValues[valueKey] = val
                    updateExpressions.push(`${placeholder} = ${valuePlaceholder}`)
                }
            }
        }
        traverseAttributes(record)
        const updateExpression = `SET ${updateExpressions.join(', ')}`
        const result = {
            UpdateExpression: updateExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: marshall(expressionAttributeValues, _marshallOptions),
        }
        this.logger.log(
            LogLevel.DEBUG,
            'Tynamo::generateUpdateExpression',
            result,
        )
        return result
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
