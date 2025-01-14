// import { config } from 'aws-sdk'
import { DynamoDbSchema } from '@_/DynamoDbSchema'
import { Tynamo } from '@_/tynamo'
// eslint-disable-next-line import/no-extraneous-dependencies
import { faker } from '@faker-js/faker'

import { DynamodbRecordFactory } from './factories/dynamodbRecordFactory'

// config.update({
//     region: 'us-east-1',
// })

describe('DynamoDB', () => {
    const record = DynamodbRecordFactory.create()
    const dynamodb = Tynamo.create('datalake', 'uuid', 'record_id', {
        logLevel: 'DEBUG',
        region: 'us-east-1',
        endpoint: 'http://localhost:8000',
        credentials: {
            accessKeyId: 'root',
            secretAccessKey: 'root',
        },
    })
    beforeAll(async () => {
        await dynamodb.putRecord(record)
    })
    test('should create a new record', async () => {
        const resp = await dynamodb.getRecord(record.uuid as string, record.record_id as string)
        expect(resp).toBeDefined()
    })
    test('update a record with nested attributes', async () => {

        const updatedRecord = { ...record }
        const fakeName = 'Patricia'
        if (typeof record.data == 'object') {
            updatedRecord.data = {
                ...record.data,
                firstname: fakeName,
                lastname: 'Ponce',
            }
        }

        await dynamodb.updateRecordIncluding(updatedRecord, ['data.firstname', 'data.lastname'])
        const resp = await dynamodb.getRecord(record.uuid as string, record.record_id as string)
        expect((resp?.data as DynamoDbSchema).firstname).toBe(fakeName)
    })

    test('upsert a record', async () => {

        const updatedRecord = { ...record, uuid: faker.string.uuid() }

        await dynamodb.upsertRecordNested(updatedRecord)
        const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
        expect(resp).toBeDefined()
    })

    describe('generateUpdateExpression', () => {
        test('when record does not exists yet, do update created_at', async () => {
            const uuid = faker.string.uuid()
            const updatedRecord = { uuid, record_id: `${uuid}_user`, created_at: '2023-10-01' }
            await dynamodb.upsertRecordNested(updatedRecord)
        
            const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
            expect(resp.created_at).toBe('2023-10-01')
        })

        test('when record exists, do not update created_at', async () => {
            const uuid = faker.string.uuid()
            const updatedRecord = { uuid, record_id: `${uuid}_user`, data: {created_at: '2023-10-01'} }
            await dynamodb.putRecord(updatedRecord)

            await dynamodb.upsertRecordNested({...updatedRecord, data: {created_at: '2023-10-02'}}, {
                insertOnly: ['data.created_at']
            })
        
            const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
            expect(resp.data?.created_at).toBe('2023-10-01')
        })
    })

    test('update not found record', async () => {
        const uuid = faker.string.uuid()
        const updatedRecord = {
            uuid,
            record_id: `${uuid}_test`,
            updated_at: faker.date.recent().toISOString(),
            data: {
                last_active_at: faker.date.recent().toISOString(),
                event_data: {
                    user_added: faker.string.uuid(),
                },

            },
            version: faker.number.int(),
        }

        await dynamodb.updateRecordNested(updatedRecord)
        const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
        console.debug(JSON.stringify(resp))
        expect(resp).toBeFalsy()
    })

    test('upsert a record 2', async () => {

        const updatedRecord = {
            uuid: faker.string.uuid(),
            record_id: faker.string.uuid(),
            domain: 'marketing',
            meta: faker.lorem.sentence(),
            created_at: faker.date.past().toISOString(),
            updated_at: faker.date.recent().toISOString(),
            targets: new Set(['emarsys']),
            data: {
                baag_purchased_at: faker.date.past().toISOString(),
                event_data: {},
                team_name: faker.person.jobType(),
                service_name: faker.internet.domainName(),
            },
            version: 4,
        }

        await dynamodb.upsertRecordNested(updatedRecord, { marshallOptions: { convertEmptyValues: false } })
        const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
        expect(resp).toBeDefined()
    })

    test('upsert a record with nested level 3', async () => {

        const updatedRecord = {
            uuid: record.uuid,
            record_id: record.record_id,
            data: {
                event_data: {
                    'account_created': '3e2e3d894dea368a1040445163c01387',
                },
                newPath: {
                    thrdLevel: {
                        frthLevel: faker.string.uuid(),
                        frthLevel_2: faker.string.uuid(),
                        frthLevel_3: faker.string.uuid(),
                    },
                },
            },
        }

        await dynamodb.upsertRecordNested(updatedRecord)
        const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
        expect(resp).toBeDefined()
        expect(resp.data?.event_data['user:created']).toEqual(record.data.event_data['user:created'])
        expect(resp.data?.newPath?.thrdLevel?.frthLevel).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel)
        expect(resp.data?.newPath?.thrdLevel?.frthLevel_2).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel_2)
        expect(resp.data?.newPath?.thrdLevel?.frthLevel_3).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel_3)
    })

    test('upsert exisiting record on condition not met', async () => {

        const updatedRecord = {
            uuid: record.uuid,
            record_id: record.record_id,
            data: {
                active_at: '2023-10-05',
                event_data: {
                    'account_created': '3e2e3d894dea368a1040445163c01387',
                },
                newPath: {
                    thrdLevel: {
                        frthLevel: faker.string.uuid(),
                        frthLevel_2: faker.string.uuid(),
                        frthLevel_3: faker.string.uuid(),
                    },
                },
            },
        }

        await dynamodb.upsertRecordNested(updatedRecord, {
            conditionExpression: 'attribute_not_exists(#data.#active_at) or #data.#active_at > :active_at',
            expressionAttributeValues: {
                ':active_at': { 'S': '2023-10-05' },
            },
        })
        const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
        console.log(JSON.stringify(resp))
        expect(resp).toBeDefined()
        expect(resp.data?.active_at).toBe('2023-10-01')
        expect(resp.data?.event_data['user:created']).toEqual(record.data.event_data['user:created'])
    })
    test('upsert exisiting record on condition met', async () => {

        const updatedRecord = {
            uuid: record.uuid,
            record_id: record.record_id,
            data: {
                active_at: '2023-10-05',
                event_data: {
                    'account_created': '3e2e3d894dea368a1040445163c01387',
                },
                newPath: {
                    thrdLevel: {
                        frthLevel: faker.string.uuid(),
                        frthLevel_2: faker.string.uuid(),
                        frthLevel_3: faker.string.uuid(),
                    },
                },
            },
        }

        await dynamodb.upsertRecordNested(updatedRecord, {
            conditionExpression: 'attribute_not_exists(#data.#active_at) or #data.#active_at < :active_at',
            expressionAttributeValues: {
                ':active_at': { 'S': '2023-10-05' },
            },
        })
        const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
        console.log(JSON.stringify(resp))
        expect(resp).toBeDefined()
        expect(resp.data?.active_at).toBe('2023-10-05')
        expect(resp.data?.event_data['user:created']).toEqual(record.data.event_data['user:created'])
        expect(resp.data?.newPath?.thrdLevel?.frthLevel).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel)
        expect(resp.data?.newPath?.thrdLevel?.frthLevel_2).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel_2)
        expect(resp.data?.newPath?.thrdLevel?.frthLevel_3).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel_3)
    })
    test('upsert a new record with condition', async () => {

        const updatedRecord = {
            uuid: faker.string.uuid(),
            record_id: record.record_id,
            data: {
                active_at: '2023-10-05',
                event_data: {
                    'account_created': '3e2e3d894dea368a1040445163c01387',
                },
                newPath: {
                    thrdLevel: {
                        frthLevel: faker.string.uuid(),
                        frthLevel_2: faker.string.uuid(),
                        frthLevel_3: faker.string.uuid(),
                    },
                },
            },
        }

        await dynamodb.upsertRecordNested(updatedRecord, {
            conditionExpression: 'attribute_not_exists(#data.#active_at) or #data.#active_at < :active_at',
            expressionAttributeValues: {
                ':active_at': { 'S': '2023-10-05' },
            },
        })
        const resp = await dynamodb.getRecord(updatedRecord.uuid, updatedRecord.record_id as string)
        console.log(JSON.stringify(resp))
        expect(resp).toBeDefined()
        expect(resp.data?.active_at).toBe('2023-10-05')
        expect(resp.data?.event_data['user:created']).toBeUndefined()
        expect(resp.data?.newPath?.thrdLevel?.frthLevel).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel)
        expect(resp.data?.newPath?.thrdLevel?.frthLevel_2).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel_2)
        expect(resp.data?.newPath?.thrdLevel?.frthLevel_3).toEqual(updatedRecord.data.newPath.thrdLevel.frthLevel_3)
    })

    test('update all record with nested attributes', async () => {

        const updatedRecord = { ...record }
        const fakeName = 'Patricia1'
        if (typeof record.data == 'object') {
            updatedRecord.data = {
                ...record.data,
                firstname: fakeName,
                lastname: null,
            }
        }
        await dynamodb.updateRecordExcluding(updatedRecord, ['data.firstname'])
        const resp = await dynamodb.getRecord(record.uuid as string, record.record_id as string)
        expect((resp?.data as DynamoDbSchema).firstname === fakeName).toBeFalsy()
        expect((resp?.data as DynamoDbSchema).lastname).toBeNull()
    })

    test('batch put records', async () => {
        const records = Array(10).fill(0).map(_ => DynamodbRecordFactory.create())
        await dynamodb.batchWriteRecord(records)
        for (const r of records) {
            const data = await dynamodb.getRecord(r.uuid as string, r.record_id as string)
            expect(data).toBeDefined()
        }
    })
    test('batch delete records', async () => {
        const records = Array(10).fill(0).map(_ => DynamodbRecordFactory.create())
        await dynamodb.batchWriteRecord(records)
        await dynamodb.batchDeleteRecord(records)
        for (const r of records) {
            const data = await dynamodb.getRecord(r.uuid as string, r.record_id as string)
            expect(data).toBeFalsy()
        }
    })

    test('batch get records', async () => {
        const records = Array(100).fill(0).map(_ => DynamodbRecordFactory.create())
        await dynamodb.batchWriteRecord(records)
        const result = await dynamodb.batchGetRecord(
            records.map(r => ({ uuid: r.uuid, record_id: r.record_id })),
        )
        expect(result).toBeDefined()
        expect(result.length).toBe(100)
    })

    test('describe a table', async () => {
        const response = await dynamodb.describeTable()
        console.debug(JSON.stringify(response))
        expect(response).toBeDefined()
    })
})
