// import { config } from 'aws-sdk'
import { DynamoDbSchema } from '@_/DynamoDbSchema'
import { Tynamo } from '@_/tynamo'
import { faker } from '@faker-js/faker'

import { DynamodbRecordFactory } from './factories/dynamodbRecordFactory'

// config.update({
//     region: 'us-east-1',
// })

describe('DynamoDB', () => {
    const record = DynamodbRecordFactory.create()
    const dynamodb = Tynamo.create('datalake', 'uuid', 'record_id', {
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
})
