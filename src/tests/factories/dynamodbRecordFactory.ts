import { CompositeKeySchema } from '@_/DynamoDbSchema'
import { faker } from '@faker-js/faker'

export class DynamodbRecordFactory {
    static create(): CompositeKeySchema<'uuid', 'record_id'> {
        const uuid = faker.string.uuid()
        return {
            uuid,
            record_id: `${uuid}_user`,
            created_at: faker.date.past().toISOString(),
            data: {
                uuid,
                city: null,
                country: null,
                active_at: '2023-10-01',
                country_alpha3: faker.location.countryCode('alpha-3'),
                created_at: faker.date.past().toISOString(),
                displayname: faker.internet.userName(),
                event_data: {
                    'user:created': faker.string.uuid(),
                },
                firstname: faker.person.firstName(),
                ietf_language_tag: faker.location.countryCode('alpha-2'),
                lastname: faker.person.lastName(),
                learn_language_alpha3: faker.location.countryCode('alpha-3').toUpperCase(),
                locale: faker.location.countryCode('alpha-2'),
                newsletter: true,
                postal_code: null,
                reference_language_alpha3: null,
                region: null,
                timezone: null,
                tracking_uuid: null,
                user_id: faker.string.uuid(),
            },
            domain: 'user',
            meta: faker.internet.email(),
            targets: null,
            updated_at: faker.date.future().toISOString(),
            version: faker.number.int(),
        }
    }
}
