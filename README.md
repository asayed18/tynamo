# Tynamo ![Test Status](https://github.com/ahmed-abdelsalam/tynamo/actions/workflows/test.yml/badge.svg) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ahmed-abdelsalam/70e8c5f5a52c8c45b84351b01b698be4/raw/tynamo.json)

This library provides a simplified interface for interacting with AWS DynamoDB, offering easy-to-use methods for common operations such as creating, updating, upserting, and querying records with support for nested attributes. It's designed to streamline the development process for applications that utilize DynamoDB by abstracting away some of the complexity involved in direct DynamoDB interactions.

## Features

- **Simplified Record Operations**: Create, update, upsert, and delete DynamoDB records with simple method calls.
- **Nested Attribute Support**: Easily manage nested attributes in your DynamoDB records.
- **Batch Operations**: Perform batch operations like batch write for efficient data management.
- **Local and AWS Deployment**: Configure the library for use with a local DynamoDB instance or an AWS-hosted instance.

## Installation

To use this library, you'll first need to install it via npm:

```bash
npm install @asalam/taynmo
```

## Configuration

Before you can start using the library, you need to configure it with your DynamoDB settings. Here's an example:

```ts
const { Tynamo } = require('@asalam/taynmo')

const dynamodb = Tynamo.create('your-table-name', 'your-primary-key', 'your-sort-key', {
  logLevel: 'ERROR',
  region: 'your-region',
  endpoint: 'your-endpoint', // Optional for local development
  credentials: {
    accessKeyId: 'your-access-key-id',
    secretAccessKey: 'your-secret-access-key',
  },
})
```

If you have only primary key without composite key you can use

```ts
const dynamodb = Tynamo.createOnlyPk('your-table-name', 'your-primary-key')
```

## Usage

### Creating a Record

```ts
const record = {
  /* your record data */
}
await dynamodb.putRecord(record)
```

### Updating a Record

```ts
const updatedRecord = {
  /* your updated record data */
}
// Update specific fields inside a record
await dynamodb.updateRecordIncluding(updatedRecord, ['path.to.attribute'])

// Update all fields inside a record except for specific attributes
await dynamodb.updateRecordExcluding(record, ['path.to.attribute'])
```

### Upserting a Record

```ts
const upsertRecord = {
  /* your record data for upsert */
}
await dynamodb.upsertRecordNested(upsertRecord)
```

### Querying a Record

```ts
const resp = await dynamodb.getRecord('<pk>', '<sk>')
```

### Querying records

```ts
const records = await dynamodb.batchGetRecord([{pk:<pk>, sk:<sk>}, ...]);
```

## Contributing

Contributions to the library are welcome! Please follow the standard fork-and-pull request workflow on GitHub to submit your changes.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
