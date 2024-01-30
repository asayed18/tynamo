# Tynamo

This library provides a simplified interface for interacting with AWS DynamoDB, offering easy-to-use methods for common operations such as creating, updating, upserting, and querying records with support for nested attributes. It's designed to streamline the development process for applications that utilize DynamoDB by abstracting away some of the complexity involved in direct DynamoDB interactions.

## Features

- **Simplified Record Operations**: Create, update, upsert, and delete DynamoDB records with simple method calls.
- **Nested Attribute Support**: Easily manage nested attributes in your DynamoDB records.
- **Batch Operations**: Perform batch operations like batch write for efficient data management.
- **Local and AWS Deployment**: Configure the library for use with a local DynamoDB instance or an AWS-hosted instance.

## Installation

To use this library, you'll first need to install it via npm:

```bash
npm install tynamo
```

## Configuration

Before you can start using the library, you need to configure it with your DynamoDB settings. Here's an example:

```javascript
const { Tynamo } = require('your-library-name');

const dynamodb = Tynamo.create('your-table-name', 'your-primary-key', 'your-sort-key', {
    region: 'your-region',
    endpoint: 'your-endpoint', // Optional for local development
    credentials: {
        accessKeyId: 'your-access-key-id',
        secretAccessKey: 'your-secret-access-key',
    },
});
```

If you have only primary key without composite key you can use 

```javascript
const dynamodb = Tynamo.createOnlyPk('your-table-name', 'your-primary-key');
```



## Usage

### Creating a Record

```javascript
const record = { /* your record data */ };
await dynamodb.putRecord(record);
```

### Updating a Record

```javascript
const updatedRecord = { /* your updated record data */ };
await dynamodb.updateRecordIncluding(updatedRecord, ['path.to.attribute']);
```

### Upserting a Record

```javascript
const upsertRecord = { /* your record data for upsert */ };
await dynamodb.upsertRecordNested(upsertRecord);
```

### Querying a Record

```javascript
const resp = await dynamodb.getRecord('your-record-uuid', 'your-record-id');
```

## Contributing

Contributions to the library are welcome! Please follow the standard fork-and-pull request workflow on GitHub to submit your changes.

## License

This project is licensed under the MIT License - see the LICENSE file for details.