module.exports = {
  tables: [
    {
      TableName: 'datalake',
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        {
          AttributeName: 'uuid',
          AttributeType: 'S',
        },
        {
          AttributeName: 'record_id',
          AttributeType: 'S',
        },
        {
          AttributeName: 'meta',
          AttributeType: 'S',
        },
        {
          AttributeName: 'domain',
          AttributeType: 'S',
        },
      ],
      KeySchema: [
        {
          AttributeName: 'uuid',
          KeyType: 'HASH',
        },
        {
          AttributeName: 'record_id',
          KeyType: 'RANGE',
        },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: 'domain-index',
          KeySchema: [
            {
              AttributeName: 'domain',
              KeyType: 'HASH',
            },
          ],
          Projection: {
            ProjectionType: 'ALL',
          },
        },
        {
          IndexName: 'meta-index',
          KeySchema: [
            {
              AttributeName: 'meta',
              KeyType: 'HASH',
            },
          ],
          Projection: {
            ProjectionType: 'ALL',
          },
        },
      ],
      StreamSpecification: {
        StreamEnabled: true,
        StreamViewType: 'NEW_AND_OLD_IMAGES',
      },
      ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
    },
  ],
};
