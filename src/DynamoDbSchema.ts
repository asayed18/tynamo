import { NativeAttributeValue } from '@aws-sdk/util-dynamodb'

export type DynamoDbSchema = NativeAttributeValue
// export type BasicDynamodbType = string | number | boolean | Set<any> | Array<any> | null

export type CompositeKeySchema<PK extends string, SK extends string | undefined = undefined> = {
    [P in PK]: string;
} & (SK extends undefined ? Record<string, never> : { [S in Exclude<SK, undefined>]: string }) & DynamoDbSchema