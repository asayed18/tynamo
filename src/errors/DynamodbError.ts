export class DynamodbError extends Error {

    constructor(message: string, orig?: Error) {
        super(message)
        this.name = 'DynamodbError'
        if (orig) this.stack = orig.stack
    }
}