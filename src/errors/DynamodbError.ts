export class DynamodbError extends Error {

    constructor(orig: Error) {
        super(orig.message)
        this.message = orig.message || 'DynamodbError'
        this.name = orig.name
        this.stack = orig.stack
    }
}