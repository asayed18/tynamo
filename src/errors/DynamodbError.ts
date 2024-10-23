export class DynamodbError extends Error {
    public original: Error

    constructor(orig: Error) {
        super(orig.message)
        this.message = orig.message || 'DynamodbError'
        this.name = orig.name
        this.original = orig
    }
}