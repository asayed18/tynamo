
/**
 * Converts a string to underscore format.
 * Replaces any non-alphanumeric characters with underscores.
 * 
 * @param input - The input string to be converted.
 * @returns The converted string in underscore format.
 */
export const convertToUnderscore = (input: string): string => {
    const regex = /[^a-zA-Z0-9_]/g
    return input.replace(regex, '_')
}