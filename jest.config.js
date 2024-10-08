const { pathsToModuleNameMapper } = require('ts-jest')
const tsPreset = require('ts-jest/jest-preset')
const { compilerOptions } = require('./tsconfig.json')

const moduleNameMapper = compilerOptions.paths
  ? pathsToModuleNameMapper(compilerOptions.paths, { prefix: '<rootDir>' })
  : {}
module.exports = {
  displayName: 'tsdynamo',
  preset: '@shelf/jest-dynamodb',
  ...tsPreset,
  testEnvironment: 'node',
  setupFiles: ['./jest.setup.js'],
  moduleFileExtensions: ['ts', 'js', 'html'],
  testTimeout: 30000,
  openHandlesTimeout: 0,
  moduleNameMapper,
}
