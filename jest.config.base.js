module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  moduleFileExtensions: ['ts', 'js', 'json'],
  testMatch: ['**/tests/**/*.test.ts', '**/?(*.)+(spec|test).ts'],
  transform: {
    '^.+\\.ts$': [
      'ts-jest',
      {
        useESM: false,
        tsconfig: {
          target: 'es2020',
          module: 'commonjs',
          esModuleInterop: true,
          allowSyntheticDefaultImports: true,
          strict: true,
          skipLibCheck: true,
        },
      },
    ],
  },
  collectCoverageFrom: [
    '**/src/**/*.ts',
    '!**/src/**/*.d.ts',
    '!**/node_modules/**',
    '!**/dist/**',
    '!**/types/**',
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
  testTimeout: 10000,
  clearMocks: true,
  testPathIgnorePatterns: ['/node_modules/', '/dist/', '/types/'],
  // setupFilesAfterEnv: ['<rootDir>/jest.setup.js'],
};