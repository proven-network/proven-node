const baseConfig = require('./jest.config.base.js');

module.exports = {
  ...baseConfig,
  projects: [
    {
      ...baseConfig,
      displayName: 'packages',
      rootDir: './packages',
      roots: [
        '<rootDir>/bundler',
        '<rootDir>/common', 
        '<rootDir>/crypto',
        '<rootDir>/handler',
        '<rootDir>/kv',
        '<rootDir>/sdk',
        '<rootDir>/session',
        '<rootDir>/sql'
      ],
      moduleNameMapper: {
        '^@proven-network/common$': '<rootDir>/common/src/index.ts',
        '^@proven-network/bundler/(.*)$': '<rootDir>/bundler/src/$1',
        '^@proven-network/bundler$': '<rootDir>/bundler/src/index.ts',
      },
    },
    {
      ...baseConfig,
      displayName: 'runtime',
      rootDir: './crates/runtime',
      roots: ['<rootDir>'],
      testMatch: ['**/test_esm/**/*.test.ts', '**/test_esm/**/*.spec.ts'],
      moduleDirectories: ['node_modules'],
      globals: {
        'ts-jest': {
          babelConfig: true,
        },
      },
    }
  ],
};