# Proven Governance Test Suite

This directory contains comprehensive tests for the Proven Network governance smart contracts. The tests cover all key functionality including token operations, timelock mechanisms, and governance processes for both node and version management.

## Test Files

- `ProvenToken.test.ts` - Tests for the ERC20 token with voting capabilities
- `ProvenTimelock.test.ts` - Tests for the timelock controller
- `NodeGovernance.test.ts` - Tests for network topology governance
- `VersionGovernance.test.ts` - Tests for node version governance
- `TestTarget.test.ts` - Tests for the helper contract used in timelock tests

## Running Tests

To run all tests:

```bash
npm run test
```

To run a specific test file:

```bash
npx hardhat test test/ProvenToken.test.ts
```

To run tests with gas reporting:

```bash
REPORT_GAS=true npm run test
```

To run tests with coverage reporting:

```bash
npm run coverage
```

## Test Coverage

The test suite aims to provide comprehensive coverage of:

1. **Token Functionality**

   - Basic ERC20 operations
   - Voting power delegation
   - Minting controls

2. **Timelock Mechanism**

   - Role management
   - Operation scheduling, execution, and cancellation
   - Delay enforcement

3. **Node Governance**

   - Opportunity creation and management
   - Candidate registration
   - Node approval workflow
   - Governance voting mechanisms

4. **Version Governance**
   - Version addition, activation, and deactivation
   - Active version tracking
   - Governance voting mechanisms

## Important Notes

- The tests use Hardhat's network helpers to manipulate blockchain time for testing timelock functionality
- For governance tests, we simulate the entire proposal lifecycle:
  1. Proposal creation
  2. Voting
  3. Queue
  4. Execution
- Test tokens are distributed to create realistic voting scenarios
