# Proven Network Governance System

This directory contains the smart contracts and deployment scripts for the Proven Network decentralized governance system, which manages both software versions and network topology.

## Overview

The governance system consists of the following components:

- **ProvenToken (PRVN)**: ERC20 governance token used for voting
- **ProvenTimelock**: Time-locked controller for delayed execution of governance decisions
- **VersionGovernance**: Manages node software versions through governance proposals
- **NodeGovernance**: Manages network topology, node opportunities, and operator selection

## Development Setup

### Prerequisites

- Node.js v18+
- Yarn or NPM

### Installation

1. Clone the repository and navigate to the governance directory:

```bash
cd /Users/sebastianedwards/Code/proven-node/governance
```

2. Install dependencies:

```bash
npm install
```

3. Copy the example environment file and configure it:

```bash
cp .env.example .env
# Edit .env with your configuration
```

### Testing

Run the comprehensive test suite:

```bash
npm test
```

Generate a code coverage report:

```bash
npm run coverage
```

### Compilation

Compile the smart contracts:

```bash
npm run compile
```

## Deployment

The contracts can be deployed to various networks:

```bash
# Deploy to a local Hardhat network
npx hardhat run scripts/deploy.ts

# Deploy to a testnet or mainnet (as configured in hardhat.config.ts)
npx hardhat run scripts/deploy.ts --network sepolia
```

## Governance Flow

### Version Management

1. A proposal is created to add a new software version (sequence, PCR values)
2. Token holders vote on the proposal
3. If passed and executed, the version is added (but not yet activated)
4. Another proposal must be created to activate the version
5. When activated, nodes can use this version

### Node Operator Onboarding

1. A proposal is created to add a new node opportunity (region, AZ, specializations)
2. Token holders vote on the proposal
3. If passed and executed, the opportunity is open
4. Node operators can register as candidates for the opportunity
5. A proposal is created to approve a specific candidate
6. If passed and executed, the candidate becomes an official node operator
7. The opportunity is closed

## Smart Contract Documentation

### ProvenToken

An ERC20 token with governance capabilities (voting, delegation).

Key functions:

- `delegate(address)`: Delegate voting power to an address
- `mint(address, uint256)`: Mint new tokens (owner only)

### VersionGovernance

Manages software versions that can be run on nodes.

Key functions:

- `addVersion(uint64, string, string, string)`: Add a new version with PCR values
- `activateVersion(uint64)`: Activate a version
- `deactivateVersion(uint64)`: Deactivate a version
- `getActiveVersions()`: Get all active versions

### NodeGovernance

Manages network topology through node opportunities and approvals.

Key functions:

- `createOpportunity(string, string, bytes32[])`: Create a new node opportunity
- `registerAsCandidate(bytes32, string, string)`: Register as a candidate for an opportunity
- `approveCandidate(bytes32, address)`: Approve a candidate
- `removeNode(string)`: Remove a node from the network
- `getNodes()`: Get all active nodes

## License

MIT
