import { HardhatUserConfig } from 'hardhat/config';
import '@nomicfoundation/hardhat-toolbox';
import * as dotenv from 'dotenv';

dotenv.config();

// Get private key from environment variables
const PRIVATE_KEY = process.env.PRIVATE_KEY;
// Get Alchemy API key
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY || '';
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY || '';

const config: HardhatUserConfig = {
  solidity: {
    version: '0.8.20',
    settings: {
      optimizer: {
        enabled: true,
        runs: 1000,
      },
      viaIR: true,
    },
  },
  paths: {
    sources: './contracts',
    tests: './test',
    cache: './cache',
    artifacts: './artifacts',
  },
  networks: {
    hardhat: {},
    ...(PRIVATE_KEY ? {
      sepolia: {
        url: `https://eth-sepolia.g.alchemy.com/v2/${ALCHEMY_API_KEY}`,
        accounts: [PRIVATE_KEY],
      },
    } : {}),
    // Add other networks as needed
  },
  etherscan: {
    apiKey: ETHERSCAN_API_KEY,
  },
};

export default config;
