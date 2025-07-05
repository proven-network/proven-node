import { run } from '@proven-network/handler';

import {
  BITCOIN_MAINNET_RPC,
  BITCOIN_TESTNET_RPC,
  ETHEREUM_HOLESKY_RPC,
  ETHEREUM_MAINNET_RPC,
  ETHEREUM_SEPOLIA_RPC,
  RADIX_MAINNET_RPC,
  RADIX_STOKENET_RPC,
} from '@proven-network/rpc';

export const test = run(() => {
  return [
    BITCOIN_MAINNET_RPC,
    BITCOIN_TESTNET_RPC,
    ETHEREUM_HOLESKY_RPC,
    ETHEREUM_MAINNET_RPC,
    ETHEREUM_SEPOLIA_RPC,
    RADIX_MAINNET_RPC,
    RADIX_STOKENET_RPC,
  ];
});
