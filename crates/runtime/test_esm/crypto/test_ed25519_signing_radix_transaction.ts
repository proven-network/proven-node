import { generateEd25519Key } from '@proven-network/crypto';
import { run } from '@proven-network/handler';

import {
  ManifestBuilder,
  NetworkId,
  NotarizedTransaction,
  RadixEngineToolkit,
  TransactionBuilder,
  TransactionHeader,
  TransactionManifest,
  address,
  bucket,
  decimal,
  generateRandomNonce,
} from '@radixdlt/radix-engine-toolkit';

export const test = run(async () => {
  const notaryPrivateKey = generateEd25519Key();
  const signerPrivateKey = generateEd25519Key();

  // We first begin by creating the transaction header that will be used for the transaction.
  const transactionHeader: TransactionHeader = {
    networkId: NetworkId.Simulator,
    startEpochInclusive: 3910,
    endEpochExclusive: 3920,
    nonce: await generateRandomNonce(),
    notaryPublicKey: notaryPrivateKey.publicKey(),
    notaryIsSignatory: true,
    tipPercentage: 0,
  };

  // We then build the transaction manifest
  const transactionManifest: TransactionManifest = new ManifestBuilder()
    .callMethod(
      'account_tdx_2_12xhxpwuf3tyerew3j9fhuhar0xwngrftw37gxn7vu3k7cynagdpysp',
      'withdraw',
      [
        address('resource_tdx_2_1tknxxxxxxxxxradxrdxxxxxxxxx009923554798xxxxxxxxxtfd2jc'),
        decimal(10),
      ]
    )
    .takeAllFromWorktop(
      'resource_tdx_2_1tknxxxxxxxxxradxrdxxxxxxxxx009923554798xxxxxxxxxtfd2jc',
      (builder, bucketId) =>
        builder.callMethod(
          'account_tdx_2_12xhxpwuf3tyerew3j9fhuhar0xwngrftw37gxn7vu3k7cynagdpysp',
          'try_deposit_or_abort',
          [bucket(bucketId)]
        )
    )
    .build();

  // We may now build the complete transaction through the transaction builder.
  const transaction: NotarizedTransaction = await TransactionBuilder.new().then((builder) =>
    builder
      .header(transactionHeader)
      .manifest(transactionManifest)
      .sign(signerPrivateKey)
      .notarize(notaryPrivateKey)
  );

  const compiledTransaction = await RadixEngineToolkit.NotarizedTransaction.compile(transaction);

  const compiledTransactionHex = Array.from(compiledTransaction)
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');

  return [compiledTransactionHex, notaryPrivateKey.publicKeyHex(), signerPrivateKey.publicKeyHex()];
});
