const { build } = require('esbuild');

const config = {
  entryPoints: [
    'static/sdk.ts',
    'static/iframes/bridge/bridge.ts',
    'static/iframes/button/button.ts',
    'static/iframes/register/register.ts',
    'static/iframes/rpc/rpc.ts',
    'static/workers/broker-worker.ts',
    'static/workers/rpc-worker.ts'
  ],
  bundle: true,
  outdir: 'static',
  // You can add more options here as needed:
  // minify: true,
  // sourcemap: true,
  // target: 'es2020',
  // format: 'esm',
};

build(config).catch(() => process.exit(1)); 
