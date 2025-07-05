import { runWithOptions } from '@proven-network/handler';

export const test = runWithOptions(
  {
    allowedOrigins: ['https://example.com'],
    timeout: 10000,
  },
  async () => {
    const response = await fetch('https://example.com/');
    return response.status;
  }
);
