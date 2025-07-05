import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { EntrypointDiscovery } from '../src/entrypoint-discovery';
import * as fs from 'fs';
import * as path from 'path';

jest.mock('fs', () => ({
  promises: {
    readFile: jest.fn(),
  },
}));
jest.mock('path');

const mockFs = fs.promises as jest.Mocked<typeof fs.promises>;
const mockPath = path as jest.Mocked<typeof path>;

describe('EntrypointDiscovery', () => {
  let discovery: EntrypointDiscovery;
  const mockProjectRoot = '/test/project';

  beforeEach(() => {
    jest.clearAllMocks();
    discovery = new EntrypointDiscovery(mockProjectRoot);

    // Setup path mocks
    mockPath.resolve.mockImplementation((...args) => args.join('/'));
    mockPath.relative.mockImplementation((from, to) => to.replace(from, ''));
    mockPath.dirname.mockImplementation((p) => p.split('/').slice(0, -1).join('/'));
    mockPath.extname.mockImplementation((p) => {
      const parts = p.split('.');
      return parts.length > 1 ? '.' + parts[parts.length - 1] : '';
    });
  });

  describe('analyzeFile', () => {
    it('should detect HTTP handler', async () => {
      const sourceCode = `
        import { runOnHttp } from '@proven-network/handler';
        
        export const handler = runOnHttp(
          { path: '/api/test', method: 'GET' },
          (req) => ({ message: 'test' })
        );
      `;

      mockFs.readFile.mockResolvedValue(sourceCode);

      const result = await discovery.analyzeFile('/test/project/src/handler.ts');

      expect(result).toBeDefined();
      expect(result?.filePath).toBe('/test/project/src/handler.ts');
      expect(result?.handlers).toHaveLength(1);
      expect(result?.handlers[0].type).toBe('http');
      expect(result?.handlers[0].name).toBe('handler');
      expect(result?.imports).toHaveLength(1);
      expect(result?.imports[0].module).toBe('@proven-network/handler');
    });

    it('should detect schedule handler', async () => {
      const sourceCode = `
        import { runOnSchedule } from '@proven-network/handler';
        
        export const scheduleHandler = runOnSchedule(
          { cron: '0 */6 * * *' },
          () => console.log('scheduled')
        );
      `;

      mockFs.readFile.mockResolvedValue(sourceCode);

      const result = await discovery.analyzeFile('/test/project/src/schedule.ts');

      expect(result).toBeDefined();
      expect(result?.handlers).toHaveLength(1);
      expect(result?.handlers[0].type).toBe('schedule');
      expect(result?.handlers[0].name).toBe('scheduleHandler');
    });

    it('should detect event handlers', async () => {
      const sourceCode = `
        import { runOnProvenEvent, runOnRadixEvent } from '@proven-network/handler';
        
        export const provenHandler = runOnProvenEvent(
          { event: 'user.created' },
          (event) => console.log(event)
        );
        
        export const radixHandler = runOnRadixEvent(
          { address: '0x123' },
          (event) => console.log(event)
        );
      `;

      mockFs.readFile.mockResolvedValue(sourceCode);

      const result = await discovery.analyzeFile('/test/project/src/events.ts');

      expect(result).toBeDefined();
      expect(result?.handlers).toHaveLength(2);
      expect(result?.handlers[0].type).toBe('event');
      expect(result?.handlers[0].name).toBe('provenHandler');
      expect(result?.handlers[1].type).toBe('event');
      expect(result?.handlers[1].name).toBe('radixHandler');
    });

    it('should detect RPC handlers', async () => {
      const sourceCode = `
        import { runWithOptions, run } from '@proven-network/handler';
        
        export const rpcHandler = runWithOptions(
          { timeout: 30000 },
          (input) => ({ result: input * 2 })
        );
        
        export const simpleHandler = run((input) => input.toUpperCase());
      `;

      mockFs.readFile.mockResolvedValue(sourceCode);

      const result = await discovery.analyzeFile('/test/project/src/rpc.ts');

      expect(result).toBeDefined();
      expect(result?.handlers).toHaveLength(2);
      expect(result?.handlers[0].type).toBe('rpc');
      expect(result?.handlers[0].name).toBe('rpcHandler');
      expect(result?.handlers[1].type).toBe('rpc');
      expect(result?.handlers[1].name).toBe('simpleHandler');
    });

    it('should handle multiple imports from same module', async () => {
      const sourceCode = `
        import { runOnHttp, runOnSchedule } from '@proven-network/handler';
        import type { HttpConfig } from '@proven-network/handler';
        
        export const httpHandler = runOnHttp(
          { path: '/api/test' },
          (req) => ({ ok: true })
        );
      `;

      mockFs.readFile.mockResolvedValue(sourceCode);

      const result = await discovery.analyzeFile('/test/project/src/multi.ts');

      expect(result).toBeDefined();
      expect(result?.imports).toHaveLength(2);
      expect(result?.imports[0].module).toBe('@proven-network/handler');
      expect(result?.imports[1].module).toBe('@proven-network/handler');
    });

    it('should return null for files without proven handlers', async () => {
      const sourceCode = `
        import express from 'express';
        
        const app = express();
        app.get('/test', (req, res) => res.json({ ok: true }));
      `;

      mockFs.readFile.mockResolvedValue(sourceCode);

      const result = await discovery.analyzeFile('/test/project/src/express.ts');

      expect(result).toBeNull();
    });

    it('should handle syntax errors gracefully', async () => {
      const sourceCode = `
        import { runOnHttp } from '@proven-network/handler';
        
        export const handler = runOnHttp(
          { path: '/api/test' },
          (req) => {
            // Missing closing brace
      `;

      mockFs.readFile.mockResolvedValue(sourceCode);

      const result = await discovery.analyzeFile('/test/project/src/broken.ts');

      expect(result).toBeNull();
    });

    it('should handle file read errors', async () => {
      mockFs.readFile.mockRejectedValue(new Error('File not found'));

      const result = await discovery.analyzeFile('/test/project/src/missing.ts');

      expect(result).toBeNull();
    });
  });

  describe('custom patterns', () => {
    it('should use custom patterns for entrypoint discovery', () => {
      const customPatterns = ['src/api/**/*.ts', 'src/handlers/**/*.ts'];
      const discoveryWithPatterns = new EntrypointDiscovery(mockProjectRoot, customPatterns);

      expect(discoveryWithPatterns).toBeDefined();
      // Custom patterns are used internally, test through integration
    });
  });
});
