const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 8081; // Different port from webpack example

// MIME types for different file extensions
const mimeTypes = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.gif': 'image/gif',
  '.ico': 'image/x-icon',
  '.svg': 'image/svg+xml',
};

const server = http.createServer((req, res) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);

  let filePath = path.join(__dirname, req.url === '/' ? 'index.html' : req.url);

  // Security check - ensure file is within the project directory
  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  const extname = path.extname(filePath).toLowerCase();
  const contentType = mimeTypes[extname] || 'application/octet-stream';

  fs.readFile(filePath, (err, content) => {
    if (err) {
      if (err.code === 'ENOENT') {
        // File not found
        res.writeHead(404);
        res.end('File not found');
      } else {
        // Server error
        res.writeHead(500);
        res.end('Server error: ' + err.code);
      }
    } else {
      // Success
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content, 'utf-8');
    }
  });
});

server.listen(PORT, () => {
  console.log(`🎯 Rollup Todo app server running at http://localhost:${PORT}`);
  console.log('Available routes:');
  console.log(`  http://localhost:${PORT}/           - Todo app`);
  console.log(`  http://localhost:${PORT}/dist/      - Built assets`);
  console.log('');
  console.log('To build the app: npm run build');
  console.log('To watch for changes: npm run dev');
  console.log('To build for production: npm run build:prod');
});
