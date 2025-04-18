const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = 6900;

const server = http.createServer((req, res) => {
  if (req.url === "/") {
    fs.readFile(path.join(__dirname, "iframe-test.html"), (err, content) => {
      if (err) {
        res.writeHead(500);
        res.end("Error loading test page");
        return;
      }
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end(content);
    });
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

server.listen(PORT, () => {
  console.log(`Test server running at http://localhost:${PORT}`);
});
