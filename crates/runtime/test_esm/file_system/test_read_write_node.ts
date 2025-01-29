import { run } from "@proven-network/handler";
import * as fs from "node:fs";

export const test = run(() => {
  const file = "test.txt";
  const content = "Hello, world!";

  fs.writeFileSync(file, content);
  const readContent = fs.readFileSync(file, "utf8");

  return readContent;
});
