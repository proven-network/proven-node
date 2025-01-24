import { run } from "@proven-network/handler";
import { getApplicationDb, sql } from "@proven-network/sql";

const APP_DB = getApplicationDb("myAppDb").migrate(
  `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`
);

export const test = run(async () => {
  const email = "alice@example.com";

  const affectedRows = await APP_DB.execute(
    sql("INSERT INTO users (email) VALUES (:email)", { email })
  );

  if (affectedRows !== 1) {
    throw new Error("Unexpected number of affected rows");
  }

  const results = await APP_DB.query("SELECT * FROM users");
  const result = results[0];

  if (!result) {
    throw new Error("Expected row not found");
  }

  return result.email;
});
