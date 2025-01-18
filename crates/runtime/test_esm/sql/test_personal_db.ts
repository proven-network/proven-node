import { getPersonalDb, sql } from "@proven-network/sql";

const PERSONAL_DB = getPersonalDb("myAppDb").migrate(`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`);

export const test = async () => {
  const email = "alice@example.com";

      await PERSONAL_DB.execute(sql`INSERT INTO users (email) VALUES (${email})`);

      const results = await PERSONAL_DB.query("SELECT * FROM users");

      return results[0].email;
}
