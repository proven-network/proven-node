import { getApplicationDb, sql } from "@proven-network/sql";

const DB = getApplicationDb("main").migrate(`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`);

export const test = async () => {
    const email = "alice@example.com";

    await DB.execute(sql`INSERT INTO users (email) VALUES (${email})`);

    const results = await DB.query("SELECT * FROM users");

    return results[0].email;
};
