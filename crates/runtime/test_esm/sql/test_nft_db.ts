import { getNftDb, sql } from "@proven-network/sql";

const NFT_DB = getNftDb("myAppDb").migrate(
  `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`
);
const RESOURCE_ADDR = "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi";

export const test = async () => {
  const email = "alice@example.com";
  const nftId = 420;

  const affectedRows = await NFT_DB.execute(
    RESOURCE_ADDR,
    nftId,
    sql("INSERT INTO users (email) VALUES (:email)", { email })
  );

  if (affectedRows !== 1) {
    throw new Error("Unexpected number of affected rows");
  }

  const results = await NFT_DB.query(
    RESOURCE_ADDR,
    nftId,
    "SELECT * FROM users"
  );

  return results[0].email;
};
