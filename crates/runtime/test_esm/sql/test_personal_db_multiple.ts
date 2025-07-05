import { run } from '@proven-network/handler';
import { getPersonalDb, sql } from '@proven-network/sql';

const PERSONAL_DB = getPersonalDb('myAppDb').migrate(
  `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`
);

export const test = run(async () => {
  let affectedRows = await PERSONAL_DB.execute(
    sql('INSERT INTO users (email) VALUES (:email)', {
      email: 'alice@example.com',
    })
  );

  if (affectedRows !== 1) {
    throw new Error('Unexpected number of affected rows');
  }

  affectedRows = await PERSONAL_DB.execute(
    sql('INSERT INTO users (email) VALUES (:email)', {
      email: 'bob@example.com',
    })
  );

  if (affectedRows !== 1) {
    throw new Error('Unexpected number of affected rows');
  }

  const results = await PERSONAL_DB.query('SELECT * FROM users');

  if ((await results.length) !== 2) {
    throw new Error('Unexpected number of rows');
  }

  return results;
});
