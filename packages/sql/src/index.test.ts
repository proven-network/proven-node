import { getApplicationDb, sql } from './index';

const DB = getApplicationDb('main')
  .migrate(
    `
  CREATE TABLE posts (
    id integer primary KEY,
    title text not null,
    content text,
    creator text not null,
    created_at integer not null,
    updated_at integer not null
  );`
  )
  .migrate(
    `
  CREATE TABLE likes (
    post_id INTEGER NOT NULL,
    user_id TEXT NOT NULL
  );`
  )
  .migrate(`ALTER TABLE posts DROP COLUMN updated_at;`)
  .migrate(`ALTER TABLE posts RENAME COLUMN created_at TO published_at;`)
  .migrate(`DROP TABLE likes;`)
  .migrate(`ALTER TABLE posts ADD COLUMN updated_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP;`);

// const rows = await DB.query('SELECT * FROM posts');

// const row = rows[0];

// for (const row of rows) {
//   console.log(row);
// }

// let mapped = rows.map((row) => row.title);
// let filtered = rows.filter((row) => row.title === 'Hello');
// let length = rows.length;
// let columnNames = rows.columnNames;

// const rows2 = await DB.query(sql('SELECT * FROM posts WHERE id = :id', { id: 1 }));
