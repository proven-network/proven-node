# SQL Database Types for Proven Application Code

This package defines SQL database types for usage in Proven Application code. It includes typed database access for application and personal contexts.

## Installation

Install this package as a dev dependency:

```bash
npm install --save-dev @proven-network/sql
```

or

```bash
yarn add -D @proven-network/sql
```

## Usage

### Application Database

```typescript
import { getApplicationDb } from '@proven-network/sql';

const APP_DB = getApplicationDb('myAppDb').migrate(`
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      content TEXT
    );
  `);

export const handler = async () => {
  const posts = await APP_DB.query(`
    SELECT * FROM posts WHERE title LIKE 'Hello%'
  `);
  console.log(posts);
};
```

### Personal Database

```typescript
import { getPersonalDb } from '@proven-network/sql';

const PERSONAL_DB = getPersonalDb('myPersonalDb').migrate(`
    CREATE TABLE tasks (
      id INTEGER PRIMARY KEY,
      description TEXT NOT NULL,
      completed BOOLEAN DEFAULT FALSE
    );
  `);

export const handler = async () => {
  const tasks = await PERSONAL_DB.query(`
    SELECT * FROM tasks WHERE completed = FALSE
  `);
  console.log(tasks);
};
```

### NFT Database

```typescript
import { getNftDb } from '@proven-network/sql';

const NFT_DB = getPersonalDb('myPersonalDb').migrate(`
    CREATE TABLE tasks (
      id INTEGER PRIMARY KEY,
      description TEXT NOT NULL,
      completed BOOLEAN DEFAULT FALSE
    );
  `);
const RESOURCE_ADDR = 'resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi';

export const handler = async (nftId: string) => {
  const tasks = await NFT_DB.query(
    RESOURCE_ADDR,
    nftId,
    `
    SELECT * FROM tasks WHERE completed = FALSE
  `
  );
  console.log(tasks);
};
```
