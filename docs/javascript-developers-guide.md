# Fluree JavaScript Developer's Guide

This guide covers how to use Fluree with JavaScript in both Node.js and browser environments.

## Working Examples

We provide complete working examples for both environments:

- **Node.js Example**: [`/examples/nodejs-demo/`](../examples/nodejs-demo/) - [README](../examples/nodejs-demo/README.md)
- **Browser Example**: [`/examples/browser-demo/`](../examples/browser-demo/) - [README](../examples/browser-demo/README.md)

Each example includes:
- Complete source code with comments
- README with setup instructions
- Demonstrations of all major Fluree operations

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Getting Started](#getting-started)
4. [API Reference](#api-reference)
5. [Working with JSON-LD](#working-with-json-ld)
6. [Querying Data](#querying-data)
7. [Examples](#examples)
8. [Troubleshooting](#troubleshooting)

## Overview

Fluree provides JavaScript SDKs for both Node.js and browser environments. Both SDKs offer the same API surface, allowing you to:

- Connect to Fluree databases
- Create and manage ledgers
- Insert, update, and delete data using JSON-LD
- Query data using Fluree's query language
- Work with immutable databases

## Installation

### Building the SDKs

First, clone the Fluree repository and build the SDKs:

```bash
# Clone the repository
git clone https://github.com/fluree/db.git
cd db

# Build Node.js SDK
make node

# Build Browser SDK
make browser
```

### Node.js

After building, the Node.js SDK will be available at `./out/fluree-node-sdk.js`. You can import it in your Node.js application:

```javascript
import fluree from './path/to/fluree-node-sdk.js';
```

**Requirements:**
- Node.js 14.0.0 or higher
- ES modules support (use `"type": "module"` in package.json)

### Browser

The browser SDK will be available at `./out/fluree-browser-sdk.js`. Include it in your HTML:

```html
<script type="module">
  import fluree from './path/to/fluree-browser-sdk.js';
  // Your code here
</script>
```

**Requirements:**
- Modern browser with ES6 module support
- Web server (modules cannot be loaded from `file://` URLs)

## Getting Started

### Creating a Connection

#### Node.js

```javascript
// Memory connection (data stored in memory)
const conn = await fluree.connectMemory({});

// File-based connection (data persisted to disk)
const conn = await fluree.connectFile({
  storageDir: './fluree-data'
});
```

#### Browser

```javascript
// Memory connection (data stored in memory)
const conn = await fluree.connectMemory({});

// LocalStorage connection (data persisted in browser)
const conn = await fluree.connectLocalStorage({
  storageId: 'my-app-storage',
  cacheMaxMb: 50
});
```

### Creating a Ledger

```javascript
const ledger = await fluree.create(conn, 'my-ledger');
```

### Getting a Database

```javascript
const db = await fluree.db(ledger);
```

## API Reference

### Connection Methods

#### `fluree.connectMemory(opts)`
Creates an in-memory connection. Data is not persisted.

```javascript
const conn = await fluree.connectMemory({});
```

#### `fluree.connectFile(opts)` (Node.js only)
Creates a file-based connection for persistent storage.

```javascript
const conn = await fluree.connectFile({
  storageDir: './data'
});
```

#### `fluree.connectLocalStorage(opts)` (Browser only)
Creates a localStorage-based connection for browser persistence.

```javascript
const conn = await fluree.connectLocalStorage({
  storageId: 'my-app',
  cacheMaxMb: 50
});
```

### Ledger Methods

#### `fluree.create(conn, ledgerName, opts?)`
Creates a new ledger.

```javascript
const ledger = await fluree.create(conn, 'my-ledger');
```

#### `fluree.load(conn, ledgerName)`
Loads an existing ledger.

```javascript
const ledger = await fluree.load(conn, 'my-ledger');
```

#### `fluree.exists(conn, ledgerName)`
Checks if a ledger exists.

```javascript
const exists = await fluree.exists(conn, 'my-ledger');
console.log(exists); // true or false
```

### Database Methods

#### `fluree.db(ledger)`
Gets the current database from a ledger.

```javascript
const db = await fluree.db(ledger);
```

#### `fluree.stage(db, data)`
Stages changes to create a new database version.

```javascript
const stagedDb = await fluree.stage(db, {
  '@context': { /* ... */ },
  'insert': [ /* ... */ ]
});
```

#### `fluree.commit(ledger, db, opts?)`
Commits a staged database.

```javascript
const committedDb = await fluree.commit(ledger, stagedDb);

// With options
const result = await fluree.commit(ledger, stagedDb, {
  message: 'Added new users'
});
// Returns: { db: committedDb, commit: commitInfo }
```

#### `fluree.status(ledger, branch?)`
Gets the status of a ledger.

```javascript
const status = await fluree.status(ledger);
```

### Query Methods

#### `fluree.query(db, query)`
Executes a query against a database.

```javascript
const results = await fluree.query(db, {
  '@context': { /* ... */ },
  'select': { '?s': ['*'] },
  'where': { /* ... */ },
  // Optional execution opts for this query
  'opts': {
    // Controls batched subject-join optimization. Default is 10000.
    // Set to 0 to disable batching for this query.
    'subjectJoinBatchSize': 0
  }
});
```

## Working with JSON-LD

Fluree uses JSON-LD for data representation. Here's how to work with it:

### Inserting Data

```javascript
const data = {
  '@context': {
    'schema': 'http://schema.org/',
    'ex': 'http://example.org/'
  },
  'insert': [
    {
      '@id': 'ex:john',
      '@type': 'schema:Person',
      'schema:name': 'John Doe',
      'schema:age': 30
    }
  ]
};

const newDb = await fluree.stage(db, data);
const committedDb = await fluree.commit(ledger, newDb);
```

### Updating Data

```javascript
const update = {
  '@context': {
    'schema': 'http://schema.org/',
    'ex': 'http://example.org/'
  },
  'delete': [
    {
      '@id': 'ex:john',
      'schema:age': 30
    }
  ],
  'insert': [
    {
      '@id': 'ex:john',
      'schema:age': 31
    }
  ]
};

const updatedDb = await fluree.stage(db, update);
const committedDb = await fluree.commit(ledger, updatedDb);
```

### Deleting Data

```javascript
const deletion = {
  '@context': {
    'schema': 'http://schema.org/',
    'ex': 'http://example.org/'
  },
  'delete': [
    {
      '@id': 'ex:john',
      '@type': 'schema:Person',
      'schema:name': 'John Doe',
      'schema:age': 31
    }
  ]
};

const deletedDb = await fluree.stage(db, deletion);
const committedDb = await fluree.commit(ledger, deletedDb);
```

## Querying Data

Fluree supports powerful graph queries:

### Basic Query

```javascript
const query = {
  '@context': {
    'schema': 'http://schema.org/'
  },
  'select': { '?person': ['*'] },
  'where': {
    '@id': '?person',
    '@type': 'schema:Person'
  }
};

const results = await fluree.query(db, query);
```

### Query with Filters

```javascript
const query = {
  '@context': {
    'schema': 'http://schema.org/'
  },
  'select': ['?name', '?age'],
  'where': {
    '@id': '?person',
    '@type': 'schema:Person',
    'schema:name': '?name',
    'schema:age': '?age'
  },
  'filter': '(> ?age 25)'
};

const results = await fluree.query(db, query);
```

### Nested Queries

```javascript
const query = {
  '@context': {
    'schema': 'http://schema.org/',
    'ex': 'http://example.org/'
  },
  'select': {
    '?company': [
      '*',
      {
        'ex:employees': ['*']
      }
    ]
  },
  'where': {
    '@id': '?company',
    '@type': 'schema:Organization'
  }
};

const results = await fluree.query(db, query);
```

## Examples

Complete working examples are available in the repository:

### Node.js Example
See [`/examples/nodejs-demo/`](../examples/nodejs-demo/README.md)

```bash
cd examples/nodejs-demo
npm start
```

This example demonstrates:
- Creating connections
- Managing ledgers
- Inserting and querying data
- Updating records

### Browser Example
See [`/examples/browser-demo/`](../examples/browser-demo/README.md)

```bash
# Start a web server from project root
python3 -m http.server 8080

# Open in browser
# http://localhost:8080/examples/browser-demo/
```

This interactive example shows:
- Memory vs localStorage connections
- Real-time data insertion
- Multiple query patterns
- Custom query execution

## Troubleshooting

### Common Issues

#### "Cannot use import statement outside a module"
Ensure your Node.js project has `"type": "module"` in package.json.

#### "Failed to fetch dynamically imported module"
Browser modules must be served over HTTP/HTTPS. Use a web server instead of opening HTML files directly.

#### "should be a map" error
This usually means JavaScript objects are being passed where ClojureScript data structures are expected. The SDKs handle conversion automatically, but ensure you're using the SDK methods correctly.

#### Large bundle size
The browser SDK is approximately 5.5MB uncompressed (1MB gzipped). Consider:
- Serving with gzip compression
- Using a CDN
- Code splitting if using a bundler

### Debugging

Enable detailed logging:

```javascript
fluree.setLogging({ level: 'fine' });
```

Logging levels:
- `severe` - Only errors
- `warning` - Warnings and errors (default)
- `info` - General information
- `config` - Configuration details
- `fine` - Detailed debugging
- `finer` - More detailed debugging
- `finest` - Most detailed debugging

## Additional Resources

- [Fluree Documentation](https://docs.flur.ee)
- [JSON-LD Specification](https://json-ld.org/)
- [Example Applications](../examples/)

## Support

For issues and questions:
- GitHub Issues: https://github.com/fluree/db/issues
- Discord: https://discord.gg/fluree