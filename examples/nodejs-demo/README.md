# Fluree Node.js SDK Demo

This example demonstrates how to use the Fluree Node.js SDK in a Node.js application.

## Features

The demo showcases:
- Creating a memory connection
- Creating a new ledger
- Inserting JSON-LD data (people and company)
- Querying data with various patterns
- Updating existing data
- Verifying updates

## Prerequisites

- Node.js version 14 or higher
- Fluree Node.js SDK built

## Getting Started

1. Build the Node.js SDK (from project root):
   ```bash
   make node
   ```

2. Navigate to this example directory:
   ```bash
   cd examples/nodejs-demo
   ```

3. Run the demo:
   ```bash
   npm start
   # or
   node index.js
   ```

## What the Demo Does

1. **Connection**: Creates an in-memory connection to Fluree
2. **Ledger Creation**: Creates a new ledger named "demo-ledger"
3. **Data Insertion**: Inserts sample data including:
   - Two people (Alice and Bob) with names, ages, emails, departments, and skills
   - A company (Tech Corp) with employees
4. **Queries**: Demonstrates various query patterns:
   - Query all people
   - Query company with employee details
   - Query people by specific skills
5. **Updates**: Updates Alice's age from 30 to 31
6. **Verification**: Queries the updated data to confirm the change

## Files

- `index.js` - Main Node.js script demonstrating Fluree SDK usage
- `package.json` - Node.js package configuration
- `README.md` - This documentation file