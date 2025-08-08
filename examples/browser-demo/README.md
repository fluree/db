# Fluree Browser SDK Demo

This example demonstrates how to use the Fluree Browser SDK in a web application.

## Features

- Connect to Fluree using memory or localStorage
- Create a ledger
- Insert JSON-LD data
- Query the data using various query patterns
- Execute custom queries

## Getting Started

1. Build the browser SDK:
   ```bash
   make browser
   ```

2. Start a local web server from the project root:
   ```bash
   # Using Python 3
   python3 -m http.server 8080
   
   # Or using Node.js
   npx http-server -p 8080
   ```

3. Open http://localhost:8080/examples/browser-demo/ in your web browser

4. Follow the steps in the demo:
   - Choose a connection type (Memory or LocalStorage)
   - Create a ledger
   - Insert sample data
   - Run queries

## Why a Web Server?

Modern browsers require ES6 modules to be served over HTTP/HTTPS due to CORS restrictions. Opening the HTML file directly (file://) will result in module loading errors.

## Connection Types

- **Memory**: Data is stored in memory and will be lost when the page is refreshed
- **LocalStorage**: Data is persisted in the browser's localStorage

## Query Examples

The demo includes several pre-built queries:
- Query all people
- Query company information with employees
- Query people by skills
- Custom query editor

## Files

- `index.html` - Main HTML file
- `demo.js` - JavaScript application logic
- `styles.css` - CSS styles