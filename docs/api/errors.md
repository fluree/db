# Errors and Status Codes

This document provides a complete reference for HTTP status codes and error responses in the Fluree API.

## Error Response Format

`fluree-server` errors return a consistent JSON structure:

```json
{
  "error": "Human-readable error description",
  "status": 400,
  "@type": "err:db/BadRequest",
  "cause": {
    "error": "Optional nested cause",
    "status": 400,
    "@type": "err:db/JsonParse"
  }
}
```

**Fields:**
- `error`: Human-readable error message (primary diagnostic text)
- `status`: HTTP status code (numeric)
- `@type`: Compact error type IRI (stable, machine-readable category)
- `cause`: Optional nested cause chain (only present for select errors)

**Stability note:** clients (including the Fluree CLI) may pattern-match on substrings within the `error` field for targeted hints, so error messages should be stable across releases.

## HTTP Status Codes

### Success Codes (2xx)

#### 200 OK

The request succeeded.

**Used for:**
- Successful queries
- Successful transactions
- Successful GET requests

**Example:**
```json
{
  "t": 5,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_id": "bafybeig...commitT5"
}
```

#### 201 Created

A new resource was created.

**Used for:**
- Ledger creation
- Index creation

**Example:**
```json
{
  "ledger_id": "mydb:main",
  "created": "2024-01-22T10:00:00.000Z"
}
```

#### 204 No Content

Request succeeded with no response body.

**Used for:**
- DELETE operations
- Administrative commands

### Client Error Codes (4xx)

#### 400 Bad Request

The request is malformed or contains invalid data.

**Common Causes:**
- Invalid JSON syntax
- Invalid JSON-LD structure
- Invalid SPARQL syntax
- Invalid IRI format
- Type mismatch

**Error typing:**

The server includes a compact error type IRI in the `@type` field. This is the
preferred stable, machine-readable category for programmatic handling.

**Example:**
```json
{
  "error": "Invalid JSON: expected value at line 5, column 12",
  "status": 400,
  "@type": "err:db/JsonParse"
}
```

**How to Fix:**
- Validate JSON syntax
- Check IRI formats
- Verify JSON-LD structure
- Review the `error` message and optional `cause`

#### 401 Unauthorized

Authentication is required but not provided or invalid.

**Common Causes:**
- Missing authentication credentials
- Invalid API key
- Expired JWT token
- Invalid signature (for signed requests)

**Example:**
```json
{
  "error": "Bearer token required",
  "status": 401,
  "@type": "err:db/Unauthorized"
}
```

**How to Fix:**
- Provide valid authentication credentials
- Check API key or token
- Renew expired tokens
- Verify signature process for signed requests

#### 403 Forbidden

Authentication succeeded but authorization failed.

**Common Causes:**
- Insufficient permissions for operation
- Policy denies access
- Ledger access restricted

**Example:**
```json
{
  "error": "access denied (403)",
  "status": 403,
  "@type": "err:db/Forbidden"
}
```

**How to Fix:**
- Verify user has required permissions
- Check policy configuration
- Contact administrator for access

#### 404 Not Found

The requested resource doesn't exist.

**Common Causes:**
- Ledger doesn't exist
- Entity not found
- Endpoint doesn't exist

**Example:**
```json
{
  "error": "Ledger not found: mydb:main",
  "status": 404,
  "@type": "err:db/LedgerNotFound"
}
```

**How to Fix:**
- Verify ledger name spelling
- Check if ledger was created
- Verify entity IRI

#### 408 Request Timeout

The request took too long to process.

**Common Causes:**
- Query timeout exceeded
- Complex query taking too long
- Database under heavy load

**Example:**
```json
{
  "error": "Query execution exceeded timeout",
  "status": 408,
  "@type": "err:db/Timeout"
}
```

**How to Fix:**
- Simplify query
- Add more specific filters
- Use LIMIT clause
- Increase timeout setting
- Check server load

#### 409 Conflict

The request conflicts with current server state.

**Common Causes:**
- Concurrent modification conflict
- Ledger already exists
- Resource state conflict

**Example:**
```json
{
  "error": "Ledger already exists: mydb:main",
  "status": 409,
  "@type": "err:db/LedgerExists"
}
```

**How to Fix:**
- Use different ledger name
- Handle concurrent modifications with retry logic
- Check resource state before modifying

#### 413 Payload Too Large

The request or response exceeds size limits.

**Common Causes:**
- Transaction too large
- Query result too large
- Request body exceeds limit

**Example:**
```json
{
  "error": "request body exceeds configured limit",
  "status": 413,
  "@type": "err:db/PayloadTooLarge"
}
```

**How to Fix:**
- Split large transactions into batches
- Use LIMIT clause for queries
- Use pagination for large result sets
- Increase size limits (if appropriate)

#### 415 Unsupported Media Type

The Content-Type is not supported.

**Common Causes:**
- Wrong Content-Type header
- Unsupported format
- Missing Content-Type header

**Example:**
```json
{
  "error": "Content-Type not supported: text/plain",
  "status": 415,
  "@type": "err:db/UnsupportedMediaType"
}
```

**How to Fix:**
- Set correct Content-Type header
- Use supported format
- Check API documentation for supported types

#### 422 Unprocessable Entity

The request is well-formed but semantically invalid.

**Common Causes:**
- Invalid data values
- Business rule violation
- Semantic constraint violation

**Example:**
```json
{
  "error": "semantic constraint violation",
  "status": 422,
  "@type": "err:db/ConstraintViolation"
}
```

**How to Fix:**
- Validate data before submitting
- Check business rules
- Review constraint requirements

#### 429 Too Many Requests

Rate limit exceeded.

**Common Causes:**
- Too many requests in time window
- Exceeded quota

**Example:**
```json
{
  "error": "rate limit exceeded",
  "status": 429,
  "@type": "err:db/RateLimited"
}
```

**Response Headers:**
```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1642857645
Retry-After: 45
```

**How to Fix:**
- Wait before retrying (check Retry-After header)
- Implement exponential backoff
- Reduce request rate
- Request higher rate limit

### Server Error Codes (5xx)

#### 500 Internal Server Error

An unexpected error occurred on the server.

**Common Causes:**
- Unhandled exception
- Database error
- Internal logic error

**Example:**
```json
{
  "error": "internal error",
  "status": 500,
  "@type": "err:db/Internal"
}
```

**How to Fix:**
- Check server logs
- Report to system administrator
- Retry request
- Contact support if persists

#### 502 Bad Gateway

Error communicating with upstream service.

**Common Causes:**
- Storage backend unavailable
- Nameservice unavailable
- Network error

**Example:**
```json
{
  "error": "upstream service error",
  "status": 502,
  "@type": "err:db/BadGateway"
}
```

**How to Fix:**
- Check storage backend status
- Verify network connectivity
- Check AWS/cloud service status
- Retry with backoff

#### 503 Service Unavailable

The server is temporarily unavailable.

**Common Causes:**
- Server overloaded
- Maintenance mode
- Resource exhaustion

**Example:**
```json
{
  "error": "service unavailable",
  "status": 503,
  "@type": "err:db/ServiceUnavailable"
}
```

**Response Headers:**
```http
Retry-After: 300
```

**How to Fix:**
- Wait and retry (check Retry-After header)
- Implement retry logic with exponential backoff
- Check service status page

#### 504 Gateway Timeout

Upstream service didn't respond in time.

**Common Causes:**
- Storage backend timeout
- Long-running query
- Network latency

**Example:**
```json
{
  "error": "gateway timeout",
  "status": 504,
  "@type": "err:db/GatewayTimeout"
}
```

**How to Fix:**
- Retry request
- Check storage backend performance
- Simplify query
- Increase timeout settings

## Error Handling Best Practices

### 1. Always Check Status Codes

Check HTTP status before parsing response:

```javascript
const response = await fetch(url, options);
if (!response.ok) {
  const err = await response.json();
  // err.error is the primary human-readable message, err["@type"] is the stable category.
  throw new Error(`${err["@type"] || "err:unknown"}: ${err.error}`);
}
```

### 2. Implement Retry Logic

Retry transient errors with exponential backoff:

```javascript
async function retryRequest(fn, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (err) {
      if (!isRetryable(err) || i === maxRetries - 1) {
        throw err;
      }
      await sleep(Math.pow(2, i) * 1000);
    }
  }
}

function isRetryable(err) {
  return [408, 429, 502, 503, 504].includes(err.status);
}
```

### 3. Handle Rate Limits

Respect rate limit headers:

```javascript
if (response.status === 429) {
  const retryAfter = response.headers.get('Retry-After');
  await sleep(retryAfter * 1000);
  return retryRequest(fn);
}
```

### 4. Log Error Details

Log complete error context for debugging:

```javascript
console.error({
  status: response.status,
  error: errorData.error,
  error_type: errorData["@type"],
  cause: errorData.cause,
  requestId: response.headers.get('X-Request-ID')
});
```

### 5. User-Friendly Messages

Show appropriate messages to users:

```javascript
function getUserMessage(error) {
  switch (error["@type"]) {
    case 'err:db/LedgerNotFound':
      return 'Database not found. Please check the name.';
    case 'err:db/Timeout':
      return 'Query took too long. Please try a simpler query.';
    case 'err:db/RateLimited':
      return 'Too many requests. Please wait a moment.';
    default:
      return 'An error occurred. Please try again.';
  }
}
```

### 6. Graceful Degradation

Handle errors gracefully:

```javascript
try {
  const data = await query(ledger);
  return data;
} catch (err) {
  if (err["@type"] === 'err:db/LedgerNotFound') {
    // Create ledger and retry
    await createLedger(ledger);
    return await query(ledger);
  }
  throw err;
}
```

### 7. Circuit Breaker Pattern

Prevent cascading failures:

```javascript
class CircuitBreaker {
  constructor(threshold = 5, timeout = 60000) {
    this.failures = 0;
    this.threshold = threshold;
    this.timeout = timeout;
    this.state = 'CLOSED';
  }
  
  async execute(fn) {
    if (this.state === 'OPEN') {
      throw new Error('Circuit breaker is OPEN');
    }
    
    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      throw err;
    }
  }
  
  onSuccess() {
    this.failures = 0;
    this.state = 'CLOSED';
  }
  
  onFailure() {
    this.failures++;
    if (this.failures >= this.threshold) {
      this.state = 'OPEN';
      setTimeout(() => {
        this.state = 'HALF_OPEN';
        this.failures = 0;
      }, this.timeout);
    }
  }
}
```

## Related Documentation

- [Overview](overview.md) - API overview
- [Endpoints](endpoints.md) - API endpoints
- [Signed Requests](signed-requests.md) - Authentication
- [Troubleshooting](../troubleshooting/README.md) - General troubleshooting
- [Common Errors](../troubleshooting/common-errors.md) - Common error solutions
