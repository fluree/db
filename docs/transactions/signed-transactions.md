# Signed / Credentialed Transactions

Fluree supports cryptographically signed transactions using **JSON Web Signatures (JWS)** and **Verifiable Credentials (VC)**. Signed transactions provide authentication, integrity, and non-repudiation for all transaction operations.

## Why Sign Transactions?

Signed transactions provide:

- **Authentication**: Prove who submitted the transaction
- **Integrity**: Ensure transaction hasn't been tampered with
- **Non-repudiation**: Transaction author cannot deny authorship
- **Authorization**: Link transaction to specific identity for policy enforcement
- **Audit Trail**: Complete provenance of all data changes

## Basic Signed Transaction

### Step 1: Create Transaction

Create your transaction as normal:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice"
    }
  ]
}
```

### Step 2: Sign with JWS

Sign the transaction using JWS:

```javascript
import jose from 'jose';

const privateKey = ... // Your Ed25519 private key

const jws = await new jose.SignJWT(transaction)
  .setProtectedHeader({
    alg: 'EdDSA',
    kid: 'did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK'
  })
  .setIssuedAt()
  .setExpirationTime('15m')
  .sign(privateKey);
```

### Step 3: Submit

Submit the signed transaction:

```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/jose" \
  -d "$jws"
```

## JWS Format

### Compact Serialization

```text
eyJhbGciOiJFZDI1NTE5IiwidHlwIjoiSldUIn0.eyJAY29udGV4dCI6eyJleCI6Imh0...
```

Three base64url-encoded parts separated by dots:
1. Header (algorithm, key ID)
2. Payload (transaction)
3. Signature

### JSON Serialization

```json
{
  "payload": "eyJAY29udGV4dCI6eyJleCI6Imh0...",
  "signatures": [
    {
      "protected": "eyJhbGciOiJFZDI1NTE5In0",
      "signature": "c2lnbmF0dXJl..."
    }
  ]
}
```

## Verifiable Credentials

Use W3C Verifiable Credentials for transactions:

```json
{
  "@context": [
    "https://www.w3.org/2018/credentials/v1"
  ],
  "type": ["VerifiableCredential"],
  "issuer": "did:key:z6Mkh...",
  "issuanceDate": "2024-01-22T10:00:00Z",
  "credentialSubject": {
    "id": "did:key:z6Mkh...",
    "flureeTransaction": {
      "@context": {
        "ex": "http://example.org/ns/"
      },
      "@graph": [
        { "@id": "ex:alice", "schema:name": "Alice" }
      ]
    }
  },
  "proof": {
    "type": "Ed25519Signature2020",
    "created": "2024-01-22T10:00:00Z",
    "verificationMethod": "did:key:z6Mkh...#z6Mkh...",
    "proofPurpose": "authentication",
    "proofValue": "z58DAdFfa9SkqZMVP..."
  }
}
```

Submit with:
```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/vc+ld+json" \
  -d @credential.json
```

## Supported Algorithm

**EdDSA (Ed25519):**
- Fast, secure, deterministic
- 64-byte signatures
- 128-bit security level

## Identity Management

### Decentralized Identifiers (DIDs)

Use DIDs to identify transaction authors:

**did:key** (simplest):
```
did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
```

**did:web** (organization-managed):
```
did:web:example.com:users:alice
```

**did:ion** (blockchain-based):
```
did:ion:EiClkZMDxPKqC9c-umQfTkR8vvZ9JPhl_xLDI9Nfk38w5w
```

### Key Resolution

Standalone server signed requests verify Ed25519 JWS material from the request
itself (for example embedded JWK / `did:key`) or configured OIDC/JWKS issuers.
There is no `/admin/keys` registration endpoint.

## Transaction Provenance

Signed transactions include author information in commit metadata:

```json
{
  "t": 42,
  "timestamp": "2024-01-22T10:30:00Z",
  "commit_id": "bafybeig...commitT42",
  "author": "did:key:z6Mkh...",
  "signature": "z58DAdFfa9...",
  "flakes_added": 3,
  "flakes_retracted": 0
}
```

Query provenance:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?t ?author ?timestamp
WHERE {
  ?commit f:t ?t ;
          f:author ?author ;
          f:timestamp ?timestamp .
}
ORDER BY DESC(?t)
```

## Policy-Based Authorization

Use signed transaction author for authorization:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "f": "https://ns.flur.ee/db#"
  },
  "@id": "ex:admin-policy",
  "f:policy": [
    {
      "f:subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
      "f:action": "transact",
      "f:allow": true
    }
  ]
}
```

Only transactions signed by this DID will be accepted.

## Code Examples

### JavaScript/TypeScript

```typescript
import jose from 'jose';
import { Ed25519VerificationKey2020 } from '@digitalbazaar/ed25519-verification-key-2020';

async function signTransaction(transaction: object, privateKey: Uint8Array) {
  const jws = await new jose.SignJWT(transaction)
    .setProtectedHeader({
      alg: 'EdDSA',
      kid: 'did:key:z6Mkh...'
    })
    .setIssuedAt()
    .setExpirationTime('15m')
    .sign(privateKey);
  
  return jws;
}

async function submitSignedTransaction(ledger: string, transaction: object) {
  const signed = await signTransaction(transaction, privateKey);
  
  const response = await fetch(`http://localhost:8090/v1/fluree/upsert?ledger=${ledger}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/jose' },
    body: signed
  });
  
  return await response.json();
}
```

### Python

```python
from jwcrypto import jwk, jws
import json

def sign_transaction(transaction, private_key):
    # Create JWK from private key
    key = jwk.JWK.from_json(private_key)
    
    # Create JWS
    payload = json.dumps(transaction).encode('utf-8')
    jws_token = jws.JWS(payload)
    jws_token.add_signature(
        key,
        alg='EdDSA',
        protected=json.dumps({"kid": "did:key:z6Mkh..."})
    )
    
    return jws_token.serialize()

def submit_signed_transaction(ledger, transaction, private_key):
    signed = sign_transaction(transaction, private_key)
    
    response = requests.post(
        f'http://localhost:8090/v1/fluree/upsert?ledger={ledger}',
        headers={'Content-Type': 'application/jose'},
        data=signed
    )
    
    return response.json()
```

## Verification Process

When Fluree receives a signed transaction:

1. **Extract signature and header**
2. **Resolve key ID (kid) to public key**
3. **Verify signature** using public key
4. **Check expiration** (if exp claim present)
5. **Validate issuer** (if required by policy)
6. **Apply authorization policies** based on DID
7. **Process transaction** if verification succeeds

## Error Handling

### Invalid Signature

```json
{
  "error": "SignatureVerificationFailed",
  "message": "Invalid signature",
  "code": "INVALID_SIGNATURE",
  "details": {
    "kid": "did:key:z6Mkh...",
    "reason": "Signature does not match"
  }
}
```

### Expired Transaction

```json
{
  "error": "TokenExpired",
  "message": "Transaction signature expired",
  "code": "TOKEN_EXPIRED",
  "details": {
    "exp": 1642857600,
    "now": 1642858000
  }
}
```

### Key Not Found

```json
{
  "error": "KeyNotFound",
  "message": "Public key not registered",
  "code": "KEY_NOT_FOUND",
  "details": {
    "kid": "did:key:z6Mkh..."
  }
}
```

### Unauthorized

```json
{
  "error": "Forbidden",
  "message": "Policy denies transact permission",
  "code": "POLICY_DENIED",
  "details": {
    "subject": "did:key:z6Mkh...",
    "action": "transact",
    "ledger": "mydb:main"
  }
}
```

## Best Practices

### 1. Use EdDSA (Ed25519)

Best security and performance:
```javascript
{
  "alg": "EdDSA",
  "kid": "did:key:z6Mkh..."
}
```

### 2. Set Expiration

Always include expiration:
```javascript
.setExpirationTime('15m')  // 15 minutes
```

### 3. Secure Key Storage

Never hardcode private keys:

Good:
```javascript
const privateKey = await loadKeyFromSecureStorage();
```

Bad:
```javascript
const privateKey = "hardcoded-key-here";
```

### 4. Use did:key for Simplicity

For simple deployments:
```
did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
```

### 5. Implement Key Rotation

Rotate keys every 90-180 days:

```javascript
async function rotateKey() {
  const newKey = generateKeyPair();
  await registerKey(newKey.publicKey);
  await revokeKey(oldKey.kid);
  updateApplicationKey(newKey);
}
```

### 6. Include Request ID

Add unique ID to prevent replay:

```javascript
.setClaim('jti', crypto.randomUUID())
```

### 7. Use HTTPS

Always use HTTPS with signed transactions to prevent replay attacks.

## Compliance and Auditing

### Complete Audit Trail

Signed transactions provide complete audit trail:

```sparql
SELECT ?t ?author ?timestamp ?action
WHERE {
  ?commit f:t ?t ;
          f:author ?author ;
          f:timestamp ?timestamp .
  ?commit f:assert ?assertion .
  ?assertion ?predicate ?object .
}
ORDER BY DESC(?t)
```

### Regulatory Compliance

Signed transactions support:
- SOC 2 (audit trails)
- HIPAA (data provenance)
- GDPR (data processing records)
- PCI DSS (transaction logs)

### Non-Repudiation

Cryptographic signatures provide non-repudiation:
- Author cannot deny submitting transaction
- Tampering is detectable
- Legal admissibility in disputes

## Related Documentation

- [API: Signed Requests](../api/signed-requests.md) - HTTP API details
- [Commit Signing and Attestation](../security/commit-signing.md) - Infrastructure-level commit signatures
- [Security: Policy Model](../security/policy-model.md) - Authorization policies
- [Verifiable Data](../concepts/verifiable-data.md) - Cryptographic verification concepts
- [Commit Receipts](commit-receipts.md) - Transaction metadata
