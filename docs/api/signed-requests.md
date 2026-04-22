# Signed Requests (JWS/VC)

Fluree supports cryptographically signed requests using **JSON Web Signatures (JWS)** and **Verifiable Credentials (VC)**. This provides tamper-proof authentication and enables trustless data exchange.

**Note:** Requires the `credential` feature flag. See [Compatibility and Feature Flags](../reference/compatibility.md#fluree-db-api-features).

## Why Sign Requests?

Signed requests provide:

- **Authentication**: Prove the identity of the request sender
- **Integrity**: Ensure the request hasn't been tampered with
- **Non-repudiation**: Sender cannot deny sending the request
- **Authorization**: Cryptographically link requests to specific identities
- **Auditability**: Complete audit trail of who did what

## JSON Web Signatures (JWS)

JWS is an IETF standard (RFC 7515) for representing digitally signed content as JSON.

### JWS Structure

A JWS consists of three parts:

1. **Protected Header**: Metadata about the signature (base64url-encoded)
2. **Payload**: The actual content being signed (base64url-encoded)
3. **Signature**: Cryptographic signature (base64url-encoded)

**Compact Serialization:**
```
eyJhbGciOiJFZDI1NTE5In0.eyJmcm9tIjoibXlkYjptYWluIn0.c2lnbmF0dXJl
|_______header_______|.|______payload______|.|_signature_|
```

**JSON Serialization:**
```json
{
  "payload": "eyJmcm9tIjoibXlkYjptYWluIn0",
  "signatures": [
    {
      "protected": "eyJhbGciOiJFZDI1NTE5In0",
      "signature": "c2lnbmF0dXJl"
    }
  ]
}
```

### Supported Algorithm

Fluree uses **EdDSA (Ed25519)** for JWS verification. All signed requests must use `"alg": "EdDSA"` in the protected header.

## Creating Signed Requests

### Step 1: Prepare the Payload

Create your query or transaction as usual:

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

### Step 2: Encode the Payload

Base64url-encode the JSON payload:

```javascript
const payload = JSON.stringify(query);
const encodedPayload = base64url.encode(payload);
```

### Step 3: Create the Protected Header

Create a header specifying the algorithm and key ID:

```json
{
  "alg": "EdDSA",
  "kid": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
}
```

Base64url-encode the header:

```javascript
const header = JSON.stringify({ alg: "EdDSA", kid: keyId });
const encodedHeader = base64url.encode(header);
```

### Step 4: Sign

Create the signing input and sign it:

```javascript
const signingInput = encodedHeader + "." + encodedPayload;
const signature = sign(signingInput, privateKey);
const encodedSignature = base64url.encode(signature);
```

### Step 5: Construct the JWS

Create the complete JWS:

**Compact Format:**
```javascript
const jws = encodedHeader + "." + encodedPayload + "." + encodedSignature;
```

**JSON Format:**
```json
{
  "payload": "<encodedPayload>",
  "signatures": [
    {
      "protected": "<encodedHeader>",
      "signature": "<encodedSignature>"
    }
  ]
}
```

### Step 6: Send the Request

Send the JWS to Fluree:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/jose" \
  -d '{
    "payload": "eyJmcm9tIjoibXlkYjptYWluIn0...",
    "signatures": [{
      "protected": "eyJhbGciOiJFZDI1NTE5In0...",
      "signature": "c2lnbmF0dXJl..."
    }]
  }'
```

## Verifiable Credentials (VC)

Verifiable Credentials are a W3C standard for cryptographically verifiable digital credentials.

### VC Structure

A Verifiable Credential includes:

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
    "flureeAction": {
      "query": {
        "from": "mydb:main",
        "select": ["?name"],
        "where": [...]
      }
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

### Creating a Verifiable Credential

Use a VC library to create signed credentials:

```javascript
import { issue } from '@digitalbazaar/vc';

const credential = {
  '@context': ['https://www.w3.org/2018/credentials/v1'],
  type: ['VerifiableCredential'],
  issuer: didKey,
  issuanceDate: new Date().toISOString(),
  credentialSubject: {
    id: didKey,
    flureeAction: {
      query: queryObject
    }
  }
};

const verifiableCredential = await issue({
  credential,
  suite: new Ed25519Signature2020({ key: keyPair }),
  documentLoader
});
```

### Sending a VC

Send the VC to Fluree:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/vc+ld+json" \
  -d '{
    "@context": ["https://www.w3.org/2018/credentials/v1"],
    "type": ["VerifiableCredential"],
    ...
  }'
```

## Decentralized Identifiers (DIDs)

Fluree uses DIDs to identify public keys.

### Supported DID Methods

**did:key** - Public key embedded in the DID (recommended):
```
did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
```

**did:web** - Web-based DID resolution:
```
did:web:example.com:users:alice
```

**did:ion** - ION network DIDs (future support):
```
did:ion:EiClkZMDxPKqC9c-umQfTkR8vvZ9JPhl_xLDI9Nfk38w5w
```

### DID Resolution

Fluree resolves DIDs to public keys:

1. **did:key**: Public key extracted directly from DID
2. **did:web**: Fetched from `https://example.com/.well-known/did.json`
3. **did:ion**: Resolved via ION network

### Public Key Registration

For production use, register public keys with Fluree:

```bash
curl -X POST http://localhost:8090/admin/keys \
  -H "Content-Type: application/json" \
  -d '{
    "did": "did:key:z6Mkh...",
    "publicKey": "...",
    "algorithm": "EdDSA",
    "permissions": ["query", "transact"]
  }'
```

## Request Verification

### Verification Process

When Fluree receives a signed request:

1. **Extract the signature and header**
2. **Resolve the key ID (kid) to a public key**
3. **Verify the signature** using the public key
4. **Check expiration** (if `exp` claim present)
5. **Validate issuer** (if required)
6. **Apply authorization policies** based on DID

### Verification Failure

If verification fails:

**Status Code:** `401 Unauthorized`

**Response:**
```json
{
  "error": "Invalid signature",
  "status": 401,
  "@type": "err:auth/InvalidSignature"
}
```

## Key Management

### Generating Keys

**Ed25519 (EdDSA):**

```javascript
import { generateKeyPair } from '@stablelib/ed25519';

const keyPair = generateKeyPair();
// keyPair.publicKey - 32 bytes
// keyPair.secretKey - 64 bytes
```

### Storing Keys

**Secure Storage:**
- Hardware Security Modules (HSM)
- Key Management Services (AWS KMS, Azure Key Vault)
- Encrypted files with strong passphrases
- Hardware wallets for blockchain-based DIDs

**Never:**
- Store private keys in code
- Commit keys to version control
- Send keys over insecure channels
- Share keys between applications

### Key Rotation

Rotate keys regularly:

1. Generate new key pair
2. Register new public key with Fluree
3. Update client to use new key
4. Revoke old key after transition period
5. Remove old key from Fluree

## Authorization with Signed Requests

### Identity-Based Policies

Fluree policies can use the signer's DID for authorization:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "f": "https://ns.flur.ee/db#"
  },
  "@id": "ex:admin-policy",
  "f:policy": [
    {
      "f:subject": "did:key:z6Mkh...",
      "f:action": ["query", "transact"],
      "f:allow": true
    }
  ]
}
```

### Role-Based Access

Link DIDs to roles:

```json
{
  "@id": "did:key:z6Mkh...",
  "@type": "ex:User",
  "ex:role": "ex:Administrator"
}
```

Policy checks the role:

```json
{
  "f:policy": [
    {
      "f:subject": { "ex:role": "ex:Administrator" },
      "f:action": "*",
      "f:allow": true
    }
  ]
}
```

## Code Examples

### JavaScript/TypeScript

```typescript
import jose from 'jose';

async function signQuery(query: object, privateKey: Uint8Array) {
  const payload = JSON.stringify(query);
  
  const jws = await new jose.SignJWT(query)
    .setProtectedHeader({ alg: 'EdDSA', kid: 'did:key:z6Mkh...' })
    .setIssuedAt()
    .setExpirationTime('5m')
    .sign(privateKey);
  
  return jws;
}

// Send signed request
const signedQuery = await signQuery(query, privateKey);
const response = await fetch('http://localhost:8090/v1/fluree/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/jose' },
  body: signedQuery
});
```

### Python

```python
from jwcrypto import jwk, jws
import json

def sign_query(query, private_key):
    # Create JWK from private key
    key = jwk.JWK.from_json(private_key)
    
    # Create JWS
    payload = json.dumps(query).encode('utf-8')
    jws_token = jws.JWS(payload)
    jws_token.add_signature(key, alg='EdDSA', 
                           protected=json.dumps({"kid": "did:key:z6Mkh..."}))
    
    return jws_token.serialize()

# Send signed request
signed_query = sign_query(query, private_key)
response = requests.post('http://localhost:8090/v1/fluree/query',
                        headers={'Content-Type': 'application/jose'},
                        data=signed_query)
```

## Best Practices

### 1. Use EdDSA (Ed25519)

EdDSA provides:
- Excellent security (128-bit security level)
- Fast signing and verification
- Small signatures (64 bytes)
- Deterministic (no random number generation needed)

### 2. Include Expiration

Always set an expiration time:

```json
{
  "alg": "EdDSA",
  "exp": 1642857600
}
```

### 3. Use Short Expiration Times

For interactive requests: 5-15 minutes
For batch processes: 1-24 hours
Never: No expiration

### 4. Rotate Keys Regularly

Rotate signing keys every 90-180 days.

### 5. Secure Key Storage

Use proper key management:
- Development: Encrypted local storage
- Production: HSM or KMS

### 6. Validate on Server

Never trust client-side validation alone. Fluree always validates signatures server-side.

### 7. Use HTTPS

Always use HTTPS with signed requests to prevent replay attacks.

### 8. Implement Nonce/JTI

Include a unique identifier to prevent replay:

```json
{
  "alg": "EdDSA",
  "jti": "unique-request-id-12345"
}
```

## Troubleshooting

### "Invalid Signature" Error

**Causes:**
- Wrong private key used
- Payload modified after signing
- Incorrect base64url encoding
- Algorithm mismatch

**Solution:** Verify the signing process end-to-end.

### "Key Not Found" Error

**Causes:**
- DID not registered with Fluree
- Incorrect key ID (kid) in header
- DID resolution failed

**Solution:** Register public key or check DID format.

### "Signature Expired" Error

**Causes:**
- Request sent after expiration time
- Clock skew between client and server

**Solution:** Use NTP to sync clocks, increase expiration time.

## Related Documentation

- [Overview](overview.md) - API overview
- [Endpoints](endpoints.md) - API endpoints
- [Headers](headers.md) - HTTP headers
- [Security](../security/README.md) - Policy and access control
- [Verifiable Data](../concepts/verifiable-data.md) - Verifiable credentials concepts
