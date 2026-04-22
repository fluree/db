# Verifiable Data

**Differentiator**: Fluree supports cryptographically signed transactions using industry-standard formats (JWS and Verifiable Credentials), enabling tamper-proof audit trails and trustless data exchange. Every transaction can be cryptographically verified, providing cryptographic proof of data provenance and integrity.

**Note:** Requires the `credential` feature flag. See [Compatibility and Feature Flags](../reference/compatibility.md#fluree-db-api-features).

## What Is Verifiable Data?

**Verifiable data** in Fluree refers to transactions that are cryptographically signed, providing proof of:
- **Authenticity**: Who created the transaction
- **Integrity**: That the data hasn't been tampered with
- **Non-repudiation**: The signer cannot deny creating the transaction
- **Provenance**: The origin and history of the data

### Key Characteristics

- **Cryptographic Signatures**: Transactions signed using standard cryptographic algorithms
- **Industry Standards**: Support for JWS (JSON Web Signatures) and Verifiable Credentials (VC)
- **Tamper-Proof**: Any modification to signed data invalidates the signature
- **Verifiable**: Anyone can verify signatures without special access

## Why Verifiable Data Matters

### Traditional Database Limitations

Most databases provide:
- **Authentication**: Who can access the database
- **Authorization**: What they can do
- **Audit Logs**: What happened (but logs can be modified)

**Problems:**
- No cryptographic proof of data origin
- Audit logs can be tampered with
- Difficult to prove data integrity
- No way to verify data across systems

### Fluree's Approach

Fluree provides:
- **Cryptographic Signatures**: Every transaction can be signed
- **Tamper-Proof History**: Signed transactions cannot be modified
- **Verifiable Provenance**: Anyone can verify data origin
- **Trustless Exchange**: Data can be shared without trusting intermediaries

**Benefits:**
- **Audit Compliance**: Cryptographic proof for compliance requirements
- **Data Integrity**: Detect any tampering with data
- **Trustless Systems**: Enable trustless data exchange
- **Provenance Tracking**: Track data origin cryptographically

## Signed Transactions

### JWS (JSON Web Signatures)

Fluree supports JWS for signing transactions:

**Transaction Structure:**

```json
{
  "ledger": "mydb:main",
  "tx": [
    {
      "@id": "ex:alice",
      "ex:name": "Alice"
    }
  ],
  "signature": {
    "protected": {
      "alg": "ES256",
      "kid": "key-1"
    },
    "signature": "base64-encoded-signature"
  }
}
```

**Verification:**
- Extract signature from transaction
- Verify signature using signer's public key
- Confirm transaction hasn't been modified

### Verifiable Credentials

Fluree supports Verifiable Credentials (VC) for credential-based transactions:

**VC Structure:**

```json
{
  "@context": [
    "https://www.w3.org/2018/credentials/v1"
  ],
  "type": ["VerifiableCredential"],
  "credentialSubject": {
    "@id": "ex:alice",
    "ex:name": "Alice"
  },
  "proof": {
    "type": "Ed25519Signature2020",
    "created": "2024-01-15T10:00:00Z",
    "verificationMethod": "did:example:alice#key-1",
    "proofValue": "base64-encoded-signature"
  }
}
```

**Verification:**
- Verify credential proof
- Check credential issuer
- Validate credential structure
- Confirm credential hasn't been revoked

## Transaction Signing

### Signing a Transaction

**Step 1: Prepare Transaction**

```json
{
  "ledger": "mydb:main",
  "tx": [
    {
      "@id": "ex:alice",
      "ex:name": "Alice"
    }
  ]
}
```

**Step 2: Create Signature**

```javascript
// Pseudo-code
const payload = JSON.stringify(tx);
const signature = sign(payload, privateKey);
```

**Step 3: Add Signature**

```json
{
  "ledger": "mydb:main",
  "tx": [...],
  "signature": {
    "protected": {
      "alg": "ES256",
      "kid": "key-1"
    },
    "signature": signature
  }
}
```

### Signature Algorithms

Fluree supports standard signature algorithms:

- **ES256**: ECDSA with P-256 and SHA-256
- **ES384**: ECDSA with P-384 and SHA-384
- **ES512**: ECDSA with P-521 and SHA-512
- **Ed25519**: EdDSA with Ed25519 curve

### Key Management

**Public Key Storage:**

Public keys can be stored:
- In the ledger itself (as data)
- In a separate key registry
- In a DID (Decentralized Identifier) document

**Example Public Key in Ledger:**

```json
{
  "@id": "ex:alice",
  "ex:publicKey": {
    "kty": "EC",
    "crv": "P-256",
    "x": "base64-x",
    "y": "base64-y"
  }
}
```

## Transaction Verification

### Verifying a Signed Transaction

**Step 1: Extract Signature**

```json
{
  "signature": {
    "protected": {...},
    "signature": "base64-signature"
  }
}
```

**Step 2: Get Public Key**

```javascript
// Pseudo-code
const kid = signature.protected.kid;
const publicKey = getPublicKey(kid);
```

**Step 3: Verify Signature**

```javascript
// Pseudo-code
const payload = JSON.stringify(tx);
const isValid = verify(payload, signature.signature, publicKey);
```

### Verification in Fluree

Fluree automatically verifies signed transactions:

1. **Signature Extraction**: Extract signature from transaction
2. **Key Resolution**: Resolve public key from signature
3. **Signature Verification**: Verify cryptographic signature
4. **Transaction Acceptance**: Accept transaction if signature valid

**If verification fails:**
- Transaction is rejected
- Error returned to client
- No data is committed

## Use Cases

### Audit Compliance

**Requirement**: Cryptographic proof of all data changes

**Solution**: Sign all transactions

```json
{
  "ledger": "audit:main",
  "tx": [
    {
      "@id": "ex:change1",
      "ex:action": "update",
      "ex:timestamp": "2024-01-15T10:00:00Z"
    }
  ],
  "signature": {...}
}
```

**Benefits:**
- Cryptographic proof of changes
- Tamper-proof audit trail
- Compliance with regulations

### Trustless Data Exchange

**Requirement**: Share data without trusting intermediaries

**Solution**: Sign data at source

```json
{
  "ledger": "shared:main",
  "tx": [
    {
      "@id": "ex:data1",
      "ex:value": "sensitive-data",
      "ex:source": "ex:system-a"
    }
  ],
  "signature": {
    "protected": {
      "kid": "ex:system-a#key-1"
    },
    "signature": "..."
  }
}
```

**Benefits:**
- Verify data origin
- Detect tampering
- Trustless data sharing

### Multi-Party Systems

**Requirement**: Multiple parties contribute data

**Solution**: Each party signs their transactions

```json
{
  "ledger": "consortium:main",
  "tx": [
    {
      "@id": "ex:contribution1",
      "ex:party": "ex:party-a",
      "ex:data": "..."
    }
  ],
  "signature": {
    "protected": {
      "kid": "ex:party-a#key-1"
    },
    "signature": "..."
  }
}
```

**Benefits:**
- Identify data contributors
- Verify party contributions
- Enable accountability

### Regulatory Compliance

**Requirement**: Prove data integrity for regulations

**Solution**: Sign all regulated data

**Examples:**
- **HIPAA**: Healthcare data integrity
- **GDPR**: Personal data provenance
- **SOX**: Financial data integrity
- **FDA**: Pharmaceutical data integrity

## Verifiable Credentials

### Credential Structure

Verifiable Credentials follow W3C VC standard:

```json
{
  "@context": [
    "https://www.w3.org/2018/credentials/v1"
  ],
  "id": "ex:credential-1",
  "type": ["VerifiableCredential", "ex:IdentityCredential"],
  "issuer": "did:example:issuer",
  "issuanceDate": "2024-01-15T10:00:00Z",
  "credentialSubject": {
    "@id": "ex:alice",
    "ex:name": "Alice",
    "ex:email": "alice@example.com"
  },
  "proof": {
    "type": "Ed25519Signature2020",
    "created": "2024-01-15T10:00:00Z",
    "verificationMethod": "did:example:issuer#key-1",
    "proofValue": "base64-signature"
  }
}
```

### Credential Verification

**Step 1: Verify Proof**

```javascript
// Pseudo-code
const proof = credential.proof;
const publicKey = resolvePublicKey(proof.verificationMethod);
const isValid = verifyProof(credential, proof, publicKey);
```

**Step 2: Check Issuer**

```javascript
// Pseudo-code
const issuer = credential.issuer;
const isTrusted = checkIssuerTrust(issuer);
```

**Step 3: Validate Credential**

```javascript
// Pseudo-code
const isValid = validateCredentialStructure(credential);
```

### Credential Revocation

Credentials can be revoked:

```json
{
  "@id": "ex:revocation-1",
  "@type": "ex:CredentialRevocation",
  "ex:credentialId": "ex:credential-1",
  "ex:revokedAt": "2024-01-20T10:00:00Z"
}
```

Verification should check revocation status.

## Data Provenance

### Tracking Data Origin

Signed transactions enable provenance tracking:

**Query Transaction History:**

```sparql
SELECT ?tx ?signer ?timestamp
WHERE {
  ?tx ex:signature ?sig .
  ?sig ex:signer ?signer .
  ?tx ex:timestamp ?timestamp .
}
ORDER BY DESC(?timestamp)
```

**Verify Data Chain:**

```sparql
SELECT ?data ?origin ?signer
WHERE {
  ?data ex:origin ?origin .
  ?origin ex:signature ?sig .
  ?sig ex:signer ?signer .
}
```

### Provenance Verification

**Step 1: Find Data Origin**

```sparql
SELECT ?tx
WHERE {
  ?tx ex:created ?data .
}
```

**Step 2: Verify Transaction Signature**

```javascript
// Pseudo-code
const tx = getTransaction(txId);
const isValid = verifySignature(tx);
```

**Step 3: Trace Provenance Chain**

```sparql
SELECT ?chain
WHERE {
  ?data ex:provenance ?chain .
  ?chain ex:signature ?sig .
}
```

## Best Practices

### Key Management

1. **Secure Storage**: Store private keys securely
2. **Key Rotation**: Rotate keys regularly
3. **Key Backup**: Backup keys securely
4. **Key Recovery**: Plan for key recovery

### Signature Practices

1. **Always Sign**: Sign all important transactions
2. **Verify Before Trust**: Always verify signatures
3. **Standard Algorithms**: Use standard signature algorithms
4. **Key Identification**: Use clear key identifiers

### Credential Management

1. **Issuer Trust**: Establish issuer trust relationships
2. **Credential Validation**: Validate credential structure
3. **Revocation Checking**: Check revocation status
4. **Credential Storage**: Store credentials securely

### Compliance

1. **Audit Logging**: Log all signature verifications
2. **Provenance Tracking**: Track data provenance
3. **Regulatory Alignment**: Align with regulations
4. **Documentation**: Document verification processes

## Comparison with Traditional Approaches

### Traditional Audit Logs

**Traditional Approach:**
- Logs stored in database
- Can be modified by admins
- No cryptographic proof
- Difficult to verify

**Problems:**
- Logs can be tampered with
- No proof of authenticity
- Difficult to verify
- Not suitable for trustless systems

### Fluree Verifiable Data

**Fluree Approach:**
- Transactions cryptographically signed
- Signatures cannot be forged
- Anyone can verify
- Suitable for trustless systems

**Benefits:**
- Tamper-proof history
- Cryptographic proof
- Easy verification
- Trustless data exchange

## Architecture

### Signature Storage

Signatures are stored with transactions:

- **Transaction Metadata**: Signature stored in transaction metadata
- **Queryable**: Signatures can be queried like any data
- **Versioned**: Signature history tracked over time

### Verification Engine

The verification engine:

- **Automatic Verification**: Verifies signatures automatically
- **Key Resolution**: Resolves public keys from signatures
- **Standard Compliance**: Follows JWS and VC standards

### API Integration

Verification integrated with:

- **Transaction API**: Verifies signatures on transaction submission
- **Query API**: Can query signature information
- **Admin API**: Administrative operations on signatures

Verifiable data makes Fluree uniquely suited for applications requiring cryptographic proof of data integrity, audit compliance, and trustless data exchange. By supporting industry-standard signature formats, Fluree enables integration with existing identity systems and credential ecosystems.
