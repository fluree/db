# Policy Enforcement

**Differentiator**: Fluree's policy system provides fine-grained, data-level access control that is enforced at query time, not at the application layer. This enables secure multi-tenant deployments, compliance with data privacy regulations, and trustless data sharing where users can only access data they're authorized to see.

## What Is Policy Enforcement?

**Policy enforcement** in Fluree is a declarative access control system that filters query results based on rules defined in the data itself. Unlike traditional database access control that operates at the table or row level, Fluree policies operate at the triple (fact) level, providing unprecedented granularity.

### Key Characteristics

- **Data-Level Control**: Policies control access to individual facts (triples), not entire records
- **Query-Time Enforcement**: Policies are evaluated during query execution, not at the application layer
- **Declarative Rules**: Policies are expressed as data in the ledger, making them queryable and auditable
- **Context-Aware**: Policies can consider user identity, roles, data relationships, and more

## Why Policy Enforcement Matters

### Traditional Access Control Limitations

Most databases provide access control at coarse granularity:

- **Database-level**: All or nothing access
- **Table-level**: Access to entire tables
- **Row-level**: Access to entire rows (still coarse for graph data)

**Problems:**
- Over-privileged access (users see more than needed)
- Complex application logic to filter results
- Security vulnerabilities if application logic is bypassed
- Difficult to audit who can access what

### Fluree's Approach

Fluree policies operate at the **triple level**:

- Control access to individual facts
- Enforced automatically by the query engine
- Cannot be bypassed by application code
- Fully auditable (policies are data)

**Benefits:**
- Fine-grained security
- Simplified application code
- Compliance-ready (GDPR, HIPAA, etc.)
- Multi-tenant ready

## Policy Model

### Policy Structure

A policy in Fluree consists of:

1. **Target**: What data the policy applies to (subjects, predicates, objects)
2. **Conditions**: When the policy applies (user, role, context)
3. **Actions**: What operations are allowed (read, write, etc.)

### Policy Example

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/ns/"
  },
  "@id": "ex:doctor-patient-policy",
  "@type": "f:Policy",
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": {
    "@type": "ex:Patient",
    "ex:assignedDoctor": "?user"
  },
  "f:condition": [
    { "@id": "?user", "ex:role": "doctor" }
  ],
  "f:allow": true
}
```

This policy allows doctors to read medical records only for patients assigned to them.

## Policy Inputs

Policies have access to contextual information:

### Authentication Context

- **User Identity**: Who is making the request
- **Roles**: What roles the user has
- **Groups**: What groups the user belongs to
- **Attributes**: Custom user attributes

### Query Context

- **Ledger**: Which ledger is being queried
- **Graph**: Which named graph (if applicable)
- **Time**: Transaction time for time-travel queries

### Data Context

- **Subject**: The subject being accessed
- **Predicate**: The predicate being accessed
- **Object**: The object value
- **Relationships**: Related data in the graph

## Policy Evaluation

### Query-Time Enforcement

When a query executes:

1. **Query Parsing**: Query is parsed into patterns
2. **Policy Resolution**: Relevant policies are identified
3. **Pattern Filtering**: Query patterns are filtered based on policies
4. **Result Filtering**: Results are filtered to remove unauthorized data
5. **Result Return**: Only authorized data is returned

### Example Query with Policy

**Query:**

```sparql
SELECT ?patient ?record
WHERE {
  ?patient ex:medicalRecord ?record .
}
```

**Policy Applied:**

- Doctor can only see records for assigned patients
- Patient can only see their own records
- Admin can see all records

**Result:**

Each user sees different results based on their authorization, even with the same query.

## Policy Types

### Subject-Based Policies

Control access based on the subject being accessed:

```json
{
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": { "@type": "ex:Patient", "ex:owner": "?user" },
  "f:allow": true
}
```

### Predicate-Based Policies

Control access to specific predicates:

```json
{
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": { "f:predicate": "ex:salary" },
  "f:condition": [
    { "@id": "?user", "ex:role": "hr-manager" }
  ],
  "f:allow": true
}
```

### Object-Based Policies

Control access based on object values:

```json
{
  "f:action": "query",
  "f:resource": { "ex:classification": "public" },
  "f:allow": true
}
```

### Relationship-Based Policies

Control access based on graph relationships:

```json
{
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": { "@type": "ex:Document", "ex:sharedWith": "?user" },
  "f:allow": true
}
```

## Use Cases

### Multi-Tenant Applications

Each tenant sees only their data:

```json
{
  "f:subject": "?user",
  "f:action": "*",
  "f:resource": { "ex:tenant": "?tenantId" },
  "f:condition": [
    { "@id": "?user", "ex:tenant": "?tenantId" }
  ],
  "f:allow": true
}
```

### Healthcare Compliance (HIPAA)

Doctors can only access patient data they're authorized for:

```json
{
  "f:subject": "?doctor",
  "f:action": "query",
  "f:resource": {
    "@type": "ex:Patient",
    "ex:assignedDoctor": "?doctor"
  },
  "f:condition": [
    { "@id": "?doctor", "ex:role": "doctor" }
  ],
  "f:allow": true
}
```

### Data Privacy (GDPR)

Users can only access their own personal data:

```json
{
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": { "@id": "?user" },
  "f:allow": true
}
```

### Role-Based Access Control (RBAC)

Different roles have different access levels:

```json
[
  {
    "f:subject": "?user",
    "f:action": "*",
    "f:resource": { "f:predicate": "ex:sensitiveData" },
    "f:condition": [
      { "@id": "?user", "ex:role": "admin" }
    ],
    "f:allow": true
  },
  {
    "f:subject": "?user",
    "f:action": "query",
    "f:resource": { "f:predicate": "ex:sensitiveData" },
    "f:condition": [
      { "@id": "?user", "ex:role": "auditor" }
    ],
    "f:allow": true
  }
]
```

## Policy in Queries

### Explicit Policy Context

Queries can specify policy context:

```json
{
  "select": ["?data"],
  "where": [["?subject", "ex:data", "?data"]],
  "policy": {
    "auth": {
      "subject": "ex:user123",
      "roles": ["user"]
    }
  }
}
```

### Implicit Policy Context

When using the HTTP API, policy context comes from authentication:

```http
GET /query?ledger=mydb:main
Authorization: Bearer <JWT token>
```

The JWT token contains the user identity and roles used for policy evaluation.

## Policy in Transactions

Policies can also control write access:

```json
{
  "f:target": {
    "f:predicate": "ex:salary"
  },
  "f:conditions": [
    {
      "f:rule": "f:equals",
      "f:path": ["f:auth", "f:role"],
      "f:value": "hr-manager"
    }
  ],
  "f:actions": ["f:write"]
}
```

This policy allows only HR managers to modify salary data.

## Performance Considerations

### Policy Evaluation Overhead

Policy evaluation adds overhead to queries:

- **Pattern Matching**: Policies must be matched against query patterns
- **Condition Evaluation**: Policy conditions must be evaluated
- **Result Filtering**: Results must be filtered

**Optimization Strategies:**
- Index policies for fast lookup
- Cache policy evaluation results
- Optimize policy conditions
- Use efficient policy rules

### Query Planning

The query planner considers policies:

- **Early Filtering**: Apply policies as early as possible
- **Index Usage**: Use indexes that align with policy filters
- **Join Optimization**: Consider policy constraints in join planning

## Best Practices

### Policy Design

1. **Principle of Least Privilege**: Grant minimum necessary access
2. **Explicit Rules**: Make policies explicit and clear
3. **Test Policies**: Test policies thoroughly before deployment
4. **Document Policies**: Document policy intent and rationale

### Policy Management

1. **Version Control**: Track policy changes over time
2. **Audit Policies**: Regularly audit who can access what
3. **Policy Testing**: Test policies with different user contexts
4. **Performance Monitoring**: Monitor policy evaluation performance

### Security Considerations

1. **Policy Validation**: Validate policy syntax and semantics
2. **Policy Testing**: Test edge cases and boundary conditions
3. **Audit Logging**: Log policy evaluation decisions
4. **Regular Review**: Regularly review and update policies

## Comparison with Traditional Approaches

### Application-Level Filtering

**Traditional Approach:**
```python
# Application code must filter results
results = db.query("SELECT * FROM data")
filtered = [r for r in results if user.can_access(r)]
```

**Problems:**
- Security depends on application code
- Easy to bypass if code has bugs
- Complex filtering logic
- Difficult to audit

### Fluree Policy Approach

**Fluree Approach:**
```sparql
# Policy enforced automatically by database
SELECT ?data WHERE { ?subject ex:data ?data }
# User only sees authorized data automatically
```

**Benefits:**
- Security enforced by database
- Cannot be bypassed
- Simple query code
- Fully auditable

## Policy Architecture

### Policy Storage

Policies are stored as data in the ledger:

- **Queryable**: Policies can be queried like any other data
- **Versioned**: Policy changes are tracked over time
- **Auditable**: Complete history of policy changes

### Policy Evaluation Engine

The policy evaluation engine:

- **Integrates with Query Engine**: Evaluates policies during query execution
- **Efficient**: Optimized for performance
- **Extensible**: Supports custom policy rules

### Policy API

Policies are managed through:

- **Data Transactions**: Create/update policies via transactions
- **Query API**: Query existing policies
- **Admin API**: Administrative operations on policies

Policy enforcement makes Fluree uniquely suited for applications requiring fine-grained access control, multi-tenant architectures, and compliance with data privacy regulations. By enforcing policies at the database level, Fluree ensures security cannot be bypassed by application code, providing a foundation for trustless data sharing and secure multi-party systems.
