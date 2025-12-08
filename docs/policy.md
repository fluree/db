# Fluree Policy System

This document provides a comprehensive reference for Fluree's policy system, which enables fine-grained access control over data viewing and modification.

## Overview

Fluree policies are JSON-LD documents that control access to data. Policies are evaluated at query time to filter results and at transaction time to validate modifications.

## Policy Structure

A policy is a JSON-LD document with `@type: f:AccessPolicy`:

```json
{
  "@context": {"f": "https://ns.flur.ee/ledger#", "ex": "http://example.org/"},
  "@id": "ex:myPolicy",
  "@type": "f:AccessPolicy",
  "f:action": {"@id": "f:view"},
  "f:query": {"@type": "@json", "@value": {"where": {...}}}
}
```

## Policy Actions

| Action | Description |
|--------|-------------|
| `f:view` | Controls read/query access |
| `f:modify` | Controls write/transaction access |

If `f:action` is not specified, the policy applies to **both** view and modify operations.

## Policy Evaluation Methods

### f:query - Conditional Access

The `f:query` property contains a query that must return results for access to be granted. The special variable `?$this` refers to the subject being evaluated.

```json
{
  "f:query": {
    "@type": "@json",
    "@value": {
      "@context": {"ex": "http://example.org/"},
      "where": {"@id": "?$this", "ex:public": true}
    }
  }
}
```

### f:allow - Unconditional Access (Boolean)

The `f:allow` property provides unconditional allow or deny without query execution:

- `"f:allow": true` - Unconditionally allows access (no query executed)
- `"f:allow": false` - Unconditionally denies access (no query executed)

**Precedence**: `f:allow` takes precedence over `f:query` if both are specified.

```json
{
  "@id": "ex:publicDataPolicy",
  "@type": "f:AccessPolicy",
  "f:action": {"@id": "f:view"},
  "f:allow": true
}
```

## Targeting Policies

Policies can be targeted to specific subjects, classes, or properties.

### f:onSubject - Subject Targeting

Target specific subjects via static IRIs or dynamic queries:

```json
{
  "f:onSubject": [{"@id": "ex:publicData"}]
}
```

Or dynamically with a query:

```json
{
  "f:onSubject": {
    "@type": "@json",
    "@value": {
      "where": {"@id": "?$this", "@type": {"@id": "ex:PublicClass"}}
    }
  }
}
```

The query must use `?$this` as the variable for the subject IRI.

### f:onClass - Class Targeting

Target all instances of a class. Class policies are automatically optimized to use O(1) property-based lookups:

```json
{
  "f:onClass": {"@id": "ex:User"},
  "f:action": {"@id": "f:view"},
  "f:query": {...}
}
```

**How it works**: When you define a class policy, Fluree automatically indexes the policy by all properties used by that class. At query time, the policy is found via fast property lookup, then verified against the subject's actual class membership.

**Performance characteristics**:
- O(1) lookup per property (same as `f:onProperty`)
- Class membership verification is cached per subject during the request
- Implicit properties (`@id`, `@type`) are always indexed for class policies

### f:onProperty - Property Targeting (Indexed)

Target specific properties with O(1) indexed lookups using static IRIs:

```json
{
  "f:onProperty": {"@id": "schema:ssn"},
  "f:action": {"@id": "f:view"},
  "f:allow": false
}
```

Or dynamically determine properties using a query:

```json
{
  "f:onProperty": {
    "@type": "@json",
    "@value": {
      "@context": {"ex": "http://example.org/"},
      "where": {"@id": "?$this", "ex:isSensitive": true}
    }
  },
  "f:action": {"@id": "f:view"},
  "f:allow": false
}
```

The query must use `?$this` as the variable for the property IRI.

You can also mix static IRIs and queries in the same policy:

```json
{
  "f:onProperty": [
    {"@id": "schema:ssn"},
    {"@type": "@json",
     "@value": {
       "@context": {"ex": "http://example.org/"},
       "where": {"@id": "?$this", "ex:isSensitive": true}
     }}
  ],
  "f:action": {"@id": "f:view"},
  "f:allow": false
}
```

This restricts both the static `schema:ssn` property AND any properties dynamically marked as sensitive.

## Policy Classes

Policies can be associated with classes to enable role-based access control:

```json
{
  "@id": "ex:adminPolicy",
  "@type": ["f:AccessPolicy", "ex:AdminRole"],
  "f:action": {"@id": "f:view"},
  "f:allow": true
}
```

Then apply policies by class:

```clojure
(fluree/wrap-class-policy db ["http://example.org/AdminRole"] nil)
```

## Special Variables

| Variable | Description |
|----------|-------------|
| `?$this` | The subject/property/object being evaluated |
| `?$identity` | The authenticated identity (provided via policy values) |

## Required Policies

Set `f:required: true` to make a policy the **only** policy evaluated for matching targets:

```json
{
  "f:onProperty": {"@id": "schema:ssn"},
  "f:required": true,
  "f:action": {"@id": "f:view"},
  "f:allow": false
}
```

When a required policy matches, other non-required policies are ignored for that target.

## Default Allow Behavior

By default, if no policy matches a piece of data, access is denied. Use the `default-allow` option to change this:

```clojure
;; Allow access when no policies apply
(fluree/wrap-policy db policy nil true)
```

## Examples

### Public Data with Restricted SSN

```json
[
  {
    "@id": "ex:publicViewPolicy",
    "@type": "f:AccessPolicy",
    "f:action": {"@id": "f:view"},
    "f:allow": true
  },
  {
    "@id": "ex:restrictSSN",
    "@type": "f:AccessPolicy",
    "f:onProperty": {"@id": "schema:ssn"},
    "f:required": true,
    "f:action": {"@id": "f:view"},
    "f:allow": false
  }
]
```

### Dynamic Sensitive Property Restriction

```json
[
  {
    "@id": "ex:sensitivePropertyPolicy",
    "@type": "f:AccessPolicy",
    "f:action": {"@id": "f:view"},
    "f:required": true,
    "f:onProperty": {
      "@type": "@json",
      "@value": {
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?$this", "ex:isSensitive": true}
      }
    },
    "f:allow": false
  },
  {
    "@id": "ex:defaultAllow",
    "@type": "f:AccessPolicy",
    "f:action": {"@id": "f:view"},
    "f:allow": true
  }
]
```

### Identity-Based Access

```json
{
  "@id": "ex:ownDataPolicy",
  "@type": "f:AccessPolicy",
  "f:onSubject": {
    "@type": "@json",
    "@value": {
      "where": {"@id": "?$this", "@type": {"@id": "ex:User"}}
    }
  },
  "f:action": {"@id": "f:view"},
  "f:query": {
    "@type": "@json",
    "@value": {
      "@context": {"ex": "http://example.org/"},
      "where": {"@id": "?$identity", "ex:user": {"@id": "?$this"}}
    }
  }
}
```

## Tracking Policy Execution

Enable policy execution tracking to understand which policies are evaluated and how often they grant access. This is useful for debugging, auditing, and performance analysis.

### Enabling Tracking

Include `"meta": true` in the query/transaction options:

```clojure
;; Query with policy tracking
@(fluree/query policy-db
  {"@context" {"ex" "http://example.org/"}
   "select" {"?s" ["*"]}
   "where" {"@id" "?s" "@type" "ex:User"}
   "opts" {"meta" true}})

;; Transaction with policy tracking
@(fluree/update policy-db
  {"@context" {"ex" "http://example.org/"}
   "insert" {"@id" "ex:newUser" "@type" "ex:User"}
   "opts" {"meta" true}})
```

### Tracking Results

When `meta` is enabled, the result includes a `:policy` map showing execution statistics:

```clojure
{:result [...],
 :fuel 5,
 :time "2.3ms",
 :policy {"http://example.org/viewPolicy" {:executed 3, :allowed 3},
          "http://example.org/adminPolicy" {:executed 1, :allowed 0}}}
```

| Key | Description |
|-----|-------------|
| `:executed` | Number of times the policy was evaluated against a flake/subject |
| `:allowed` | Number of times the policy granted access |

### Interpreting Results

- **High executed, low allowed**: Policy is frequently checked but restrictive
- **executed = allowed**: Policy always grants access when evaluated
- **Only executed, no allowed**: Policy evaluated but denied (may be normal for deny policies)
- **Policy not in map**: Policy was never evaluated (didn't match any data)

### Error Tracking

When a policy-enforced operation fails, the exception data includes tracking:

```clojure
(try
  @(fluree/update policy-db {..., "opts" {"meta" true}})
  (catch Exception e
    (println (:policy (ex-data e)))))
;; => {"http://example.org/denyPolicy" {:executed 1}}
```

This shows which policy caused the denial.

## API Functions

| Function | Description |
|----------|-------------|
| `fluree/wrap-policy` | Apply policy documents directly |
| `fluree/wrap-class-policy` | Apply policies by class |
| `fluree/wrap-identity-policy` | Apply policies by identity's `f:policyClass` |

