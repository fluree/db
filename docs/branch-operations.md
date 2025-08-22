# Fluree Branch Operations

## Overview

Fluree provides Git-like branch operations for managing database version control. These operations help you manage parallel development, integrate changes between branches, and maintain clean history.

### Current API Status

| Function | Status | Description |
|----------|--------|-------------|
| `merge!` | ✅ Implemented | Fast-forward and squash modes working |
| `rebase!` | ⚠️ Partial | Squash mode only (commit-by-commit replay not yet) |
| `branch-divergence` | ✅ Implemented | Check if branches can fast-forward |
| `reset-branch!` | ⚠️ Partial | Safe mode only (hard reset not yet) |
| `branch-graph` | ✅ Implemented | JSON and ASCII graph visualization |

### Core Concepts

- **Branch**: A named sequence of database commits (e.g., `"ledger:main"`, `"ledger:feature"`)
- **Commit**: An immutable snapshot of database state at a specific point in time
- **LCA (Last Common Ancestor)**: The most recent commit shared by two branches
- **Fast-forward**: Moving a branch pointer forward when there's no divergence
- **Squash**: Combining multiple commits into a single commit

## Quick Start

### Basic Branch Integration

```clojure
(require '[fluree.db.api :as fluree])
(require '[fluree.db.merge :as merge])
(require '[clojure.core.async :refer [<!!]])

;; Connect to your database
(def conn @(fluree/connect {...}))

;; MERGE: Update target branch with source changes
;; Simple case: Fast-forward when no divergence
@(fluree/merge! conn "mydb:feature" "mydb:main")

;; When branches have diverged: Squash commits
@(fluree/merge! conn "mydb:feature" "mydb:main" 
  {:squash? true
   :message "Feature complete: Add user authentication"})

;; REBASE: Update source branch by replaying onto target
;; Note: This updates the feature branch, not main!
@(fluree/rebase! conn "mydb:feature" "mydb:main"
  {:squash? true
   :message "Rebase feature onto latest main"})

;; Check if fast-forward is possible before operating
(def divergence @(fluree/branch-divergence conn "mydb:feature" "mydb:main"))
(when (:can-fast-forward divergence)
  (println "Fast-forward is possible!"))

;; Visualize branch relationships
(println (<!! (merge/branch-graph conn "mydb" {:format :ascii})))
```

### Design Principles

All operations follow consistent patterns:
- **Safe defaults**: Non-destructive operations are preferred
- **Preview mode**: Use `preview? true` for dry-run before actual changes
- **Consistent API**: All operations have similar request/response shapes
- **Clear errors**: Descriptive error messages with resolution hints
- **Atomic operations**: Changes either fully succeed or fully fail (no partial states)

## Understanding Branch Divergence

Before performing branch operations, it's helpful to understand how branches relate:

```clojure
;; Check relationship between branches
(def divergence @(fluree/branch-divergence conn "feature" "main"))

;; Returns:
{:common-ancestor "fluree:commit:sha256:..."
 :can-fast-forward true  ; or false
 :fast-forward-direction :branch1-to-branch2}  ; or :branch2-to-branch1, nil
;; Note: branch1-ahead and branch2-ahead are documented but not yet implemented
```

## Operations

### 1. MERGE - Updates Target Branch with Source Changes

Merges commits from source branch into target branch. Currently supports:
- **Fast-forward** when target branch is behind source (no divergence)
- **Squash** to combine all source commits into a single commit on target

#### How Squash Works

When using `squash? true`, Fluree:
1. Collects all flakes (assertions/retractions) from source commits
2. Groups them by `[subject predicate object datatype meta]`
3. Counts net operations (assertions - retractions) for each unique value
4. Produces final flakes:
   - Net positive → assertion
   - Net negative → retraction  
   - Net zero → cancelled out (no flake)

This means that if you assert a value in one commit and retract it in another, they cancel out and disappear from the squashed result.

Future support planned for:
- Regular 3-way merge (without squash)
- Schema-aware conflict resolution when validation is added

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ff` | keyword | `:auto` | Fast-forward behavior: `:auto`, `:only`, `:never` |
| `squash?` | boolean | `false` | Combine all commits into one |
| `message` | string | auto-generated | Commit message for the new commit |
| `preview?` | boolean | `false` | Dry run without making changes |

**Note on `:message`:**
- The `:message` option sets the commit message for the new commit created by the merge
- If not provided, auto-generates: `"Squash merge from <source-branch>"` or similar
- This is stored as the commit's message property (separate from annotation)
- For fast-forward operations, no new commit is created so the message is not used

#### Examples

##### Standard Merge (Auto fast-forward when possible)
```clojure
;; Automatically fast-forwards if possible, otherwise fails
(fluree/merge! conn "ledger:feature" "ledger:main")
```

##### Squash Merge
```clojure
;; Combines all commits from feature branch into single commit on main
(fluree/merge! conn "ledger:feature" "ledger:main"
  {:squash? true
   :message "Feature X implementation"})
```

##### Fast-forward Only
```clojure
(fluree/merge! conn "ledger:feature" "ledger:main"
  {:ff :only})  ; Fail if fast-forward not possible
```

#### Response

```clojure
{:status :success  ; or :error
 :operation :merge
 :from "ledger:feature"
 :to "ledger:main" 
 :strategy "fast-forward"  ; or "squash"
 :commits {:merged 3}  ; number of commits merged
 :new-commit "sha-of-new-commit"}  ; for squash merge
```

#### Implementation Status
**Currently Implemented:**
- Fast-forward (`:ff :auto`, `:ff :only`, `:ff :never`)
- Squash (`:squash? true`)

**Not Yet Implemented:**
- Regular 3-way merge (without squash)
- Conflict detection and resolution (will come with SHACL/policy validation)

---

### 3. RESET - Safe Rollback or Hard Reset

Resets a branch to a previous state.

#### Implementation Status

| Mode | Status | Description |
|------|--------|-------------|
| Safe mode (`:mode :safe`) | ✅ Implemented | Creates a revert commit to target state |
| Hard mode (`:mode :hard`) | ❌ Not Implemented | Will move branch pointer (rewrite history) |
| Preview (`:preview? true`) | ✅ Implemented | Dry-run to see what would happen |

**Safe mode** (default): Creates a new commit that reverts to the target state by:
1. Finding all commits after the target state
2. Flipping all operations (assertions become retractions, retractions become assertions)
3. Creating a single new commit with these reversed operations

**Hard mode**: Will move the branch pointer, rewriting history (requires archive or force)

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `branch` | string | required | Target branch to reset |
| `to` | map | required | Target state: `{:t 90}` or `{:sha "..."}` |
| `mode` | keyword | `:safe` | Reset mode: `:safe` or `:hard` |
| `archive` | map | `{:as :tag}` | How to archive on hard reset |
| `force?` | boolean | `false` | Required for hard reset without archive |
| `message` | string | auto | Commit message (safe mode only) |
| `preview?` | boolean | `false` | Dry run without making changes |

##### Archive Options (Hard Mode)
- `{:as :tag :name "backup-xyz"}` - Create a tag at old HEAD
- `{:as :branch :name "backup-xyz"}` - Create a branch at old HEAD
- `{:as :none}` - No archive (requires `force? true`)

#### Examples

##### Safe Reset (Creates Revert Commit)
```clojure
;; Reset to a specific transaction number
(fluree/reset-branch! conn "ledger:main" {:t 90}
  {:message "Reverting to stable state at t=90"})

;; Reset to a specific commit SHA
(fluree/reset-branch! conn "ledger:main" {:sha "abc123"}
  {:message "Reverting to commit abc123"})
```

##### Hard Reset with Archive Tag
```clojure
(fluree/reset-branch! conn "ledger:main" {:sha "abc123"}
  {:mode :hard
   :archive {:as :tag :name "pre-reset-backup"}})
```

##### Force Hard Reset (Dangerous)
```clojure
(fluree/reset-branch! conn "ledger:main" {:t 85}
  {:mode :hard
   :archive {:as :none}
   :force? true})
```

#### Response

```clojure
{:status :success  ; or :error
 :operation :reset
 :branch "ledger:main"
 :mode :safe  ; or :hard
 :reset-to {:t 90}  ; or {:sha "..."}
 :new-commit "sha"  ; For safe mode
 :archived {:type :tag :name "backup-123"}  ; For hard mode
 :previous-head "sha-before-reset"}
```

---

### 2. REBASE - Replays Source Branch onto Target

Rebases source branch onto target branch by replaying source commits on top of target. The **source branch is updated** with new commits, while the target branch remains unchanged.

**Note:** This is true git-style rebase where the source branch is modified, not the target.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `squash?` | boolean | `false` | Combine all commits into one |
| `message` | string | auto-generated | Commit message for the new commit |
| `preview?` | boolean | `false` | Dry run without making changes |

#### Examples

##### Squash Rebase
```clojure
;; Rebases feature branch ONTO main (feature gets main's commits, main unchanged)
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:squash? true
   :message "Rebase feature onto main"})
```

##### Preview Mode
```clojure
;; See what would happen without making changes
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:squash? true
   :preview? true})
```

#### Response

```clojure
{:status :success  ; or :error
 :operation :rebase
 :from "ledger:feature"
 :to "ledger:main"
 :strategy "squash"  ; or "replay" when implemented
 :commits {:rebased 3}  ; number of commits rebased
 :new-commit "sha-of-new-commit"}
```

#### Implementation Status
**Currently Implemented:**
- Squash rebase (`:squash? true`)

**Not Yet Implemented:**
- Commit-by-commit replay (without squash)
- Cherry-pick specific commits

---

## Understanding Multi-Cardinality Predicates

Fluree's branch operations handle multi-cardinality predicates (where a subject can have multiple values for the same predicate) through its flake model:

### Flake Structure
A flake is Fluree's atomic unit of data: `[subject predicate object datatype t op meta]`
- `op` (operation): `true` for assertion, `false` for retraction
- `meta`: Optional metadata, used for ordered lists (e.g., `{:i 0}` for position 0)

### Set vs List Semantics
- **Sets** (default): Multiple values with `meta = nil`, order doesn't matter
- **Lists**: Multiple values with position metadata `{:i 0}`, `{:i 1}`, etc.

### Example: Squashing with Multi-Cardinality
```clojure
;; Commit 1: Add skills
{"@id" "ex:alice" "ex:skills" ["Java" "Python" "Rust"]}
;; Creates: [alice skills "Java" string t1 true nil]
;;          [alice skills "Python" string t1 true nil]  
;;          [alice skills "Rust" string t1 true nil]

;; Commit 2: Remove Java and Rust
{"delete" {"@id" "ex:alice" "ex:skills" ["Java" "Rust"]}}
;; Creates: [alice skills "Java" string t2 false nil]
;;          [alice skills "Rust" string t2 false nil]

;; Commit 3: Re-add Rust
{"@id" "ex:alice" "ex:skills" ["Rust"]}
;; Creates: [alice skills "Rust" string t3 true nil]

;; After squash:
;; - Java: 1 assert, 1 retract = cancelled out (removed)
;; - Python: 1 assert = kept
;; - Rust: 2 asserts, 1 retract = kept (net positive)
;; Result: Alice has skills ["Python", "Rust"]
```

## Error Handling

All operations return consistent error shapes:

```clojure
{:status :error
 :operation :merge|:rebase|:reset
 :error :db/conflict|:db/invalid-branch|:db/cannot-fast-forward
 :message "Human-readable error message"
 :details {...}}  ; Operation-specific error details
```

For conflicts:
```clojure
{:status :conflict
 :operation :merge|:rebase
 :conflicts [{:commit "sha"
              :transaction {...}
              :original-result [...]
              :replay-result [...]}]
 :resolution-hint "Try using :squash? true or different conflict-policy"}
```

---

## Common Workflows

### 1. Feature Development Workflow

```clojure
;; Create feature branch from main
@(fluree/create-branch! conn "ledger:feature-x" "ledger:main")

;; Work on feature...
@(fluree/insert! conn "ledger:feature-x" {...})
@(fluree/update! conn "ledger:feature-x" {...})

;; When ready to integrate:
;; First, check if you can fast-forward
(def div @(fluree/branch-divergence conn "ledger:feature-x" "ledger:main"))

(if (:can-fast-forward div)
  ;; Simple fast-forward
  @(fluree/merge! conn "ledger:feature-x" "ledger:main")
  ;; Squash for clean history
  @(fluree/merge! conn "ledger:feature-x" "ledger:main" 
    {:squash? true
     :message "Add feature X with complete implementation"}))
```

### 2. Handling Conflicts

When branches have modified the same data:

```clojure
;; Attempt merge
(def result @(fluree/merge! conn "feature" "main" {:squash? true}))

;; Check for errors
(when (= :error (:status result))
  (println "Error:" (:message result))
  ;; Handle the error based on :error key
  )
```

## Best Practices

1. **Always preview first**: Use `preview? true` for dry runs on important operations
2. **Check divergence first**: Use `branch-divergence` to understand branch relationships
3. **Prefer safe operations**: Default modes are non-destructive
4. **Use squash for feature branches**: Keeps main branch history clean
5. **Fast-forward when possible**: Maintains linear history
6. **Write descriptive commit messages**: Especially important when squashing
7. **Test after operations**: Verify data integrity after branch operations

---

## Implementation Details for Developers

### Code Organization

The branch operations are implemented across several namespaces:

- `fluree.db.merge` - Public API entry points
- `fluree.db.merge.operations` - Core operation implementations (squash!, fast-forward!, safe-reset!)
- `fluree.db.merge.branch` - Branch analysis and LCA detection
- `fluree.db.merge.commit` - Commit data reading and namespace handling
- `fluree.db.merge.flake` - Flake manipulation and cancellation logic
- `fluree.db.merge.db` - Database preparation and staging
- `fluree.db.merge.response` - Response formatting and error messages

### Key Algorithms

#### Last Common Ancestor (LCA) Detection
The LCA is found by:
1. Checking if branches are at the same commit
2. Checking if one branch was created from the other's current commit
3. Checking if branches share a creation origin
4. Walking commit chains and finding the first common commit

#### Squash Operation Cancellation
The cancellation algorithm (`cancel-opposite-operations`):
1. Groups flakes by `[s p o dt m]` (including meta for ordered lists)
2. Counts assertions and retractions for each group
3. Calculates net effect (assertions - retractions)
4. Produces:
   - Assertion if net > 0
   - Retraction if net < 0
   - Nothing if net = 0 (cancelled out)

#### Safe Reset Implementation
Safe reset (`safe-reset!`):
1. Loads the current database state
2. Gets the target state (by t-value or SHA)
3. Finds all commits after the target state
4. Reverses each commit's flakes using `flake/flip-flake`
5. Creates a single new commit with all reversed operations

### Testing Branch Operations

The test suite (`fluree.db.merge-test`) includes:
- Fast-forward merge tests
- Squash merge with divergent branches
- Cancellation of assert/retract pairs
- Safe reset to previous states
- File-based and memory-based storage tests

Example test for cancellation:
```clojure
(deftest squash-cancellation-test
  ;; Tests that assert/retract pairs cancel out
  ;; See test/fluree/db/merge_test.clj for full implementation)
```

## Troubleshooting

### Common Issues

#### "Cannot fast-forward" Error
```clojure
;; This happens when branches have diverged
;; Solution: Use squash mode instead
@(fluree/merge! conn "feature" "main" {:squash? true})
```


#### "Cannot operate across different ledgers" Error
```clojure
;; Branches must be from same ledger
;; Wrong: "ledger1:branch" -> "ledger2:main" 
;; Right: "ledger1:feature" -> "ledger1:main"
```

## Examples by Use Case

### Feature Branch Integration
```clojure
;; Check if fast-forward is possible
(def divergence @(fluree/branch-divergence conn "feature" "main"))

;; If can fast-forward, do it
(if (:can-fast-forward divergence)
  @(fluree/merge! conn "feature" "main" {:ff :only})
  ;; Otherwise, squash commits
  @(fluree/merge! conn "feature" "main" {:squash? true
                                          :message "Feature implementation"}))
```

### Reverting Bad Changes
```clojure
;; Safe reset creates a new commit that reverts to the target state
(fluree/reset-branch! conn "main" {:t 100}
  {:message "Reverting to known good state at t=100"})

;; Or reset to a specific commit SHA
(fluree/reset-branch! conn "main" {:sha "abc123def"}
  {:message "Reverting to stable release"})
```

### Rebasing Feature Branch
```clojure
;; Rebase feature branch onto main (updates feature branch)
(fluree/rebase! conn "feature" "main"
  {:squash? true
   :message "Rebase feature onto latest main"})
```