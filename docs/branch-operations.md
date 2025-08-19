# Fluree Branch Operations

## Overview

Fluree provides Git-like branch operations for managing database version control. These operations help you manage parallel development, integrate changes between branches, and maintain clean history.

### Current API Status

| Function | Status | Description |
|----------|--------|-------------|
| `rebase!` | ✅ Implemented | Fast-forward and squash modes working |
| `branch-divergence` | ✅ Implemented | Check if branches can fast-forward |
| `merge!` | ❌ Not Implemented | Returns not-implemented error |
| `reset-branch!` | ⚠️ Partial | API exists but delta computation incomplete |

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

;; Connect to your database
(def conn @(fluree/connect {...}))

;; Simple case: Fast-forward when no divergence
@(fluree/rebase! conn "mydb:feature" "mydb:main")

;; When branches have diverged: Squash commits
@(fluree/rebase! conn "mydb:feature" "mydb:main" 
  {:squash? true
   :message "Feature complete: Add user authentication"})

;; Check if fast-forward is possible before operating
(def divergence @(fluree/branch-divergence conn "mydb:feature" "mydb:main"))
(when (:can-fast-forward divergence)
  (println "Fast-forward is possible!"))
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

### 1. REBASE - Atomic Strict Replay

Replays commits from source branch onto target branch. Currently supports:
- **Fast-forward** when target branch is behind source (no divergence)
- **Squash** to combine all source commits into a single commit on target

Future support planned for:
- Cherry-pick specific commits
- Non-atomic mode (apply commits until conflict)

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ff` | keyword | `:auto` | Fast-forward behavior: `:auto`, `:only`, `:never` |
| `squash?` | boolean | `false` | Combine all commits into one |
| `atomic?` | boolean | `true` | All-or-nothing vs. apply-until-conflict |
| `from` | int/string/nil | `nil` | Starting commit (t-value or SHA) |
| `to` | int/string/keyword/nil | `nil` | Ending commit (t-value, SHA, or `:conflict`) |
| `commits` | vector/nil | `nil` | Specific commits to cherry-pick |
| `message` | string | auto-generated | Commit message for the new commit |
| `preview?` | boolean | `false` | Dry run without making changes |

**Note on `:message`:**
- The `:message` option sets the commit message for the new commit created by the rebase
- If not provided, auto-generates: `"Squash rebase from <source-branch>"` or `"Fast-forward from <source-branch>"`
- This is stored as the commit's message property (separate from annotation)
- For fast-forward operations, no new commit is created so the message is not used

##### Commit Selection (Future Feature)
When implemented, you'll be able to select specific commits:
- **Default** (no options): All commits after LCA
- **Range by t-value**: `{:from 42 :to 44}` - Include commits from t=42 to t=44
- **Range by SHA**: `{:from "sha1" :to "sha2"}` - Include commits from sha1 to sha2
- **Until conflict**: `{:from 42 :to :conflict}` - Apply from t=42 until first conflict
- **Cherry-pick specific**: `{:commits [42 43 45]}` or `{:commits ["sha1" "sha2"]}`

**Note**: Type determines interpretation - integers are t-values, strings are SHAs

#### Examples

##### Standard Rebase (Auto fast-forward when possible)
```clojure
;; Automatically fast-forwards if possible, otherwise fails
(fluree/rebase! conn "ledger:feature" "ledger:main")
```

##### Squash Rebase
```clojure
;; Combines all commits from feature branch into single commit on main
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:squash? true
   :message "Feature X implementation"})
```

##### Cherry-pick Specific Commits (Not Yet Implemented)
```clojure
;; This will be the API when implemented:

;; Cherry-pick by SHA
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:commits ["sha1" "sha2"]
   :atomic? false})  ; Continue past conflicts

;; Or cherry-pick by t-value
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:commits [42 45 47]})

;; Or select a range
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:from 42 :to 50})  ; All commits from t=42 to t=50

;; Or until conflict
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:from 42 :to :conflict})  ; Apply from t=42 until first conflict
```

##### Fast-forward Only
```clojure
(fluree/rebase! conn "ledger:feature" "ledger:main"
  {:ff :only})  ; Fail if fast-forward not possible
```

#### Response

```clojure
{:status :success  ; or :conflict, :error
 :operation :rebase
 :from "ledger:feature"
 :to "ledger:main" 
 :strategy "fast-forward"  ; or "squash", "replay"
 :commits {:applied ["sha1" "sha2"]  ; commits that were applied
           :skipped []  ; commits skipped (future feature)
           :conflicts []}  ; conflicting commits (if any)
 :new-commit "sha-of-new-commit"}  ; nil for fast-forward (just moves pointer)
```

#### Implementation Status
**Currently Implemented:**
- Fast-forward (`:ff :auto`, `:ff :only`, `:ff :never`)
- Squash (`:squash? true`)
- Conflict detection for overlapping changes

**Not Yet Implemented:**
- Cherry-pick (`:from`, `:to`, `:commits` options)
- Non-atomic mode (`:atomic? false`)
- Commit-by-commit replay without squash

---

### 2. RESET - Safe Rollback or Hard Reset

Resets a branch to a previous state.

#### Implementation Status

| Mode | Status | Description |
|------|--------|-------------|
| Safe mode (`:mode :safe`) | ⚠️ Partial | API exists, delta computation not implemented |
| Hard mode (`:mode :hard`) | ❌ Not Implemented | Will move branch pointer (rewrite history) |
| Preview (`:preview? true`) | ✅ Implemented | Dry-run to see what would happen |

**Safe mode** (default): Creates a new commit that reverts to the target state
**Hard mode**: Moves the branch pointer, rewriting history (requires archive or force)

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
;; Note: Safe reset is partially implemented - delta computation not yet complete
(fluree/reset-branch! conn "ledger:main" {:t 90}
  {:message "Reverting to stable state at t=90"})
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

### 3. MERGE - Three-way Delta Merge (Future API)

**⚠️ NOT YET IMPLEMENTED** - This section describes the planned future API for reference.

Will compute deltas from the Last Common Ancestor (LCA) for both branches, auto-merge based on a conflict policy, and create a merge commit on the target branch. Will reject if there are unresolved conflicts.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `conflict-policy` | keyword/function | `:conservative` | How to handle conflicts |
| `preview?` | boolean | `false` | Dry run without making changes |

##### Conflict Policies
- `:conservative` - Reject on any conflict (default)
- `:ours` - Keep changes from target branch
- `:theirs` - Keep changes from source branch
- `:last-write-wins` - Use the most recent change based on timestamp
- `:schema-aware` - Use schema rules to resolve conflicts
- `function` - Custom conflict resolution function

#### Example

```clojure
;; This will be the API when implemented:
(fluree/merge! conn "ledger:feature" "ledger:main"
  {:conflict-policy :conservative
   :preview? false})
```

#### Response

```clojure
{:status :success  ; or :conflict, :error
 :operation :merge
 :from "ledger:feature"
 :to "ledger:main"
 :strategy "3-way"
 :commits {:merged ["sha1" "sha2"]
           :conflicts []}
 :new-commit "sha-of-merge-commit"}
```

---

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
  @(fluree/rebase! conn "ledger:feature-x" "ledger:main")
  ;; Squash for clean history
  @(fluree/rebase! conn "ledger:feature-x" "ledger:main" 
    {:squash? true
     :message "Add feature X with complete implementation"}))
```

### 2. Handling Conflicts

When branches have modified the same data:

```clojure
;; Attempt rebase
(def result @(fluree/rebase! conn "feature" "main" {:squash? true}))

;; Check for conflicts
(when (= :conflict (:status result))
  (println "Conflict detected!")
  (println "Conflicting commits:" (get-in result [:commits :conflicts]))
  ;; Currently, manual resolution required - merge the data in your application
  ;; Then create a new commit with resolved data)
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

## Troubleshooting

### Common Issues

#### "Cannot fast-forward" Error
```clojure
;; This happens when branches have diverged
;; Solution: Use squash mode instead
@(fluree/rebase! conn "feature" "main" {:squash? true})
```

#### "Rebase conflict" Error
```clojure
;; Occurs when same data modified in both branches
;; Currently requires manual resolution:
;; 1. Query both branches to understand the conflict
;; 2. Determine correct final state
;; 3. Create new commit with resolved data
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
  @(fluree/rebase! conn "feature" "main" {:ff :only})
  ;; Otherwise, squash commits
  @(fluree/rebase! conn "feature" "main" {:squash? true
                                           :message "Feature implementation"}))
```

### Reverting Bad Changes
```clojure
;; Note: This functionality is not yet fully implemented
;; Currently returns a not-implemented error
(fluree/reset-branch! conn "main" {:t 100})
```

### Cherry-picking Bug Fixes
```clojure
;; Note: Cherry-pick functionality not yet implemented
;; This will be the API when available:
(fluree/rebase! conn "develop" "main"
  {:commits ["fix1-sha" "fix2-sha"]})
```