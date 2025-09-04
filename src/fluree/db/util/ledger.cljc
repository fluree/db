(ns fluree.db.util.ledger
  "Utility functions for working with ledger names and branches."
  (:require [clojure.string :as str]))

(defn ledger-base-name
  "Extracts the base ledger name from a ledger alias that may include a branch.
   e.g., 'my-ledger:main' -> 'my-ledger'"
  [ledger-alias]
  (first (str/split ledger-alias #":" 2)))

(defn ledger-branch
  "Extracts the branch name from a ledger alias.
   Returns the branch name or nil if no branch is specified.
   e.g., 'my-ledger:main' -> 'main'
         'my-ledger' -> nil"
  [ledger-alias]
  (second (str/split ledger-alias #":" 2)))

(defn ledger-parts
  "Splits a ledger alias into [ledger-name branch-name].
   e.g., 'my-ledger:main' -> ['my-ledger' 'main']
         'my-ledger' -> ['my-ledger' nil]"
  [ledger-alias]
  (let [parts (str/split ledger-alias #":" 2)]
    [(first parts) (second parts)]))

(defn validate-ledger-name
  "Validates a ledger name for creation. Throws if invalid.
   Rules:
   - Cannot contain ':' (reserved for branch separator)
   - Cannot contain '@', '#', '?' (reserved characters)
   - Cannot contain whitespace
   - Cannot start with '/', '-'
   - Cannot end with '/'
   - Cannot be empty
   - Cannot contain path traversal patterns like '../'"
  [ledger-name]
  (cond
    (str/blank? ledger-name)
    (throw (ex-info "Ledger name cannot be empty"
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (str/includes? ledger-name ":")
    (throw (ex-info (str "Ledger name cannot contain ':' character. "
                         "Branches must be created separately. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (re-find #"[@#?]" ledger-name)
    (throw (ex-info (str "Ledger name cannot contain '@', '#', or '?' characters. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (re-find #"\s" ledger-name)
    (throw (ex-info (str "Ledger name cannot contain whitespace. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (str/ends-with? ledger-name "/")
    (throw (ex-info (str "Ledger name cannot end with '/'. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (re-matches #"^[/\-].*" ledger-name)
    (throw (ex-info (str "Ledger name cannot start with '/' or '-'. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (re-matches #"^/+$" ledger-name)
    (throw (ex-info (str "Ledger name cannot consist only of '/' characters. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (str/includes? ledger-name "../")
    (throw (ex-info (str "Ledger name cannot contain path traversal patterns. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    (not (re-matches #"^[a-zA-Z0-9][\w\-\./_]*$" ledger-name))
    (throw (ex-info (str "Ledger name must start with alphanumeric character and "
                         "contain only alphanumeric, underscore, hyphen, dot, or slash characters. "
                         "Provided: " ledger-name)
                    {:error :db/invalid-ledger-name
                     :ledger-name ledger-name}))

    :else ledger-name))

(defn validate-branch-name
  "Validates a branch name. Throws if invalid.
   Rules:
   - Cannot contain '/' (would create subdirectories)
   - Cannot contain whitespace
   - Should only contain alphanumeric characters, hyphens, underscores, and dots"
  [branch-name]
  (cond
    (str/blank? branch-name)
    (throw (ex-info "Branch name cannot be empty"
                    {:error :db/invalid-branch-name
                     :branch-name branch-name}))

    (str/includes? branch-name "/")
    (throw (ex-info (str "Branch name cannot contain '/' character. "
                         "Provided: " branch-name)
                    {:error :db/invalid-branch-name
                     :branch-name branch-name}))

    (re-find #"\s" branch-name)
    (throw (ex-info (str "Branch name cannot contain whitespace. "
                         "Provided: " branch-name)
                    {:error :db/invalid-branch-name
                     :branch-name branch-name}))

    (not (re-matches #"^[a-zA-Z0-9][\w\-\.]*$" branch-name))
    (throw (ex-info (str "Branch name must start with alphanumeric character and "
                         "contain only alphanumeric, underscore, hyphen, or dot characters. "
                         "Provided: " branch-name)
                    {:error :db/invalid-branch-name
                     :branch-name branch-name}))

    :else branch-name))