(ns fluree.db.util.ledger
  "Utility functions for working with ledger names and branches."
  (:require [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.util :as util]))

(defn ledger-parts
  "Splits a ledger alias into [ledger-name branch-name].
   e.g., 'my-ledger:main' -> ['my-ledger' 'main']
         'my-ledger' -> ['my-ledger' nil]"
  [ledger-alias]
  (let [parts (str/split ledger-alias #":" 2)]
    [(first parts) (second parts)]))

(defn ledger-base-name
  "Extracts the base ledger name from a ledger alias that may include a branch.
   e.g., 'my-ledger:main' -> 'my-ledger'"
  [ledger-alias]
  (first (ledger-parts ledger-alias)))

(defn ledger-branch
  "Extracts the branch name from a ledger alias.
   Returns the branch name or nil if no branch is specified.
   e.g., 'my-ledger:main' -> 'main'
         'my-ledger' -> nil"
  [ledger-alias]
  (second (ledger-parts ledger-alias)))

(defn ensure-ledger-branch
  "Ensures a ledger alias includes a branch.
   If no : symbol present, appends :main as default branch.
   e.g., 'my-ledger' -> 'my-ledger:main'
         'my-ledger:branch' -> 'my-ledger:branch'"
  [ledger-alias]
  (if (ledger-branch ledger-alias)
    ledger-alias
    (str ledger-alias ":" const/default-branch-name)))

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

(defn- query-str->map
  "Converts query string parameters from k=v&k2=v2 format to a map.
  Private helper for parse-ledger-alias."
  [query-str]
  (->> (str/split query-str #"&")
       (map str/trim)
       (map #(str/split % #"="))
       (reduce (fn [acc [k v]]
                 (assoc acc k v))
               {})))

(defn- parse-t-val
  "Parses a time value - converts numeric strings to longs,
  leaves ISO strings as-is. Private helper for parse-ledger-alias."
  [t-val]
  (if (re-matches #"^\d+$" t-val)
    (util/str->long t-val)
    t-val))

(defn- parse-time-travel-val
  "Parses time travel value from @ syntax.
  Supports:
   - t:42 -> returns 42 as long
   - iso:2025-07-01T00:00:00Z -> returns ISO string
   - sha:abc123 -> returns {:sha \"abc123\"} map
  Private helper for parse-ledger-alias."
  [time-str]
  (cond
    (str/starts-with? time-str "t:")
    (let [val (subs time-str 2)]
      (when (str/blank? val)
        (throw (ex-info "Missing value for time travel spec"
                        {:status 400 :error :db/invalid-time-travel})))
      (util/str->long val))

    (str/starts-with? time-str "iso:")
    (let [val (subs time-str 4)]
      (when (str/blank? val)
        (throw (ex-info "Missing value for time travel spec"
                        {:status 400 :error :db/invalid-time-travel})))
      val)

    (str/starts-with? time-str "sha:")
    (let [val (subs time-str 4)]
      (when (str/blank? val)
        (throw (ex-info "Missing value for time travel spec"
                        {:status 400 :error :db/invalid-time-travel})))
      (when (< (count val) 6)
        (throw (ex-info "SHA prefix must be at least 6 characters"
                        {:status 400 :error :db/invalid-commit-sha :min 6})))
      {:sha val})

    :else
    (throw (ex-info (str "Invalid time travel format: " time-str
                         ". Expected t:, iso:, or sha: prefix")
                    {:status 400 :error :db/invalid-time-travel}))))

(defn parse-ledger-alias
  "Parses a ledger alias string that may contain branch and time-travel information.

  Supports formats:
   - 'my-ledger' -> {:ledger 'my-ledger' :branch nil :t nil}
   - 'my-ledger:main' -> {:ledger 'my-ledger' :branch 'main' :t nil}
   - 'my-ledger?t=42' -> {:ledger 'my-ledger' :branch nil :t 42}
   - 'my-ledger:main?t=42' -> {:ledger 'my-ledger' :branch 'main' :t 42}
   - 'my-ledger@t:42' -> {:ledger 'my-ledger' :branch nil :t 42}
   - 'my-ledger:main@iso:2025-01-01' -> {:ledger 'my-ledger' :branch 'main' :t '2025-01-01'}
   - 'my-ledger@sha:abc123' -> {:ledger 'my-ledger' :branch nil :t {:sha 'abc123'}}

  The @ syntax takes precedence over ? syntax for time travel.

  Returns a map with keys:
   - :ledger - the base ledger name (always present)
   - :branch - the branch name (may be nil)
   - :t - time travel value (may be nil, Long, String, or {:sha ...} map)"
  [alias]
  (let [;; First extract time travel if present
        [base-with-branch time-val]
        (cond
          ;; @ syntax takes precedence
          (str/includes? alias "@")
          (let [at-idx (str/index-of alias "@")
                base (subs alias 0 at-idx)
                time-str (subs alias (inc at-idx))]
            [base (parse-time-travel-val time-str)])

          ;; ? query string syntax
          (str/includes? alias "?")
          (let [[base query-str] (str/split alias #"\?" 2)]
            [base (some-> query-str
                          query-str->map
                          (get "t")
                          parse-t-val)])

          ;; No time travel
          :else
          [alias nil])

        ;; Then extract ledger and branch
        [ledger-name branch-name] (ledger-parts base-with-branch)]

    {:ledger ledger-name
     :branch branch-name
     :t time-val}))
