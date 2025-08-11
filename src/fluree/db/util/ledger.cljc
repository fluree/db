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