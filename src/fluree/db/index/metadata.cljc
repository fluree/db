(ns fluree.db.index.metadata
  (:require [fluree.db.constants :as const]
            [fluree.db.util :as util :refer [get-first get-first-value]]))

#?(:clj (set! *warn-on-reflection* true))

(defn index-metadata
  "Returns a canonical index metadata map from either an internal commit map
  or an expanded JSON-LD commit. The result is either nil (no index) or
  {:address <string> :t <int>}.

  Accepts either:
  - Internal commit map: {:index {:address <addr> :data {:t <int>}}}
  - Expanded JSON-LD commit: f:index → { f:address, f:data → { f:t }}"
  [commit-or-jsonld]
  (let [internal-address (get-in commit-or-jsonld [:index :address])
        internal-t       (get-in commit-or-jsonld [:index :data :t])]
    (if (or internal-address internal-t)
      (when internal-address
        {:address internal-address
         :t       internal-t})
      (let [idx           (get-first commit-or-jsonld const/iri-index)
            jsonld-addr   (some-> idx (get-first-value const/iri-address))
            jsonld-data   (get-first idx const/iri-data)
            jsonld-t      (some-> jsonld-data (get-first-value const/iri-fluree-t))]
        (when jsonld-addr
          {:address jsonld-addr
           :t       jsonld-t})))))

(defn index-address
  "Returns the index address from a commit map or JSON-LD commit, or nil."
  [commit-or-jsonld]
  (some-> (index-metadata commit-or-jsonld) :address))

(defn index-t
  "Returns the index t from a commit map or JSON-LD commit, or nil."
  [commit-or-jsonld]
  (some-> (index-metadata commit-or-jsonld) :t))


