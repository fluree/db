(ns fluree.db.serde.json-dict
  "JSON serialization with segment-level SID dictionary.

   Format (version 2):
   {\"version\": 2
    \"dict\": [[ns-code name] ...]              ; SID dictionary
    \"flakes\": [[s-idx p-idx o dt-idx t op m] ...]  ; flakes using dict indices}

   Version handling:
   - Version 2: Dictionary format (always used for new writes)
   - Version 1/no version: Legacy standard format (read-only support)"
  (:require [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.serde :as serde]
            [fluree.db.serde.json :as json]))

#?(:clj (set! *warn-on-reflection* true))

(defn- add-sid-to-dict
  "Add a SID to the dictionary if not already present.
   Returns [updated-seen-sids updated-dict-vec sid-index]."
  [seen-sids dict-vec sid]
  (if-let [existing-idx (get seen-sids sid)]
    [seen-sids dict-vec existing-idx]
    (let [new-idx (count dict-vec)]
      [(assoc! seen-sids sid new-idx)
       (conj! dict-vec sid)
       new-idx])))

(defn- serialize-flake-indices
  "Convert a flake to dictionary indices for s, p, dt and handle o.
   Returns [updated-seen-sids updated-dict-vec [s-idx p-idx o-serialized dt-idx]]."
  [seen-sids dict-vec flake]
  (let [s (flake/s flake)
        p (flake/p flake)
        dt (flake/dt flake)
        o (flake/o flake)

        [seen-sids' dict-vec' s-idx] (add-sid-to-dict seen-sids dict-vec s)
        [seen-sids'' dict-vec'' p-idx] (add-sid-to-dict seen-sids' dict-vec' p)
        [seen-sids''' dict-vec''' dt-idx] (add-sid-to-dict seen-sids'' dict-vec'' dt)

        ;; Handle o: dict index if SID reference, otherwise serialized value
        [seen-sids'''' dict-vec'''' o-serialized]
        (if (json/subject-reference? dt)
          (let [[ss dv o-idx] (add-sid-to-dict seen-sids''' dict-vec''' o)]
            [ss dv o-idx])
          [seen-sids''' dict-vec''' (json/serialize-object o dt)])]

    [seen-sids'''' dict-vec'''' [s-idx p-idx o-serialized dt-idx]]))

(defn serialize-leaf-with-dict
  "Serialize leaf node with SID dictionary (version 2 format).
   Builds dictionary and serializes flakes in a single pass."
  [leaf]
  (let [flakes (:flakes leaf)]
    (loop [flakes flakes
           seen-sids (transient {})
           dict-vec (transient [])
           serialized-flakes (transient [])]
      (if-let [flake (first flakes)]
        (let [[seen-sids' dict-vec' [s-idx p-idx o-serialized dt-idx]]
              (serialize-flake-indices seen-sids dict-vec flake)

              t (flake/t flake)
              op (flake/op flake)
              m (json/serialize-meta (flake/m flake))

              serialized-flake [s-idx p-idx o-serialized dt-idx t op m]]
          (recur (rest flakes)
                 seen-sids'
                 dict-vec'
                 (conj! serialized-flakes serialized-flake)))
        {"version" 2
         "dict" (mapv iri/serialize-sid (persistent! dict-vec))
         "flakes" (persistent! serialized-flakes)}))))

(defn- deserialize-flake-with-dict
  "Deserialize a flake using dictionary for SID lookups.
   s, p, dt always use dict lookup.
   o uses dict lookup if dt is a reference type, otherwise deserializes as a value."
  [flake-vec sid-dict]
  (let [s-idx  (get flake-vec 0)
        p-idx  (get flake-vec 1)
        o-raw  (get flake-vec 2)
        dt-idx (get flake-vec 3)
        t      (get flake-vec 4)
        op     (get flake-vec 5)
        m      (json/deserialize-meta (get flake-vec 6))

        s  (get sid-dict s-idx)
        p  (get sid-dict p-idx)
        dt (get sid-dict dt-idx)

        ;; o is either a dict index (for references) or a literal value
        o  (if (json/subject-reference? dt)
             (get sid-dict o-raw)
             (json/deserialize-object o-raw dt))]

    (flake/create s p o dt t op m)))

(defn deserialize-leaf-with-dict
  "Deserialize leaf node, auto-detecting format by version.
   - version 2: Dictionary format
   - no version/version 1: Standard format (legacy)"
  [leaf]
  (let [version (get leaf :version)]
    (if (= version 2)
      (let [dict-data (get leaf :dict)
            sid-dict (mapv iri/deserialize-sid dict-data)
            flakes (mapv #(deserialize-flake-with-dict % sid-dict)
                         (get leaf :flakes))]
        (assoc leaf :flakes flakes))
      (json/deserialize-leaf-node leaf))))

(defrecord DictSerializer [json-serde]
  serde/StorageSerializer
  (-serialize-db-root [_ db-root]
    (serde/-serialize-db-root json-serde db-root))

  (-deserialize-db-root [_ db-root]
    (serde/-deserialize-db-root json-serde db-root))

  (-serialize-branch [_ branch]
    (serde/-serialize-branch json-serde branch))

  (-deserialize-branch [_ branch]
    (serde/-deserialize-branch json-serde branch))

  (-serialize-leaf [_ leaf]
    (serialize-leaf-with-dict leaf))

  (-deserialize-leaf [_ leaf]
    (deserialize-leaf-with-dict leaf))

  (-serialize-garbage [_ garbage-map]
    (serde/-serialize-garbage json-serde garbage-map))

  (-deserialize-garbage [_ garbage]
    (serde/-deserialize-garbage json-serde garbage))

  serde/BM25Serializer
  (-serialize-bm25 [_ bm25]
    (serde/-serialize-bm25 json-serde bm25))

  (-deserialize-bm25 [_ bm25]
    (serde/-deserialize-bm25 json-serde bm25)))

(defn json-dict-serde
  "Returns a JSON serializer with dictionary support"
  []
  (->DictSerializer (json/->Serializer)))
