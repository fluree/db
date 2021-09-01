(ns fluree.db.dbproto
  (:refer-clojure :exclude [-lookup]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol IResolve
  "All nodes must implement this protocol. It's includes the minimal functionality
   necessary to avoid resolving nodes unless strictly necessary."
  (-first-flake [node] "Returns the first flake in this node")
  (-rhs [node] "Returns the next node's first flake")
  (-history-count [node] "Returns how many history nodes are present for this node (if a leaf)")
  (-resolve [node] "Returns node resolved with data as async channel")
  (-resolve-to-t [node t idx-novelty] [node t idx-novelty fast-forward-db?] [node t idx-novelty fast-forward-db? remove-flakes]
    "Resolves this node at specified transaction 't'. Novelty included for the specified index.")
  (-resolve-history [node] "Returns the history for data nodes.")
  (-resolve-history-range [node from-t to-t] [node from-t to-t idx-novelty] "Returns the history within specified range of 't' values. From is most recent time."))


(defprotocol INode
  (-lookup [node flake] "Returns the child node which contains the given key")
  (-lookup-after [node flake] "Returns the child node which comes after the given key")
  (-lookup-leaf [node flake] "Returns the leaf node which contains the given key")
  (-lookup-leaf-after [node flake] "Returns the leaf node which comes after the given key"))


(defprotocol IFlureeDb
  (-latest-db [db] "Updates a db to the most current version of the db known to this server. Maintains existing permissions")
  (-rootdb [db] "Returns root db version of this db.")
  (-forward-time-travel [db flakes] [db tt-id flakes])
  ;; schema-related
  (-c-prop [db property collection] "Returns schema property for a collection.")
  (-p-prop [db property predicate] "Returns the property specified for the given predicate.")
  (-class-prop [db property class] "Return class properties")
  ;; following return async chans
  (-tag [db tag-id] [db tag-id pred] "Returns resolved tag, shortens namespace if pred provided.")
  (-tag-id [db tag-name] [db tag-name pred] "Returns the tag sid. If pred provided will namespace tag if not already.")
  (-subid [db ident] [db ident strict?] "Returns subject ID if exists, else nil")
  (-search [db fparts] "Performs a slice, but determines best index to use.")
  (-query [db query] [db query opts] "Performs a query.")
  (-with [db block flakes] [db block flakes opts] "Applies flakes to this db as a new block with possibly multiple 't' transactions.")
  (-with-t [db flakes] [db flakes opts] "Applies flakes to this db as a new 't', but retains current block.")
  (-add-predicate-to-idx [db pred-id] "Adds predicate to idx, return updated db."))


(defn db?
  [db]
  (satisfies? IFlureeDb db))