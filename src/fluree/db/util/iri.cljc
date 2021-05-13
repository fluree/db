(ns fluree.db.util.iri
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])])
  #?(:clj (:import (fluree.db.flake Flake))))

;; utilities related to iris, prefixes, expansion and compaction

(defn parse-prefix
  [s]
  (let [[_ prefix rest] (re-find #"([^:]+):(.+)" s)]
    (if (nil? prefix)
      nil
      [prefix rest])))

(defn system-context
  "Returns context/prefix for the db when given all of the prefix flakes."
  [prefix-flakes]
  (->> prefix-flakes
       (group-by #(.-s ^Flake %))
       (reduce (fn [acc [_ p-flakes]]
                 (let [prefix (some #(when (= const/$_prefix:prefix (.-p %))
                                       (.-o %)) p-flakes)
                       iri    (some #(when (= const/$_prefix:iri (.-p %))
                                       (.-o %)) p-flakes)]
                   (if (and prefix iri)
                     (assoc acc prefix iri)
                     acc))) {})))

(defn expanded-context
  "Returns a fully expanded context map from a source map"
  ([context] (expanded-context context nil))
  ([context default-context]
   (merge default-context
          (->> context
               (reduce-kv
                 (fn [acc prefix iri]
                   (if-let [[val-prefix rest] (parse-prefix iri)]
                     (if-let [iri-prefix (or (get acc val-prefix)
                                             (get default-context val-prefix))]
                       (assoc acc prefix (str iri-prefix rest))
                       acc)
                     acc))
                 context)))))

(defn expand
  "Expands a compacted iri string to full iri.

  If the iri is not compacted, returns original iri string."
  [compact-iri context]
  (if-let [[prefix rest] (parse-prefix compact-iri)]
    (if-let [p-iri (get context prefix)]
      (str p-iri rest)
      compact-iri)
    compact-iri))


(defn expand-db
  "Expands an iri, if compact, with the db's default context"
  [compact-iri db]
  (expand compact-iri (get-in db [:schema :prefix])))


(defn reverse-context
  "Flips context map from prefix -> iri, to iri -> prefix"
  [context]
  (reduce-kv #(assoc %1 %3 %2) {} context))


(defn compact-fn
  "Returns a single prefix-resolving function based on the provided context.

  If a prefix can be resolved, returns a 3-tuple of:
  [compacted-iri prefix base-iri]"
  [context]
  (let [flipped    (reverse-context context)            ;; flips context map
        match-iris (->> flipped
                        keys
                        (sort-by #(* -1 (count %))))        ;; want longest iris checked first
        match-fns  (mapv
                     (fn [base-iri]
                       (let [count  (count base-iri)
                             re     (re-pattern (str "^" base-iri))
                             prefix (get flipped base-iri)]
                         (fn [iri]
                           (when (re-find re iri)
                             [(str prefix ":" (subs iri count)) prefix base-iri]))))
                     match-iris)]
    (fn [iri]
      (some (fn [match-fn]
              (match-fn iri))
            match-fns))))