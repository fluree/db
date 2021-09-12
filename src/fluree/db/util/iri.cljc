(ns fluree.db.util.iri
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            #?(:clj [clojure.java.io :as io])
            [clojure.string :as str]
            [fluree.db.util.json :as json]
            [fluree.db.util.core :as util]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [clojure.edn :as edn])
  #?(:clj (:import (fluree.db.flake Flake))))

;; utilities related to iris, prefixes, expansion and compaction

(defn parse-prefix
  [s]
  (try*
    (let [[_ prefix rest] (re-find #"([^:]+):(.+)" s)]
      (if (nil? prefix)
        nil
        [prefix rest]))
    (catch* e
            (log/warn (str "Error attempting to parse iri: " s))
            (throw e))))


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
                     (assoc-in acc [prefix :iri] iri)
                     acc))) {})))

(defn internalize-context
  "Takes a standard JSON context and does some validation, turns
  key parts that are frequently used/looked up (like @id) into keywords (like :id)"
  [ctx]
  (reduce-kv (fn [acc k v]
               (assoc acc k (if (map? v)
                              (assoc v :iri (get v "@id")
                                       :type (get v "@type"))
                              {:iri v})))
             {} ctx))


(def ^:const context-dir "contexts/")

(defn load-external
  "Loads external JSON-LD context if it is registered with Fluree, else returns nil.
  If throw? is true, will throw with exception if context is not available."
  ([context-iri] (load-external context-iri false))
  ([context-iri throw?]
   #?(:cljs (throw (ex-info (str "Loading external contexts is not supported in JS Fluree at this point.")
                            {:status 400 :error :db/invalid-context}))
      :clj  (let [path     (second (str/split context-iri #"://")) ;; remove i.e. http://, or https://
                  edn-ctx  (some-> (str context-dir path ".edn")
                                   io/resource
                                   slurp)
                  json-ctx (when-not edn-ctx
                             (some-> (str context-dir path ".json")
                                     io/resource
                                     slurp))]
              (cond
                edn-ctx (edn/read-string edn-ctx)           ;; we already convert edn contexts
                json-ctx (-> json-ctx (json/parse false) (get "@context") internalize-context)
                throw? (throw (ex-info (str "External context is not registered with Fluree: " context-iri
                                            ". You could supply the context map from the URL directly "
                                            "in your transaction and try again.")
                                       {:status 400 :error :db/invalid-context})))))))


(defn- normalize-context
  "Converts a standard context map into normalized structure where
  we can easily look up @id values that are reference itself. If an external
  context URL (or several) are provided, will load them here.

  i.e. in context {'pfx': 'http://blah.com/ns#', 'bfx': 'pfx:blah'}, 'bfx' will
  need to be able to look up 'pfx' in itself to resolve. Problem is 'pfx' could
  also be defined as {'pfx': {'@id': 'http://blah.com/ns#'}, ...}. This normalizes
  the map so lookups can be consistent."
  [context]
  (cond
    (map? context)
    (internalize-context context)

    (string? context)
    (load-external context true)

    (vector? context)
    (->> context
         (mapv #(load-external % true))
         (apply merge-with util/deep-merge))

    :else
    (throw (ex-info (str "Unrecognized context provided: " context ".")
                    {:status 400 :error :db/invalid-context}))))


(defn expanded-context
  "Returns a fully expanded context map from a source map.
  If a default context is provided, it will be used for prefixes
  that don't match the supplied context. This is typically used
  as a 'parent context' that might have children still utilizing context
  values defined up the tree."
  ([context] (expanded-context context {}))
  ([context default-context]
   (let [context* (normalize-context context)]
     (reduce-kv
       (fn [acc prefix prefix-map]
         (if (= \@ (first prefix))
           (dissoc acc prefix)
           (let [iri         (:iri prefix-map)
                 sub-context (when-let [ctx (get prefix-map "@context")]
                               (expanded-context ctx (merge default-context acc)))
                 [val-prefix rest] (try* (parse-prefix iri)
                                         (catch* e (throw (ex-info (str "While expanding context, error parsing prefix: " prefix prefix-map)
                                                                   {:status     500
                                                                    :error      :db/unexpected-error
                                                                    :prefix-map prefix-map}))))
                 iri*        (if-let [expanded-prefix (or (get-in context* [val-prefix :id])
                                                          (get default-context val-prefix))]
                               (str expanded-prefix rest)
                               iri)]
             (assoc acc prefix (assoc prefix-map
                                 :iri iri*
                                 :prefix prefix
                                 :context sub-context
                                 :container (get prefix-map "@container"))))))
       {} context*))))


(defn item-ctx
  "If a compact-iri resolves to something in the context, returns the context map
  for that specific item."
  [compact-iri context]
  (or (get context compact-iri)                             ;; first try compact-iri without parsing
      (when-let [[prefix rest] (parse-prefix compact-iri)]
        (when-let [sub-ctx (get context prefix)]
          ;; if prefix has an entry in the context, update the :iri to include the new full IRI
          (assoc sub-ctx :iri (str (:iri sub-ctx) rest))))))


(defn expand
  "Expands a compacted iri string to full iri.

  If the iri is not compacted, returns original iri string."
  [compact-iri context]
  (or (:iri (item-ctx compact-iri context))
      ;; no matches, return original name
      compact-iri))


(defn expand-db
  "Expands an iri, if compact, with the db's default context"
  [compact-iri db]
  (expand compact-iri (get-in db [:schema :prefix])))


(defn class-sid*
  [prefix suffix db]
  (when prefix
    (let [match (str prefix suffix)]
      (get-in db [:schema :pred match :id]))))

(defn class-sid
  "Returns the class subject id (or nil).
  First attempts to expand the class-iri to a full iri.
  If a match exists, returns the subject id for the class."
  [class-iri db context]
  (if context
    (or (get-in db [:schema :pred class-iri :id])
        (-> context (get "") :iri (class-sid* class-iri db))
        (when-let [[prefix rest] (parse-prefix class-iri)]
          (-> context (get prefix) :iri (class-sid* rest db))))
    (get-in db [:schema :pred class-iri :id])))


(defn reverse-context
  "Flips context map from prefix -> prefix-map, to iri -> prefix-map"
  [context]
  (reduce-kv #(assoc %1 (:id %3) %2) {} context))


(defn compact-fn
  "Returns a single prefix-resolving function based on the provided context.

  If a prefix can be resolved, returns a 3-tuple of:
  [compacted-iri prefix base-iri]"
  [context]
  (let [flipped    (reverse-context context)                ;; flips context map
        match-iris (->> flipped
                        keys
                        (sort-by #(* -1 (count %))))        ;; want longest iris checked first
        match-fns  (mapv
                     (fn [base-iri]
                       (let [count  (count base-iri)
                             re     (re-pattern (str "^" base-iri))
                             prefix (get-in flipped [base-iri :prefix])]
                         (fn [iri]
                           (when (re-find re iri)
                             [(str prefix ":" (subs iri count)) prefix base-iri]))))
                     match-iris)]
    (fn [iri]
      (some (fn [match-fn]
              (match-fn iri))
            match-fns))))


;; TODO - generating a compacting-fn that works for queries just like above up front will make this
;; more efficient
(defn compact
  "Foes through context and attempts to shorten iri if matches context, else returns original IRI.

  Uses query context format where context values have {:iri 'iri-here'}, so must already be parsed."
  [iri context]
  (let [match-iris (->> (vals context) (map :iri) (sort-by #(* -1 (count %)))) ;; should retain user-provided order assuming context is not massive
        flipped    (reduce-kv #(assoc %1 (:iri %3) %2) {} context)]
    (or (some
          #(when (str/starts-with? iri %)                   ;; match
             (let [prefix (get flipped %)
                   suffix (subs iri (count %))]
               (cond
                 (= "" prefix)
                 (subs iri (count %))

                 (= "" suffix)
                 prefix

                 :else
                 (str prefix ":" suffix))))
          match-iris)
        iri)))


(defn query-context
  "Context primarily for use with queries. Merges DB context based on prefix."
  [ctx db]
  (let [db-ctx (get-in db [:schema :prefix])]
    (if ctx
      (cond
        (string? ctx)
        (assoc-in db-ctx ["" :iri] ctx)

        (map? ctx)
        (reduce-kv
          (fn [acc k v]
            (assoc-in acc [k :iri] v))
          db-ctx ctx)

        :else (throw (ex-info "Invalid query context provided." {:status 400 :error :db/invalid-query})))
      db-ctx)))
