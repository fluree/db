(ns fluree.db.util.iri
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            #?(:clj [clojure.java.io :as io])
            [clojure.string :as str]
            [fluree.db.util.json :as json]
            [fluree.db.util.core :as util])
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
                     (assoc-in acc [prefix :id] iri)
                     acc))) {})))


(declare expanded-context)

(def ^:const context-dir "contexts/")

(defn load-external
  "Loads external JSON-LD context if it is registered with Fluree, else returns nil.
  If throw? is true, will throw with exception if context is not available."
  ([context-iri] (load-external context-iri false))
  ([context-iri throw?]
   #?(:cljs (throw (ex-info (str "Loading external contexts is not supported in JS Fluree at this point.")
                            {:status 400 :error :db/invalid-context}))
      :clj  (let [path    (second (str/split context-iri #"://")) ;; remove i.e. http://, or https://
                  context (some-> (str context-dir path ".json")
                                  io/resource
                                  slurp)]
              (cond
                context (-> context
                            (json/parse false)
                            (get "@context"))

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
  (let [context* (cond
                   (map? context)
                   context

                   (string? context)
                   (load-external context true)

                   (vector? context)
                   (->> context
                        (mapv #(load-external % true))
                        (apply merge-with util/deep-merge))

                   :else
                   (throw (ex-info (str "Unrecognized context provided: " context ".")
                                   {:status 400 :error :db/invalid-context})))]
    (reduce-kv (fn [acc k v]
                 (assoc acc k (if (map? v)
                                (assoc v :id (get v "@id"))
                                {:id v})))
               {} context*)))


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
           (let [iri         (:id prefix-map)
                 sub-context (when-let [ctx (get prefix-map "@context")]
                               (expanded-context ctx (merge default-context acc)))
                 [val-prefix rest] (parse-prefix iri)
                 iri*        (if-let [expanded-prefix (or (get-in context* [val-prefix :id])
                                                          (get default-context val-prefix))]
                               (str expanded-prefix rest)
                               iri)]
             (assoc acc prefix (assoc prefix-map
                                 :id iri*
                                 :prefix prefix
                                 :context sub-context
                                 :type (get prefix-map "@type")
                                 :container (get prefix-map "@container"))))))
       {} context*))))


(defn expand
  "Expands a compacted iri string to full iri.

  If the iri is not compacted, returns original iri string."
  [compact-iri context]
  (let [[prefix rest] (parse-prefix compact-iri)
        expanded (when prefix
                   (when-let [p-iri (get-in context [prefix :id])]
                     (str p-iri rest)))]
    (or expanded                                            ;; use expanded if avail
        (get-in context [compact-iri :id])                  ;; try to see if entire name is in context
        compact-iri)))                                      ;; no matches, return original name


(defn expand-db
  "Expands an iri, if compact, with the db's default context"
  [compact-iri db]
  (expand compact-iri (get-in db [:schema :prefix])))


(defn class-sid
  "Returns the class subject id (or nil).
  First attempts to expand the class-iri to a full iri.
  If a match exists, returns the subject id for the class."
  [class-iri db]
  (let [expanded-iri (expand-db class-iri db)]
    (get-in db [:schema :pred expanded-iri :id])))


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