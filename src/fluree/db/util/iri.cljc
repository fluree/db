(ns fluree.db.util.iri
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

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
       (group-by flake/s)
       (reduce (fn [acc [_ p-flakes]]
                 (let [prefix (some (fn [flake]
                                      (when (= const/$_prefix:prefix (flake/p flake))
                                        (flake/o flake)))
                                    p-flakes)
                       iri    (some (fn [flake]
                                      (when (= const/$_prefix:iri (flake/p flake))
                                        (flake/o flake)))
                                    p-flakes)]
                   (if (and prefix iri)
                     (assoc-in acc [prefix :id] iri)
                     acc))) {})))


(defn class-sid
  "Returns the class subject id (or nil).
  First attempts to expand the class-iri to a full iri.
  If a match exists, returns the subject id for the class."
  [class-iri db context]
  (let [iri (if context
              (json-ld/expand-iri class-iri context)
              class-iri)]
    (get-in db [:schema :pred iri :id])))
