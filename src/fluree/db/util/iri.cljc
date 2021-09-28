(ns fluree.db.util.iri
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld])
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
                 (let [prefix (some (fn [^Flake flake]
                                      (when (= const/$_prefix:prefix (.-p flake))
                                        (.-o flake)))
                                    p-flakes)
                       iri    (some (fn [^Flake flake]
                                      (when (= const/$_prefix:iri (.-p flake))
                                        (.-o flake)))
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
              (json-ld/expand class-iri context)
              class-iri)]
    (get-in db [:schema :pred iri :id])))

