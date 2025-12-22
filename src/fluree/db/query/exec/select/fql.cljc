(ns fluree.db.query.exec.select.fql
  (:require [clojure.core.async :as async :refer [go >!]]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util :refer [catch* try*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defmulti display
  (fn [match _compact]
    (where/get-datatype-iri match)))

(defmethod display :default
  [match _compact]
  (where/get-value match))

(defmethod display "@json"
  [match _compact]
  (-> match where/get-value (json/parse false)))

(defmethod display const/iri-id
  [match compact]
  (some-> match where/get-iri compact))

(defmethod display const/iri-vector
  [match _compact]
  (some-> match where/get-value vec))

(defn format-variable-selector-value
  [var]
  (fn [_ _db _iri-cache _context compact _tracker error-ch solution]
    (go (try* (-> solution (get var) (display compact))
              (catch* e
                (log/error e "Error formatting variable:" var)
                (>! error-ch e))))))

(defn format-wildcard-selector-value
  [_ _db _iri-cache _context compact _tracker error-ch solution]
  (go
    (try*
      (loop [[var & vars] (sort (remove nil? (keys solution))) ; implicit grouping can introduce nil keys in solution
             result {}]
        (if var
          (let [output (-> solution (get var) (display compact))]
            (recur vars (assoc result var output)))
          result))
      (catch* e
        (log/error e "Error formatting wildcard")
        (>! error-ch e)))))

(defn format-as-selector-value
  [bind-var]
  (fn [_ _ _ _ compact _ _ solution]
    (go (-> solution (get bind-var) (display compact)))))
