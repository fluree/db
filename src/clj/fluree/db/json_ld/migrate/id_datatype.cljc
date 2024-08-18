(ns fluree.db.json-ld.migrate.id-datatype
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index.rebalance :as rebalance]))

(defn migrate-flake
  [f]
  (if (= (flake/dt f) const/$xsd:anyURI)
    (assoc f :dt const/$id)
    f))

(defn migrate
  [db leaf-size branch-size error-ch]
  (let [flake-xf (map migrate-flake)]
    (rebalance/homogenize db leaf-size branch-size flake-xf error-ch)))
