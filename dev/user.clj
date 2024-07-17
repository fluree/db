(ns user
  (:require [fluree.db.api :as fluree]
            [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn :refer [refresh refresh-all]]
            [clojure.core.async :as async]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.did :as did]
            [fluree.db.util.async :refer [<? <?? go-try merge-into?]]
            [fluree.db.flake :as flake]
            [fluree.db.util.json :as json]
            [fluree.db.serde.json :as serdejson]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.json-ld :as json-ld]
            [clojure.string :as str]
            [criterium.core :refer [bench]]
            ;; cljs
            [figwheel-sidecar.repl-api :as ra]))

;; make sure all of following response are async
;; http-api/db-handler* (transator)


(set! *warn-on-reflection* true)

(defn read-json-resource
  "Utility function to read and parse a json file on the resource path into edn
  without converting anything to keywords"
  [rsc]
  (some-> rsc io/resource slurp (json/parse false)))


(def default-private-key
  "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")


(def did (did/private->did-map default-private-key))

(def default-context
  {:id     "@id"
   :type   "@type"
   :xsd    "http://www.w3.org/2001/XMLSchema#"
   :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
   :sh     "http://www.w3.org/ns/shacl#"
   :schema "http://schema.org/"
   :skos   "http://www.w3.org/2008/05/skos#"
   :wiki   "https://www.wikidata.org/wiki/"
   :f      "https://ns.flur.ee/ledger#"
   :ex     "http://example.org/ns/"})

(comment

  (def file-conn @(fluree/connect {:method       :file
                                   :storage-path "dev/data"
                                   :defaults     {:did did}}))

  (def ledger-alias "user/test")

  (def ledger @(fluree/create file-conn ledger-alias))

  (def db1 @(fluree/stage
              (fluree/db ledger)
              {"@context" default-context
               "insert"   [{:id           :ex/brian,
                            :type         :ex/User,
                            :schema/name  "Brian"
                            :schema/email "brian@example.org"
                            :schema/age   50
                            :ex/favNums   7}
                           {:id           :ex/alice,
                            :type         :ex/User,
                            :schema/name  "Alice"
                            :schema/email "alice@example.org"
                            :schema/age   50
                            :ex/favNums   [42, 76, 9]}
                           {:id           :ex/cam,
                            :type         :ex/User,
                            :schema/name  "Cam"
                            :schema/email "cam@example.org"
                            :schema/age   34
                            :ex/favNums   [5, 10]
                            :ex/friend    [:ex/brian :ex/alice]}]}))

  @(fluree/query db1 {:context default-context
                      :select  '[?e ?n]
                      :where   '{:id          ?e
                                 :schema/name ?n}})

  (def db2 @(fluree/commit! ledger db1 {:message "hi"}))

  (-> @(fluree/load file-conn ledger-alias)
      (fluree/db)
      (fluree/query {:context default-context
                     :select  '[?e ?n]
                     :where   '{:id          ?e
                                :schema/name ?n}})
      deref)

  )




                                        ;cljs-stuff
(defn start [] (ra/start-figwheel!))
(defn start-cljs [] (ra/cljs-repl "dev"))
                                        ;(defn stop [] (ra/stop-figwheel!))  ;; errs
