(ns fluree.db.test-utils
  (:require [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?@(:cljs [[clojure.core.async :refer [go go-loop]]
                       [clojure.core.async.interop :refer [<p!]]])))

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
   :f      "https://ns.flur.ee/ledger#"})

(def default-private-key
  "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")

(def movies
  [{"@context"                  "https://schema.org",
    "id"                        "https://www.wikidata.org/wiki/Q836821",
    "type"                      ["Movie"],
    "name"                      "The Hitchhiker's Guide to the Galaxy",
    "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
    "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
    "isBasedOn"                 {"id"     "https://www.wikidata.org/wiki/Q3107329",
                                 "type"   "Book",
                                 "name"   "The Hitchhiker's Guide to the Galaxy",
                                 "isbn"   "0-330-25864-8",
                                 "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                           "@type" "Person"
                                           "name"  "Douglas Adams"}}}
   {"@context"                  "https://schema.org",
    "id"                        "https://www.wikidata.org/wiki/Q91540",
    "type"                      ["Movie"],
    "name"                      "Back to the Future",
    "disambiguatingDescription" "1985 film by Robert Zemeckis",
    "titleEIDR"                 "10.5240/09A3-1F6E-3538-DF46-5C6F-I",
    "followedBy"                {"id"         "https://www.wikidata.org/wiki/Q109331"
                                 "type"       "Movie"
                                 "name"       "Back to the Future Part II"
                                 "titleEIDR"  "10.5240/5DA5-C386-2911-7E2B-1782-L"
                                 "followedBy" {"id" "https://www.wikidata.org/wiki/Q230552"}}}
   {"@context"                  "https://schema.org"
    "id"                        "https://www.wikidata.org/wiki/Q230552"
    "type"                      ["Movie"]
    "name"                      "Back to the Future Part III"
    "disambiguatingDescription" "1990 film by Robert Zemeckis"
    "titleEIDR"                 "10.5240/15F9-F913-FF25-8041-E798-O"}])

(def people
  [{:id           :ex/brian,
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
    :ex/friend    [:ex/brian :ex/alice]}
   {:id           :ex/liam
    :type         :ex/User
    :schema/name  "Liam"
    :schema/email "liam@example.org"
    :schema/age   13
    :ex/favNums   [42, 11]
    :ex/friend    [:ex/brian :ex/alice :ex/cam]}])

(defn create-conn
  ([]
   (create-conn {}))
  ([{:keys [context did context-type]
     :or   {context default-context
            context-type :keyword
            did     (did/private->did-map default-private-key)}}]
   (let [conn-p (fluree/connect-memory {:defaults {:context      context
                                                   :context-type context-type
                                                   :did          did}})]
     #?(:clj @conn-p :cljs (go (<p! conn-p))))))

(defn load-movies
  [conn]
  (let [ledger @(fluree/create conn "test/movies")]
    (doseq [movie movies]
      (let [staged @(fluree/stage (fluree/db ledger) movie)]
        @(fluree/commit! ledger staged
                         {:message (str "Commit " (get movie "name"))
                          :push?   true})))
    ledger))

(defn load-people
  [conn]
  (let [ledger @(fluree/create conn "test/people" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
        staged @(fluree/stage (fluree/db ledger) people)]
    @(fluree/commit! ledger staged {:message "Adding people", :push? true})
    ledger))

(defn transact
  ([ledger data]
   (transact ledger data {}))
  ([ledger data commit-opts]
   (let [staged @(fluree/stage (fluree/db ledger) data)]
     (fluree/commit! ledger staged commit-opts))))

(defn retry-promise-wrapped
  "Retries a fn that when deref'd might return a Throwable. Intended for
  retrying promise-wrapped API fns. Do not deref the return value, this will
  do it for you. In CLJS it will not retry and will return a core.async chan."
  [pwrapped max-attempts & [retry-on-false?]]
  (#?(:clj loop :cljs go-loop) [attempt 0]
    (let [res' (try*
                 (let [res (#?(:clj deref :cljs <p!) (pwrapped))]
                   (if (util/exception? res)
                     (throw res)
                     res))
                 (catch* e e))]
      (if (= (inc attempt) max-attempts)
        (if (util/exception? res')
          (throw res')
          res')
        (if (or (util/exception? res')
                (and retry-on-false? (false? res')))
          (do
            #?(:clj (Thread/sleep 100))
            (recur (inc attempt)))
          res')))))

(defn retry-load
  "Retry loading a ledger until it loads or max-attempts. Hopefully not needed
  once JSON-LD code has an equivalent to :syncTo"
  [conn ledger-alias max-attempts]
  (retry-promise-wrapped #(fluree/load conn ledger-alias) max-attempts))

(defn retry-exists?
  "Retry calling exists? until it returns true or max-attempts."
  [conn ledger-alias max-atttemts]
  (retry-promise-wrapped #(fluree/exists? conn ledger-alias) max-atttemts true))
