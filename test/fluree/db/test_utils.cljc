(ns fluree.db.test-utils
  (:require [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            #?@(:cljs [[clojure.core.async :refer [go]]
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
  [{:context      {:ex "http://example.org/ns/"}
    :id           :ex/brian,
    :type         :ex/User,
    :schema/name  "Brian"
    :schema/email "brian@example.org"
    :schema/age   50
    :ex/favNums   7}
   {:context      {:ex "http://example.org/ns/"}
    :id           :ex/alice,
    :type         :ex/User,
    :schema/name  "Alice"
    :schema/email "alice@example.org"
    :schema/age   50
    :ex/favNums   [42, 76, 9]}
   {:context      {:ex "http://example.org/ns/"}
    :id           :ex/cam,
    :type         :ex/User,
    :schema/name  "Cam"
    :schema/email "cam@example.org"
    :schema/age   34
    :ex/favNums   [5, 10]
    :ex/friend    [:ex/brian :ex/alice]}])

(defn create-conn
  ([]
   (create-conn {}))
  ([{:keys [context did]
     :or   {context default-context
            did     (did/private->did-map default-private-key)}}]
   (let [conn-p (fluree/connect-memory {:defaults {:context context
                                                   :did     did}})]
     #?(:clj @conn-p :cljs (go (<p! conn-p))))))

(defn load-movies
  [conn]
  (let [ledger @(fluree/create conn "test/movies")]
    (doseq [movie movies]
      (let [staged @(fluree/stage (fluree/db ledger) movie)]
        @(fluree/commit! ledger staged
                         {:message (str "Commit " (get movie "name"))
                          :push? true})))
    ledger))

(defn load-people
  [conn]
  (let [ledger @(fluree/create conn "test/people")
        staged @(fluree/stage (fluree/db ledger) people)]
    @(fluree/commit! ledger staged {:message "Adding people", :push? true})
    ledger))

(defn transact
  [ledger data]
  (->> @(fluree/stage (fluree/db ledger) data)
       (fluree/commit! ledger)))

(defn retry-load
  "Retry loading a ledger until max-attempts. Hopefully not needed once JSON-LD
  code has an equivalent to :syncTo"
  [conn ledger-alias max-attempts]
  (loop [attempt 0]
    (let [ledger (try
                   (let [res @(fluree/load conn ledger-alias)]
                     (if (instance? Throwable res)
                       (throw res)
                       res))
                   (catch Exception e
                     (when (= (inc attempt) max-attempts)
                       (throw e)
                       (Thread/sleep 100))))]
      (if ledger
        ledger
        (recur (inc attempt))))))
