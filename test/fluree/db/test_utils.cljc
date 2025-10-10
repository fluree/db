(ns fluree.db.test-utils
  (:require #?(:clj [clojure.core.async :as async :refer [<!!]]
               :cljs [clojure.core.async.interop :refer [<p!]])
            #?(:clj [fluree.db.storage.s3 :as s3])
            #?(:cljs [clojure.core.async :as async :refer [go go-loop <! timeout]])
            [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.did :as did]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]))

(def default-context
  {:id     "@id"
   :type   "@type"
   :value  "@value"
   :graph  "@graph"
   :xsd    "http://www.w3.org/2001/XMLSchema#"
   :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
   :sh     "http://www.w3.org/ns/shacl#"
   :schema "http://schema.org/"
   :skos   "http://www.w3.org/2008/05/skos#"
   :wiki   "https://www.wikidata.org/wiki/"
   :f      "https://ns.flur.ee/ledger#"})

(def default-str-context
  {"id"     "@id"
   "type"   "@type"
   "value"  "@value"
   "graph"  "@graph"
   "foaf"   "http://xmlns.com/foaf/0.1/"
   "owl"    "http://www.w3.org/2002/07/owl#"
   "xsd"    "http://www.w3.org/2001/XMLSchema#"
   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"
   "sh"     "http://www.w3.org/ns/shacl#"
   "schema" "http://schema.org/"
   "skos"   "http://www.w3.org/2008/05/skos#"
   "wiki"   "https://www.wikidata.org/wiki/"
   "f"      "https://ns.flur.ee/ledger#"})

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
    "titleEIDR"                 "10.5240/15F9-F913-FF25-8041-E798-O"}
   {"@context"                  "https://schema.org",
    "id"                        "https://www.wikidata.org/wiki/Q2875",
    "type"                      ["Movie"],
    "name"                      "Gone with the Wind",
    "disambiguatingDescription" "1939 film by Victor Fleming",
    "titleEIDR"                 "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4",
    "isBasedOn"                 {"id"     "https://www.wikidata.org/wiki/Q2870",
                                 "type"   "Book",
                                 "name"   "Gone with the Wind",
                                 "isbn"   "0-582-41805-4",
                                 "author" {"@id"   "https://www.wikidata.org/wiki/Q173540"
                                           "@type" "Person"
                                           "name"  "Margaret Mitchell"}}}])

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
    :ex/favNums   [42, 76, 9]
    :schema/birthDate {"@value" "1974-09-26" "@type" :xsd/date}}
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
    :ex/friend    [:ex/brian :ex/alice :ex/cam]
    :schema/birthDate {"@value" "2011-09-26" "@type" :xsd/date}}])

(def people-strings
  [{"@id"           "ex:brian",
    "type"         "ex:User",
    "schema:name"  "Brian"
    "schema:email" "brian@example.org"
    "schema:age"   50
    "ex:favNums"   7}
   {"id"           "ex:alice",
    "type"         "ex:User",
    "schema:name"  "Alice"
    "schema:email" "alice@example.org"
    "schema:age"   50
    "ex:favNums"   [42, 76, 9]}
   {"id"           "ex:cam",
    "type"         "ex:User",
    "schema:name"  "Cam"
    "schema:email" "cam@example.org"
    "schema:age"   34
    "ex:favNums"   [5, 10]
    "ex:friend"    [{"@id" "ex:brian"} {"@id" "ex:alice"}]}
   {"id"           "ex:liam"
    "type"         "ex:User"
    "schema:name"  "Liam"
    "schema:email" "liam@example.org"
    "schema:age"   13
    "ex:favNums"   [42, 11]
    "ex:friend"    [{"@id" "ex:brian"} {"@id" "ex:alice"} {"@id" "ex:cam"}]}])

(defn create-conn
  ([]
   (create-conn {}))
  ([{:keys [did]
     :or   {did (did/private->did-map default-private-key)}}]
   (let [conn-p (fluree/connect-memory {:defaults {:identity did}})]
     #?(:clj @conn-p :cljs (go (<p! conn-p))))))

(defn load-movies
  [conn]
  (let [ledger-id "test/movies"
        db0       @(fluree/create conn ledger-id)]
    (loop [db db0
           [movie & rest] movies]
      (if movie
        (let [staged @(fluree/update db {"@context" default-str-context
                                         "insert" movie})
              committed @(fluree/commit! conn staged
                                         {:message (str "Commit " (get movie "name"))
                                          :push?   true})]
          (recur committed rest))
        db))))

(defn load-people
  [conn]
  (#?(:clj do, :cljs go)
    (let [ledger-id "test/people"
          db0      #?(:clj @(fluree/create conn ledger-id) :cljs (<p! (fluree/create conn ledger-id)))
          staged-p (fluree/update db0 {"@context" [default-context
                                                   {:ex "http://example.org/ns/"}]
                                       "insert" people})
          staged   #?(:clj @staged-p, :cljs (<p! staged-p))
          commit-p (fluree/commit! conn staged {:message "Adding people"
                                                :push? true})]
      #?(:clj @commit-p, :cljs (<p! commit-p))
      ledger-id)))

(defn retry-promise-wrapped
  "Retries a fn that when deref'd might return a Throwable. Intended for
  retrying promise-wrapped API fns. Do not deref the return value, this will
  do it for you. In CLJS it will not retry and will return a core.async chan."
  [pwrapped max-attempts & [retry-on-false?]]
  (#?(:clj loop, :cljs go-loop) [attempt 0]
    (let [res' (try*
                 (let [res (#?(:clj deref, :cljs <p!) (pwrapped))]
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

(defn load-to-t
  "Retries loading until it gets a db whose t value is equal to or
  greater than the given t arg or max-attempts is reached."
  [conn ledger-alias t max-attempts]
  (let [attempts-per-batch (/ max-attempts 10)]
    (loop [attempts-left (- max-attempts attempts-per-batch)]
      (let [db (retry-load conn ledger-alias attempts-per-batch)]
        (cond
          (util/exception? db)
          (throw db)

          (nil? db)
          (throw (ex-info (str "Failed to load ledger: " ledger-alias)
                          {:status 404
                           :error :db/ledger-not-found}))

          :else
          (let [db-t (-> db :t)]
            (cond
              (nil? db-t)
              (throw (ex-info (str "Database has nil :t value. Database keys: " (keys db))
                              {:status 500
                               :error :db/invalid-database}))

              (and (< db-t t) (pos-int? attempts-left))
              (recur (- attempts-left attempts-per-batch))

              :else
              db)))))))

(defn retry-exists?
  "Retry calling exists? until it returns true or max-attempts."
  [conn ledger-alias max-atttemts]
  (retry-promise-wrapped #(fluree/exists? conn ledger-alias) max-atttemts true))

(def base32-pattern
  "[a-z2-7]")

(def base58-pattern
  "[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]")

(def base64-pattern
  "[0-9a-fA-F]")

(def did-regex
  (re-pattern (str "did:key:z" base58-pattern "+")))

(defn did?
  [s]
  (let [result (and (string? s) (re-matches did-regex s))]
    (when-not result
      (log/warn "did? falsey result from:" s))
    result))

(def addr-regex
  (re-pattern "fluree:(memory|file|ipfs)://.+"))

(defn address?
  [s]
  (let [result (and (string? s) (re-matches addr-regex s))]
    (when-not result
      (log/warn "address? falsey result from:" s))
    result))

(def db-id-regex
  (re-pattern (str "fluree:db:sha256:" base32-pattern "{51,53}")))

(defn db-id?
  [s]
  (let [result (and (string? s) (re-matches db-id-regex s))]
    (when-not result
      (log/warn "db-id? falsey result from:" s))
    result))

(def commit-id-regex
  (re-pattern (str "fluree:commit:sha256:" base32-pattern "{51,53}")))

(defn commit-id?
  [s]
  (let [result (and (string? s) (re-matches commit-id-regex s))]
    (when-not result
      (log/warn "commit-id? falsey result from:" s))
    result))

(defn blank-node-id?
  [s]
  (let [result (and (string? s) (str/starts-with? s "_:"))]
    (when-not result
      (log/warn "blank-node-id? falsey result from:" s))
    result))

(defn pred-match?
  "Does a deep compare of expected and actual map values but any predicate fns
  in expected are run with the equivalent value from actual and the result is
  used to determine whether there is a match. Returns true if all pred fns
  return true and all literal values match or false otherwise."
  [expected actual]
  (or (= expected actual)
      (cond
        (fn? expected)
        (expected actual)

        (and (map? expected) (map? actual))
        (every? (fn [k]
                  (pred-match? (get expected k) (get actual k)))
                (set (concat (keys actual) (keys expected))))

        (and (coll? expected) (coll? actual))
        (and (= (count expected) (count actual))
             (every? (fn [[e a]]
                       (pred-match? e a))
                     (zipmap expected actual)))

        :else false)))

(defn set-matcher
  [expected]
  (fn [actual]
    (loop [[e & er]  expected
           actual*   actual]
      (if e
        (let [[result remaining] (loop [[a & ar]  actual*
                                        a-checked []]
                                   (if a
                                     (if (pred-match? e a)
                                       [true (into a-checked ar)]
                                       (recur ar (conj a-checked a)))
                                     [false]))]
          (if result
            (recur er remaining)
            false))
        true))))

(defn error-status
  [ex]
  (-> ex ex-data :status))

(defn error-type
  [ex]
  (-> ex ex-data :error))

(defn shacl-error?
  [x]
  (and (= (error-type x)
          :shacl/violation)
       (= (error-status x)
          422)))

#?(:clj
   (defn s3-available?
     "Check if S3 is available (using AWS credentials)"
     []
     (try
       (let [credentials (s3/get-credentials)]
         (boolean credentials))
       (catch Exception _
         false))))

(defn block-until-index-complete
  "Returns a channel that will receive a value when indexing completes.
   Waits for the :root file message on index-files-ch, then includes a small delay
   to ensure file handles are released.

   Parameters:
     index-files-ch - Channel that receives messages about index file writes
     delay-ms - Optional delay in milliseconds after index completes (default 100ms)

   Returns:
     A channel that will receive `true` when indexing is complete and delay has elapsed.

   Usage:
     (let [index-ch (async/chan 10)
           _        @(fluree/commit! conn db {:index-files-ch index-ch})]
       (<!! (block-until-index-complete index-ch))
       ;; Now safe to reload or access index files
       )"
  ([index-files-ch]
   (block-until-index-complete index-files-ch 100))
  ([index-files-ch delay-ms]
   #?(:clj
      (async/thread
        ;; Wait for index completion (root file is written last)
        (loop []
          (when-let [msg (<!! index-files-ch)]
            (when-not (= :root (:file-type msg))
              (recur))))
        ;; Small delay to ensure file handles are released
        (<!! (async/timeout delay-ms))
        true)
      :cljs
      (go
        ;; Wait for index completion (root file is written last)
        (loop []
          (when-let [msg (<! index-files-ch)]
            (when-not (= :root (:file-type msg))
              (recur))))
        ;; Small delay to ensure file handles are released
        (<! (timeout delay-ms))
        true))))
