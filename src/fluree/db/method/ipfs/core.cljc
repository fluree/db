(ns fluree.db.method.ipfs.core
  (:require [fluree.db.util.xhttp :as http]
            [org.httpkit.client :as client]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.json :as json]
            [clojure.string :as str]
            [fluree.db.json-ld-db :as json-ld-db]
            [fluree.db.json-ld.flakes :as json-ld-flakes]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.transact :as jld-transact]))

#?(:clj (set! *warn-on-reflection* true))

(def default-ipfs-server (atom "http://127.0.0.1:5001/"))

(defn set-default-ipfs-server!
  [endpoint]
  (reset! default-ipfs-server endpoint))


(defn get-json
  ([ipfs-id] (get-json @default-ipfs-server ipfs-id))
  ([server block-id]
   (log/debug "Retrieving json from IPFS cid:" block-id)
   (let [url (str server "api/v0/cat?arg=" block-id)
         res @(client/post url {})]
     (try* (json/parse (:body res) false)
           (catch* e (log/error e "JSON parse error for data: " (:body res))
                   (throw e))))))


(defn add-json
  "Adds json from clojure data structure"
  [ipfs-server json]
  (let [endpoint (str ipfs-server "api/v0/add")
        req      {:multipart [{:name        "json-ld"
                               :content     json
                               :contentType "application/ld+json"}]}]
    @(client/post endpoint req)))


(defn add
  "Adds clojure data structure to IPFS by serializing first into JSON"
  [ipfs-server data]
  (let [json (json/stringify data)]
    (add-json ipfs-server json)))


(defn add-directory
  [data]
  (let [endpoint   (str @default-ipfs-server "api/v0/add")
        directory  "blah"
        ledgername "here"
        json       (json/stringify data)
        req        {:multipart [{:name        "file"
                                 :content     json
                                 :filename    (str directory "%2F" ledgername)
                                 :contentType "application/ld+json"}
                                {:name        "file"
                                 :content     ""
                                 :filename    directory
                                 :contentType "application/x-directory"}]}]
    @(client/post endpoint req)))


(defn generate-dag
  "Items must contain :name, :size and :hash"
  [items]
  (let [links     (mapv (fn [{:keys [name size hash]}]
                          {"Hash" {"/" hash} "Name" name "Tsize" size})
                        items)
        dag       {"Data"  {"/" {"bytes" "CAE"}}
                   "Links" links}
        endpoint  (str @default-ipfs-server "api/v0/dag/put?store-codec=dag-pb&pin=true")
        endpoint2 (str @default-ipfs-server "api/v0/dag/put?pin=true")
        req       {:multipart [{:name        "file"
                                :content     (json/stringify dag)
                                :contentType "application/json"}]}]
    @(client/post endpoint req)))


(defn ipns-push
  "Adds json from clojure data structure"
  [ipfs-server ipfs-cid]
  (let [endpoint (str ipfs-server "api/v0/name/publish?arg=" ipfs-cid)]
    @(client/post endpoint {})))


(defn default-commit-fn
  "Default push function for IPFS"
  [ipfs-server]
  (let [server (or ipfs-server @default-ipfs-server)]
    (fn [json]
      (let [res  (add-json server json)
            body (json/parse (:body res))
            name (:Name body)]
        (when-not name
          (throw (ex-info (str "IPFS publish error, unable to retrieve IPFS name. Response object: " res)
                          {:status 500 :error :db/push-ipfs})))
        (str "fluree:ipfs:" name)))))


(defn default-push-fn
  "Default publish function updates IPNS record based on a
  provided Fluree IPFS database ID, i.e.
  fluree:ipfs:<ipfs cid>

  Returns an async promise-chan that will eventually contain a result."
  [ipfs-server]
  (let [server (or ipfs-server @default-ipfs-server)]
    (fn [fluree-dbid]
      (let [p (promise)]
        (future
          (log/info (str "Pushing db " fluree-dbid " to IPNS. (IPNS is slow!)"))
          (let [start-time (System/currentTimeMillis)
                [_ _ ipfs-cid] (str/split fluree-dbid #":")
                res        (ipns-push server ipfs-cid)
                seconds    (quot (- (System/currentTimeMillis) start-time) 1000)
                body       (json/parse (:body res))
                name       (:Name body)]
            #_(when-not name
                (throw (ex-info (str "IPNS publish error, unable to retrieve IPFS name. Response object: " res)
                                {:status 500 :error :db/push-ipfs})))
            (log/info (str "Successfully updated fluree:ipns:" name " with db: " fluree-dbid " in "
                           seconds " seconds. (IPNS is slow!)"))
            (deliver p (str "fluree:ipns:" name))))
        p))))


(defn default-read-fn
  "Default reading function for IPFS. Reads either IPFS or IPNS docs"
  [ipfs-server]
  (let [server (or ipfs-server @default-ipfs-server)]
    (fn [file-key]
      (when-not (string? file-key)
        (throw (ex-info (str "Invalid file key, cannot read: " file-key)
                        {:status 500 :error :db/invalid-commit})))
      (let [[_ method identifier] (str/split file-key #":")
            ipfs-cid (str "/" method "/" identifier)]
        (get-json server ipfs-cid)))))


;; TODO - cljs support, use async version of (client/post ...)
(defn ipfs-block-read
  [{:keys [endpoint] :as opts}]
  (fn [k]
    (go-try
      (get-json endpoint k))))


(defn connect
  [{:keys [endpoint] :as opts}]
  (let [endpoint*  (or endpoint @default-ipfs-server)
        block-read (ipfs-block-read {:endpoint endpoint*})]
    {:block-read  block-read
     :index-read  :TODO
     :transactor? false}))


(defn block-read
  [conn cid]
  ((:block-read conn) cid))


(defn db
  "ipfs IRI looks like: fluree:ipfs:cid"
  ([db-iri] (db db-iri {}))
  ([db-iri opts]
   (let [conn (connect opts)
         [_ method cid] (str/split db-iri #":")
         pc   (async/promise-chan)]
     (async/go
       (try*
         (let [block-data (async/<! (block-read conn cid))
               db         (-> (json-ld-db/blank-db conn method cid (atom {})
                                                   (fn [] (throw (Exception. "NO CURRENT DB FN YET"))))
                              (assoc :t 0))]
           (if (util/exception? block-data)
             (async/put! pc block-data)
             (let [tx-res   (jld-transact/stage db block-data)
                   db-after (:db-after tx-res)]
               (async/put! pc db-after))))
         (catch* e (async/put! pc e))))
     pc)))



(comment

  (-> (get-json "/ipns/k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2")
      fluree.json-ld/expand)

  (add-directory {"hi" "there you 5"})
  (get-json "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH/blah/here")
  (get-json "QmZy5Me2DMHP7iYhdF5mWDkWZCfhzCXY9ouvnjWyq3VK3D/here")
  (json/parse
    "{\"Data\":{\"/\":{\"bytes\":\"CAE\"}},\"Links\":[{\"Hash\":{\"/\":\"QmWwDDrYuHqjZxDJtq9WAyT6tUUJwJdCYzrf4dBfCGaH7v\"},\"Name\":\"here\",\"Tsize\":28}]}"
    false)
  (generate-dag [{:name "filename-a" :hash "QmWwDDrYuHqjZxDJtq9WAyT6tUUJwJdCYzrf4dBfCGaH7v" :size 28}
                 {:name "filename-b" :hash "Qme1QJSGRXCSPrHKXsX8gj77wVwVaNt3Xkmk6RzHPr9kFv" :size 28}])

  )

(comment

  (def mydb (db "fluree:ipfs:QmYBHRyaTybGv2mUKCP5TxShh3rfPQfD1nEkFxRbXRzEVZ"))

  (-> mydb async/<!!)

  @(fluree.db.api/query mydb {:context "https://schema.org/"
                              :select  {"?s" ["*"]}
                              :where   [["?s" "a" "Book"]]})

  (add {"blah" 987798})

  (get-json "Qmc5f6ms6oXGhZLT3uEvg8b3es5yqM4y4vhC6hXNrbk7dj")

  (add {"@context" {"owl" "http://www.w3.org/2002/07/owl#",
                    "ex"  "http://example.org/ns#"},
        "@graph"   [{"@id"   "ex:ontology",
                     "@type" "owl:Ontology"}
                    {"@id"   "ex:Book",
                     "@type" "owl:Class"}
                    {"@id"   "ex:Person",
                     "@type" "owl:Class"}
                    {"@id"   "ex:author",
                     "@type" "owl:ObjectProperty"}
                    {"@id"   "ex:name",
                     "@type" "owl:DatatypeProperty"}
                    {"@type"     "ex:Book",
                     "ex:author" {"@id" "_:b1"}}
                    {"@id"     "_:b1",
                     "@type"   "ex:Person",
                     "ex:name" {"@value" "Fred"
                                "@type"  "xsd:string"}}
                    {"@id"     "ex:someMember",
                     "@type"   "ex:Person",
                     "ex:name" {"@value" "Brian"
                                "@type"  "xsd:string"}}]})

  (get-json "QmYBHRyaTybGv2mUKCP5TxShh3rfPQfD1nEkFxRbXRzEVZ")

  (add {"@context" "https://schema.org/",
        "@graph"   [{"@id"             "http://worldcat.org/entity/work/id/2292573321",
                     "@type"           "Book",
                     "author"          {"@id" "http://viaf.org/viaf/17823"},
                     "inLanguage"      "fr",
                     "name"            "Rouge et le noir",
                     "workTranslation" {"@type" "Book", "@id" "http://worldcat.org/entity/work/id/460647"}}
                    {"@id"               "http://worldcat.org/entity/work/id/460647",
                     "@type"             "Book",
                     "about"             "Psychological fiction, French",
                     "author"            {"@id" "http://viaf.org/viaf/17823"},
                     "inLanguage"        "en",
                     "name"              "Red and Black : A New Translation, Backgrounds and Sources, Criticism",
                     "translationOfWork" {"@id" "http://worldcat.org/entity/work/id/2292573321"},
                     "translator"        {"@id" "http://viaf.org/viaf/8453420"}}]})

  (get-json "QmYBHRyaTybGv2mUKCP5TxShh3rfPQfD1nEkFxRbXRzEVZ")

  (def book-db (db "fluree:ipfs:Qmc5f6ms6oXGhZLT3uEvg8b3es5yqM4y4vhC6hXNrbk7dj"))

  (async/<!! mydb)

  @(fluree.db.api/query book-db
                        {:context "https://schema.org/"
                         :select  {"?s" ["*", {"workTranslation" ["*"]}]}
                         :where   [["?s" "a" "Book"]]})

  (add {"accessibilityControl" ["fullKeyboardControl" "fullMouseControl"],
        "bookFormat"           "EBook/DAISY3",
        "@context"             "https://schema.org",
        "aggregateRating"      {"@type" "AggregateRating", "reviewCount" "0"},
        "numberOfPages"        "598",
        "accessibilityHazard"  ["noFlashingHazard" "noMotionSimulationHazard" "noSoundHazard"],
        "copyrightYear"        "2007",
        "isFamilyFriendly"     "true",
        "name"                 "Holt Physical Science",
        "copyrightHolder"      {"@type" "Organization", "name" "Holt, Rinehart and Winston"},
        "inLanguage"           "en-US",
        "genre"                "Educational Materials",
        "accessibilityFeature" ["largePrint/CSSEnabled"
                                "highContrast/CSSEnabled"
                                "resizeText/CSSEnabled"
                                "displayTransformability"
                                "longDescription"
                                "alternativeText"],
        "publisher"            {"@type" "Organization", "name" "Holt, Rinehart and Winston"},
        "@type"                "Book",
        "isbn"                 "9780030426599",
        "description"          "NIMAC-sourced textbook",
        "accessibilityAPI"     "ARIA"})

  (def movie-db (db "fluree:ipfs:QmWofgUFbvLyqwdmVVKE7K6SQNPrMcigy49cQXYBJm1f2H"))

  @(fluree.db.api/query movie-db
                        {:context "https://schema.org/"
                         :select  {"?s" ["*"]}
                         :where   [["?s" "a" "Movie"]]})


  (json/parse
    "{\n  \"@context\": {\n    \"geojson\": \"https://purl.org/geojson/vocab#\",\n    \"Feature\": \"geojson:Feature\",\n    \"FeatureCollection\": \"geojson:FeatureCollection\",\n    \"GeometryCollection\": \"geojson:GeometryCollection\",\n    \"LineString\": \"geojson:LineString\",\n    \"MultiLineString\": \"geojson:MultiLineString\",\n    \"MultiPoint\": \"geojson:MultiPoint\",\n    \"MultiPolygon\": \"geojson:MultiPolygon\",\n    \"Point\": \"geojson:Point\",\n    \"Polygon\": \"geojson:Polygon\",\n    \"bbox\": {\n      \"@container\": \"@list\",\n      \"@id\": \"geojson:bbox\"\n    },\n    \"coordinates\": {\n      \"@container\": \"@list\",\n      \"@id\": \"geojson:coordinates\"\n    },\n    \"features\": {\n      \"@container\": \"@set\",\n      \"@id\": \"geojson:features\"\n    },\n    \"geometry\": \"geojson:geometry\",\n    \"id\": \"@id\",\n    \"properties\": \"geojson:properties\",\n    \"type\": \"@type\"\n  }\n}"
    false)

  )
