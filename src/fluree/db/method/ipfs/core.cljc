(ns fluree.db.method.ipfs.core
  (:require [fluree.db.util.xhttp :as xhttp]
            #?(:clj  [org.httpkit.client :as client]
               :cljs ["axios" :as axios])
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.json :as json]
            [clojure.string :as str]
            [fluree.db.json-ld-db :as json-ld-db]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

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
         res #?(:clj @(client/post url {})
                :cljs (let [res (atom nil)]
                        (-> axios
                            (.request (clj->js {:url url
                                                :post "post"
                                                :data {}}))
                            (.then (fn [resp] (reset! res resp)))
                            (.catch (fn [err] (reset! res err))))
                        @res))]
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
    #?(:clj @(client/post endpoint req)
       :cljs (let [res (atom nil)]
               (-> axios
                   (.request (clj->js {:url endpoint
                                       :post "post"
                                       :data req}))
                   (.then (fn [resp] (reset! res resp)))
                   (.catch (fn [err] (reset! res err))))
               @res))))


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
    #?(:clj @(client/post endpoint req)
       :cljs (let [res (atom nil)]
               (-> axios
                   (.request (clj->js {:url endpoint
                                       :post "post"
                                       :data req}))
                   (.then (fn [resp] (reset! res resp)))
                   (.catch (fn [err] (reset! res err))))
               @res))))


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
    #?(:clj @(client/post endpoint req)
       :cljs (let [res (atom nil)]
               (-> axios
                   (.request (clj->js {:url endpoint
                                       :post "post"
                                       :data req}))
                   (.then (fn [resp] (reset! res resp)))
                   (.catch (fn [err] (reset! res err))))
               @res))))


(defn ipns-push
  "Adds json from clojure data structure"
  [ipfs-server ipfs-cid]
  (let [endpoint (str ipfs-server "api/v0/name/publish?arg=" ipfs-cid)]
    #?(:clj @(client/post endpoint {})
       :cljs (let [res (atom nil)]
               (-> axios
                   (.request (clj->js {:url endpoint
                                       :post "post"
                                       :data {}}))
                   (.then (fn [resp] (reset! res resp)))
                   (.catch (fn [err] (reset! res err))))
               @res))))


(defn default-commit-fn
  "Default push function for IPFS"
  [ipfs-server]
  (let [server (or ipfs-server @default-ipfs-server)]
    (fn
      ([json]
       (let [res  (add-json server json)
             body (json/parse (:body res))
             name (:Name body)]
         (when-not name
           (throw (ex-info (str "IPFS publish error, unable to retrieve IPFS name. Response object: " res)
                           {:status 500 :error :db/push-ipfs})))
         (str "fluree:ipfs:" name)))
      ([json opts]
       (throw (ex-info (str "IPFS commit does not support a second argument: opts." )
                       {:status 500 :error :db/commit-ipfs-2}))))))


(defn default-push-fn
  "Default publish function updates IPNS record based on a
  provided Fluree IPFS database ID, i.e.
  fluree:ipfs:<ipfs cid>

  Returns an async promise-chan that will eventually contain a result."
  [ipfs-server]
  (let [server (or ipfs-server @default-ipfs-server)]
    (fn [fluree-dbid]
      #?(:clj
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
           p)
         :cljs
         (js/Promise
           (fn [resolve reject]
             (log/info (str "Pushing db " fluree-dbid " to IPNS. (IPNS is slow!)"))
             (let [start-time (js/Date.now)
                   [_ _ ipfs-cid] (str/split fluree-dbid #":")
                   res (ipns-push server ipfs-cid)
                   seconds (quot (- (js/Date.now) start-time) 1000)
                   body (json/parse (:body res))
                   name (:Name body)]
               #_ (when-not name
                    (throw (ex-info (str "IPNS publish error, unable to retrieve IPFS name. Response object: " res)
                                    {:status 500 :error :db/push-ipfs})))
               (log/info (str "Successfully updated fluree:ipns:" name " with db: " fluree-dbid " in "
                              seconds " seconds. (IPNS is slow!)"))
               (resolve (str "fluree:ipns:" name)))))))))


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
   (throw (ex-info "DEPRECATED call to: fluree.db.method.ipfs.core/db" {}))
   ;; TODO - clean up this fn, not deleting yet as we still need the logic migrated
   #_(let [conn (connect opts)
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

