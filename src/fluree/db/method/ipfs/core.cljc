(ns fluree.db.method.ipfs.core
  (:require [fluree.db.util.xhttp :as xhttp]
            [fluree.db.method.ipfs.xhttp :as ipfs]
            #?(:clj  [org.httpkit.client :as client]
               :cljs ["axios" :as axios])
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.json :as json]
            [clojure.string :as str]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn ipns-push
  "Adds json from clojure data structure"
  [ipfs-server ipfs-cid]
  (let [endpoint (str ipfs-server "api/v0/name/publish?arg=" ipfs-cid)]
    #?(:clj  @(client/post endpoint {})
       :cljs (let [res (atom nil)]
               (-> axios
                   (.request (clj->js {:url  endpoint
                                       :post "post"
                                       :data {}}))
                   (.then (fn [resp] (reset! res resp)))
                   (.catch (fn [err] (reset! res err))))
               @res))))

(defn default-commit-fn
  "Default push function for IPFS"
  [ipfs-endpoint]
  (fn [json]
    (log/warn "WRITING JSON: " (type json) json)
    (go-try
      (let [res (<? (ipfs/add ipfs-endpoint json))
            {:keys [name]} res]
        (when-not name
          (throw (ex-info (str "IPFS publish error, unable to retrieve IPFS name. Response object: " res)
                          {:status 500 :error :db/push-ipfs})))
        (str "fluree:ipfs://" name)))))

(defn default-push-fn
  "Default publish function updates IPNS record based on a
  provided Fluree IPFS database ID, i.e.
  fluree:ipfs:<ipfs cid>

  Returns an async promise-chan that will eventually contain a result."
  [ipfs-endpoint]
  (fn [fluree-dbid]
    #?(:clj
       (let [p (promise)]
         (future
           (log/info (str "Pushing db " fluree-dbid " to IPNS. (IPNS is slow!)"))
           (let [start-time (System/currentTimeMillis)
                 [_ _ ipfs-cid] (str/split fluree-dbid #":")
                 res        (ipns-push ipfs-endpoint ipfs-cid)
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
                 res        (ipns-push ipfs-endpoint ipfs-cid)
                 seconds    (quot (- (js/Date.now) start-time) 1000)
                 body       (json/parse (:body res))
                 name       (:Name body)]
             #_(when-not name
                 (throw (ex-info (str "IPNS publish error, unable to retrieve IPFS name. Response object: " res)
                                 {:status 500 :error :db/push-ipfs})))
             (log/info (str "Successfully updated fluree:ipns:" name " with db: " fluree-dbid " in "
                            seconds " seconds. (IPNS is slow!)"))
             (resolve (str "fluree:ipns:" name))))))))


(defn default-read-fn
  "Default reading function for IPFS. Reads either IPFS or IPNS docs"
  [ipfs-endpoint]
  (fn [file-key]
    (when-not (string? file-key)
      (throw (ex-info (str "Invalid file key, cannot read: " file-key)
                      {:status 500 :error :db/invalid-commit})))
    (let [[address path] (str/split file-key #"://")
          [type method] (str/split address #":")
          ipfs-cid (str "/" method "/" path)]
      (when-not (and (= "fluree" type)
                     (#{"ipfs" "ipns"} method))
        (throw (ex-info (str "Invalid file type or method: " file-key)
                        {:status 500 :error :db/invalid-commit})))
      (ipfs/cat ipfs-endpoint ipfs-cid))))
