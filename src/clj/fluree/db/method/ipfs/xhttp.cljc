(ns fluree.db.method.ipfs.xhttp
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.xhttp :as xhttp]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true])
  (:refer-clojure :exclude [cat]))

#?(:clj (set! *warn-on-reflection* true))

(defn key-list
  "Returns a map of key names and corresponding key ids"
  [ipfs-endpoint]
  (go-try
    (let [url (str ipfs-endpoint "api/v0/key/list")
          res (<? (xhttp/post-json url {} nil))]
      (->> res
           :Keys
           (reduce (fn [acc {:keys [Name Id]}]
                     (assoc acc Name Id))
                   {})))))


(defn ls
  "Performs ipfs directory list. Returns vector of directory items.
  If a directory item is a sub-directory, :type will = 1.
  Returns core-async channel with results."
  [ipfs-endpoint ipfs-address]
  (go-try
    (let [endpoint (str ipfs-endpoint "api/v0/ls?arg=" ipfs-address)
          nodes    (-> (<? (xhttp/post-json endpoint nil nil))
                       :Objects
                       first
                       :Links)]
      (mapv
        (fn [{:keys [Name Hash Size Type Target]}]
          {:name   Name
           :hash   Hash
           :size   Size
           :type   Type
           :target Target})
        nodes))))

(defn add
  "Adds payload data to IPFS.
  Returns core async channel with exception or map containing keys:
  :name - name
  :hash - hash (likely same as name)
  :size - size of file."
  [ipfs-endpoint data]
  (go-try
    (let [endpoint (str ipfs-endpoint "api/v0/add")
          req      {:multipart [{:name        "json-ld"
                                 :content     data
                                 :contentType "application/ld+json"}]}
          {:keys [Name Hash Size]} (<? (xhttp/post endpoint req {:json? true}))]
      {:name Name
       :hash Hash
       :size (util/str->int Size)})))

(defn cat
  "Retrieves JSON object from IPFS, returns core async channel with
  parsed JSON without keywordizing keys."
  [ipfs-endpoint block-id keywordize-keys?]
  (log/debug "Retrieving json from IPFS cid:" block-id)
  (let [url (str ipfs-endpoint "api/v0/cat?arg=" block-id)]
    (xhttp/post-json url nil {:keywordize-keys keywordize-keys?})))


(defn publish
  "Publishes ipfs-cid to IPNS server using specified IPNS address key.
  Returns core async channel with response."
  [ipfs-endpoint ipfs-cid key]
  (log/debug "Publishing IPNS update for key:" key "with IPFS CID:" ipfs-cid)
  (go-try
    (let [endpoint (cond-> (str ipfs-endpoint "api/v0/name/publish?")
                           key (str "key=" key "&")
                           true (str "arg=" ipfs-cid))
          {:keys [Name Value] :as res} (<? (xhttp/post-json endpoint nil {:request-timeout 200000}))]
      (log/debug "IPNS publish complete with response: " res)
      {:name  Name
       :value Value})))

(defn name-resolve
  [ipfs-endpoint ipns-key]
  (go-try
    (let [endpoint (cond-> (str ipfs-endpoint "api/v0/name/resolve?")
                           ipns-key (str "arg=" ipns-key))
          {:keys [Path] :as res} (<? (xhttp/post-json endpoint nil {:request-timeout 200000}))]
      (log/debug "IPNS name resolve complete with response: " res)
      Path)))


(comment
  (def ipfs-endpoint "http://127.0.0.1:5001/")
  (require '[clojure.core.async :as async])

  (async/<!! (name-resolve ipfs-endpoint "k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2"))

  (async/go
    (let [resp (async/<! (ls ipfs-endpoint "/ipfs/QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc/"))]
      (println resp)))

  (async/go
    (println (async/<! (cat ipfs-endpoint (str "/ipfs/QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc/"
                                               "readme")))))

  (async/go
    (println (async/<! (add ipfs-endpoint "Twas brillig"))))


  (async/go
    (println (async/<! (publish ipfs-endpoint "Twas brillig"))))


  (clojure.core.async/<!!
    (cat "http://127.0.0.1:5001/" "/ipfs/QmXh2W5GPnocpiyFYtQKu4cPLDSwdDELYRTyZkhMSKx7vj"))

  (clojure.core.async/<!!
    (publish "http://127.0.0.1:5001/" "/ipfs/QmPTXAvmWrmcbqAPxY82N6nRcLUyEWP51UZVtq15CDMVYs" "Fluree1"))

  (clojure.core.async/<!!
    (xhttp/post-json "http://127.0.0.1:5001/api/v0/cat?arg=/ipns/k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2" nil {:keywordize-keys false}))

  (clojure.core.async/<!!
    (xhttp/post-json "http://127.0.0.1:5001/api/v0/ls?arg=k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2" nil nil))


  (clojure.core.async/<!!
    (ls "http://127.0.0.1:5001/" "/ipns/k51qzi5uqu5dllaos3uy3sx0o8gw221tyaiu2qwmgdzy5lofij0us0h4ai41az"))

  (clojure.core.async/<!!
    (add "http://127.0.0.1:5001/" {:hi "there" :im "blahhere"})))

