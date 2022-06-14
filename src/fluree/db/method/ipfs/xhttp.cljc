(ns fluree.db.method.ipfs.xhttp
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.xhttp :as xhttp]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log])
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
  [ipfs-endpoint block-id]
  (log/debug "Retrieving json from IPFS cid:" block-id)
  (let [url (str ipfs-endpoint "api/v0/cat?arg=" block-id)]
    (xhttp/post-json url nil {:keywordize-keys false})))


#_(defn add-directory
    [ipfs-endpoint data]
    (let [endpoint   (str ipfs-endpoint "api/v0/add")
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
      #?(:clj  @(client/post endpoint req)
         :cljs (let [res (atom nil)]
                 (-> axios
                     (.request (clj->js {:url  endpoint
                                         :post "post"
                                         :data req}))
                     (.then (fn [resp] (reset! res resp)))
                     (.catch (fn [err] (reset! res err))))
                 @res))))


(comment

  (clojure.core.async/<!!
    (cat "http://127.0.0.1:5001/" "/ipfs/QmXh2W5GPnocpiyFYtQKu4cPLDSwdDELYRTyZkhMSKx7vj"))

  (clojure.core.async/<!!
    (xhttp/post-json "http://127.0.0.1:5001/api/v0/cat?arg=/ipns/k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2" nil {:keywordize-keys false}))

  (clojure.core.async/<!!
    (xhttp/post-json "http://127.0.0.1:5001/api/v0/ls?arg=k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2" nil nil))


  (clojure.core.async/<!!
    (ls "http://127.0.0.1:5001/" "/ipns/k51qzi5uqu5dllaos3uy3sx0o8gw221tyaiu2qwmgdzy5lofij0us0h4ai41az"))

  (clojure.core.async/<!!
    (add "http://127.0.0.1:5001/" {:hi "there" :im "blahhere"})
    )

  )