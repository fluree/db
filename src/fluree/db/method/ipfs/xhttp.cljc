(ns fluree.db.method.ipfs.xhttp
  (:require [clojure.core.async :as async]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.xhttp :as xhttp])
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
  ([ipfs-endpoint data]
   (add ipfs-endpoint "json-ld" data))
  ([ipfs-endpoint filename data]
   (go-try
     (let [endpoint (str ipfs-endpoint "api/v0/add")
           req      {:multipart [{:name        filename
                                  :content     data
                                  :contentType "application/ld+json"}]}
           {:keys [Name Hash Size]} (<? (xhttp/post endpoint req {:json? true}))]
       {:name Name
        :hash Hash
        :size (util/str->int Size)}))))

(defn cat
  "Retrieves JSON object from IPFS, returns core async channel with
  parsed JSON. If keywordize-keys? is truthy, will keywordize JSON
  retrieved.

  Returns ex-info exception if failure in either retrieving or parsing."
  [ipfs-endpoint block-id keywordize-keys?]
  (log/debug "Retrieving json from IPFS - cid:" block-id)
  (go-try
    (let [url (str ipfs-endpoint "api/v0/cat?arg=" block-id)
          res (async/<! (xhttp/post-json url nil {:keywordize-keys keywordize-keys?}))]
      (if (util/exception? res)
        (case (:error (ex-data res))
          :db/invalid-json
          (ex-info (str "IPFS file read (cat) JSON parsing exception for CID " block-id)
                   {:status 400 :error :db/invalid-json} res)

          :xhttp/exception
          (ex-info (str "IPFS file read (cat) failed - file unavailable: " (ex-message res))
                   {:status 400 :error :db/file-unavailable} res)

          ;; else
          (ex-info (str "IPFS file read (cat) failed with unexpected exception: " (ex-message res))
                   {:status 500 :error :db/ipfs-failure} res))
        res))))

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
