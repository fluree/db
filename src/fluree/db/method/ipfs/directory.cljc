(ns fluree.db.method.ipfs.directory
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.method.ipfs.xhttp :as ipfs]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.xhttp :as xhttp]))

#?(:clj (set! *warn-on-reflection* true))

;; Manages IPFS directory structure and cache

;; maintains current state tree for IPNS
(def ipns-state (atom {}))

(defn generate-dag
  "Generates a dag directory file given list/vector of items to add in the file
  where each item must have:
  :name - name of file
  :hash - the IPFS CID hash for the field the name points to
  :size - the size of the file contained in the hash"
  [ipfs-endpoint items]
  (go-try
    (let [links    (mapv (fn [{:keys [name size hash]}]
                           {"Hash" {"/" hash} "Name" name "Tsize" size})
                         items)
          dag      {"Data"  {"/" {"bytes" "CAE"}}
                    "Links" links}
          endpoint (str ipfs-endpoint "api/v0/dag/put?store-codec=dag-pb&pin=true")
          req      {:multipart [{:name        "file"
                                 :content     dag
                                 :contentType "application/json"}]}
          res      (<? (xhttp/post-json endpoint req nil))]
      (-> res :Cid :/))))

(defn write-dag!
  "Returns async channel that will contain newly written dag cid"
  [ipfs-endpoint items]
  (log/debug "Attempting to write IPFS dag: " items)
  (let [dag-items (mapv (fn [{:keys [hash name size]}]
                          {:name name
                           :hash hash
                           :size (or size 0)})
                        items)]
    (generate-dag ipfs-endpoint dag-items)))

(defn update-parents!
  "Once a new leaf node is written, traverses up the tree to update parents."
  [dag-map ipfs-endpoint path]
  (go-try
    (let [dag-path   (into [:child] (interpose :child path))
          children   (:child (get-in dag-map dag-path))
          parent-cid (<? (write-dag! ipfs-endpoint (vals children)))
          dag-map*   (-> dag-map                            ;; add in updated :hash + :name (if new dag won't have :name yet)
                         (assoc-in (conj dag-path :hash) parent-cid)
                         (assoc-in (conj dag-path :name) (last dag-path)))
          rest-path  (pop path)]
      (if (empty? rest-path)
        dag-map*
        (<? (update-parents! ipfs-endpoint dag-map* rest-path))))))

(defn update-directory!
  "Stores updates to the dag map and returns updated version"
  [dag-map ipfs-endpoint relative-address new-cid size]
  (go-try
    (let [path           (str/split relative-address #"/")
          parents        (pop path)
          parent-path    (into [:child] (interpose :child path))
          new-node       {:name (last path)
                          :hash new-cid
                          :size size}
          existing-child (get-in dag-map (pop parent-path))
          new-child      (assoc existing-child (:name new-node) new-node)
          dag-map*       (assoc-in dag-map (pop parent-path) new-child)
          dag-map**      (if (empty? parents)
                           dag-map*
                           (<? (update-parents! dag-map* ipfs-endpoint parents)))
          parent-cid     (<? (write-dag! ipfs-endpoint (vals (:child dag-map**))))]
      (assoc dag-map** :hash parent-cid))))

(defn dag-map
  "Returns a nested map of a directory-dag containing the key of
  the file/directory name and values of maps with keys:
  :cid - ipfs cid
  :size - file size (only for files, not for sub-directories)
  :child - sub-directory contents (only for directories)"
  ([ipfs-endpoint root-cid] (dag-map ipfs-endpoint root-cid ""))
  ([ipfs-endpoint root-cid parent-name]
   (go-try
     (let [base-nodes (<? (ipfs/ls ipfs-endpoint root-cid))]
       (loop [[node & r] base-nodes
              acc {:hash  root-cid
                   :name  parent-name
                   :child nil}]
         (if node
           (let [{:keys [name hash size type]} node
                 directory? (= 1 type)]
             (if directory?
               (recur r (assoc-in acc [:child name] (<? (dag-map ipfs-endpoint hash name))))
               (recur r (assoc-in acc [:child name] {:name name
                                                     :hash hash
                                                     :size size}))))
           acc))))))

(defn flatten-dag
  "Flattens our dag representation returning a list of two-tuples as:
  [relative-path ipfs-cid]"
  [node prefix]
  (reduce-kv (fn [acc _ {:keys [name child hash]}]
               (let [name* (if prefix
                             (str prefix "/" name)
                             name)]
                 (if child
                   (into acc (flatten-dag child name*))
                   (conj acc [name* (str "fluree:ipfs://" hash)]))))
             [] node))

(defn list-all
  "Takes a root address, like IPNS, and returns a map of all the ledgers and
  their respective endpoints.

  Return map looks like:
  {'my/db'      'Qmbjig3cZbUUufWqCEFzyCppqdnmQj3RoDjJWomnqYGy1f'
   'another/db' 'Qmz...'
   'a-db        'Qmx...}"
  [ipfs-endpoint root-cid]
  (go-try
    (let [{:keys [child]} (<? (dag-map ipfs-endpoint root-cid))]
      (->> (flatten-dag child nil)
           (into {})))))

;; TODO - probably makes sense to have a queue for updates, and apply multiple pending updates simultaneously under the same IPNS address
(defn refresh-state
  "Updates the ipns state map with latest directory + hashes.
  Returns updated map."
  ([ipfs-endpoint ipns-address]
   (refresh-state ipfs-endpoint ipns-address ipns-state))
  ([ipfs-endpoint ipns-address ipns-state-atom]
   (go-try
     (let [dag-map  (async/<! (dag-map ipfs-endpoint ipns-address))
           dag-map* (if (util/exception? dag-map)
                      (do
                        (log/info (str "IPNS address does not yet hold a Fluree ledger: " ipns-address))
                        nil)
                      dag-map)]
       (swap! ipns-state-atom assoc ipns-address dag-map*)
       dag-map*))))
