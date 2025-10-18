(ns index-explore
  (:require [clojure.java.io :as io]
            [fluree.db.storage :as storage]
            [fluree.db.util.json :as json]))

(def data-directory "./dev/data")

(defn set-data-dir!
  [data-dir]
  (alter-var-root #'data-directory (constantly data-dir)))

(defn read-roots
  [ledger-alias]
  (->> (io/file data-directory ledger-alias "index" "root")
       file-seq
       (filter #(.isFile %))
       (map slurp)
       (map (fn [f]
              (json/parse f false)))))

(defn read-index-file
  [address]
  (let [local-path (storage/get-local-path address)]
    (-> data-directory
        (io/file local-path)
        slurp
        (json/parse false))))

(defn at-t
  [t roots]
  (some (fn [r]
          (when (= t (get r "t"))
            r))
        roots))

(defn t-values
  [roots]
  (->> roots
       (map #(get % "t"))
       sort))

(defn latest-t
  [roots]
  (->> roots
       t-values
       (apply max)))

(defn latest-root
  [roots]
  (at-t (latest-t roots) roots))

(defn read-latest-root
  [ledger-name]
  (-> ledger-name read-roots latest-root))

(defn idx-branch
  [root idx-type]
  (let [idx*        (name idx-type)
        idx-address (get-in root [idx* "id"])]
    (read-index-file idx-address)))

(defn expand-idx
  [branch]
  (let [children  (get branch "children")
        children* (mapv
                   (fn [child]
                     (let [child-addr (get child "id")
                           child-data (read-index-file child-addr)
                           child*     (merge child child-data)
                           leaf?      (true? (get child "leaf"))]
                       (if leaf?
                         child*
                         (expand-idx child*))))
                   children)]
    (assoc branch "children" children*)))

(defn read-latest-index
  [ledger-name idx-type]
  (-> ledger-name read-latest-root (idx-branch idx-type) expand-idx))

(defn idx-children
  [root idx-type]
  (let [branch (idx-branch root idx-type)]
    (expand-idx branch)))

(defn next-sibling
  [branch child]
  (let [first-flake (:first child)]
    (loop [[sibling & r] (get branch "children")]
      (when sibling
        (if (= first-flake (key sibling))
          (val sibling)
          (recur r))))))

(defn expand-addresses
  [branch]
  (let [children    (get branch "children")
        child-addrs (mapv
                     (fn [child]
                       (let [child-addr (get child "id")
                             leaf?      (true? (get child "leaf"))]
                         (if leaf?
                           child-addr
                           [child-addr (expand-addresses (read-index-file child-addr))])))
                     children)]
    child-addrs))

(defn idx-addresses
  "Reads all index address and puts in nested vector data structure
  until reaching leafs.

  e.g.
  [root [child1 [child1-1 [child 1-1-1 child1-1-2...]"
  [root idx-type]
  (let [branch-id   (get-in root [(name idx-type) "id"])
        branch-data (idx-branch root idx-type)]
    [branch-id (expand-addresses branch-data)]))

(defn idx-depth
  "Returns the depth of the index structure"
  [root idx-type]
  (let [addresses (idx-addresses root idx-type)]
    (loop [branch addresses
           depth 1]
      ;; each level down it is of the structure [addr [child-addr [child-addr [leaf1, leaf2]]]]
      (let [branch? (sequential? (first (second branch)))]
        (if branch?
          (recur (first (second branch))
                 (inc depth))
          depth)))))
(comment
  (def ledger-name "")

  ;; get latest index-root
  (-> (read-roots ledger-name)
      (latest-root))

  ;; get all nested addresses for :spot
  (-> (read-roots ledger-name)
      (latest-root)
      (idx-addresses :spot))

  ;; get the depth (how many parents) of index type
  (-> (read-roots ledger-name)
      (latest-root)
      (idx-depth :spot)))
