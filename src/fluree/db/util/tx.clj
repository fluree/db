(ns fluree.db.util.tx
  (:require [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.flake :as flake]))

;; transaction utilities


(defn validate-command
  "Takes a command (map) and validates signature, adds in auth or authority and does
  some additional checks. This can be done before putting the command into the queue for processing.

  Puts original :cmd string and :sig string into this one map for use downstream."
  [{:keys [sig cmd]}]
  ;; TODO - here again we calc the sha3 id, I think redundant at this point
  (let [cmd-map       (-> (json/parse cmd)
                          (assoc :txid (crypto/sha3-256 cmd) ;; don't trust their id if provided
                                 :cmd cmd
                                 :sig sig))

        sig-authority (crypto/account-id-from-message cmd sig) ;; throws if invalid signature
        ;; merge everything together into one map for transaction.
        current-time  (System/currentTimeMillis)
        {:keys [auth authority expire]} cmd-map
        expired?      (and expire (< expire current-time))
        _             (when expired?
                        (throw (ex-info (format "Transaction is expired. Current time: %s expire time: %s." current-time expire)
                                        {:status 400 :error :db/invalid-transaction})))
        cmd-map*      (cond
                        (and (nil? auth) (nil? authority))
                        (assoc cmd-map :auth sig-authority)

                        (and (nil? auth) authority)
                        (throw (ex-info (str "An authority without an auth is not allowed.")
                                        {:status 400 :error :db/invalid-transaction}))

                        (and auth authority)
                        (if (= authority sig-authority)
                          cmd-map
                          (throw (ex-info (format "Signing authority: %s does not match command authority: %s." sig-authority authority)
                                          {:status 400 :error :db/invalid-transaction})))

                        (and auth (nil? authority))
                        (if (= auth sig-authority)
                          cmd-map
                          (assoc cmd-map :authority sig-authority)))]
    cmd-map*))


(defn get-tx-meta-from-tx
  "Separates tx-meta from the rest of the transaction.
  If by chance tx-meta was included twice, will throw an exception."
  [txn]
  (let [grouped (group-by #(if (str/starts-with? (:_id %) "_tx")
                             :tx-meta
                             :rest-tx) txn)
        tx-meta (when-let [tx-meta+ (not-empty (:tx-meta grouped))]
                  (when (not= 1 (count tx-meta+))
                    (throw (ex-info "You have multiple _tx metadata records in a single transaction, only one is allowed."
                                    {:status 400 :error :db/invalid-transaction})))
                  (->> tx-meta+
                       first
                       (reduce-kv (fn [acc k v]
                                    (cond
                                      (or (= :_id k) (= :_action k) (= :_meta k))
                                      (assoc acc k v)

                                      (nil? (namespace k))
                                      (assoc acc (keyword "_tx" (name k)) v)

                                      :else
                                      (assoc acc k v)))
                                  {})))]
    {:tx-meta tx-meta
     :rest-tx (:rest-tx grouped)}))

(defn gen-tx-hash
  "From a list of transaction flakes, returns the sha3 hash.

  Note, this assumes the _tx/hash flake is NOT included in this list,
  else the resulting hash will be different from the one that would have
  been computed when performing the transaction."
  ([tx-flakes]
   ;; sort in block sort order as defined by fluree.db.flake/cmp-flakes-block
   (-> (apply flake/sorted-set-by flake/cmp-flakes-block tx-flakes)
       (gen-tx-hash true)))
  ([tx-flakes sorted?]
   (if-not sorted?
     (gen-tx-hash tx-flakes)
     (->> tx-flakes
          (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
          (json/stringify)
          (crypto/sha3-256)))))


;;;
;;; Block merkle root calculation
;;;

(defn- exp [x n]
  (loop [acc 1 n n]
    (if (zero? n) acc
                  (recur (* x acc) (dec n)))))

(defn- find-closest-power-2
  [n]
  (loop [i 1]
    (if (>= (exp 2 i) n)
      (exp 2 i)
      (recur (inc i)))))

(defn- generate-hashes
  [cmds]
  (loop [[f s & r] cmds
         acc []]
    (let [hash (crypto/sha2-256 (str f s))
          acc* (conj acc hash)]
      (if r
        (recur r acc*)
        acc*))))

(defn generate-merkle-root
  "hashes should already be in the correct order."
  [& hashes]
  (let [count-cmds   (count hashes)
        repeat-last  (- count-cmds (find-closest-power-2 count-cmds))
        leaves-ordrd (concat hashes (repeat repeat-last (last hashes)))]
    (loop [merkle-results (apply generate-hashes leaves-ordrd)]
      (if (> 1 (count merkle-results))
        (recur (apply generate-hashes merkle-results))
        (first merkle-results)))))
