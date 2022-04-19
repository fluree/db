(ns fluree.db.util.tx
  (:require [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;; transaction utilities

(defn validate-command
  "Takes a command (map) and validates signature, adds in auth or authority and does
  some additional checks. This can be done before putting the command into the queue for processing.

  Puts original :cmd string and :sig string into this one map for use downstream."
  [{:keys [command id] :as cmd-data}]
  (let [{:keys [sig cmd signed]} command
        cmd-map       (-> (try (json/parse cmd)
                               (catch Exception _
                                 (throw (ex-info (format "Transaction %s is not valid JSON, ignoring." id)
                                                 {:status 400 :error :db/invalid-transaction}))))
                          (assoc :txid id
                                 :cmd cmd
                                 :sig sig
                                 :signed signed)
                          util/without-nils)
        _             (log/trace "Validating command:" cmd-map)
        sig-authority (try (crypto/account-id-from-message (or signed cmd) sig)
                           (catch Exception _
                             (throw (ex-info (format "Transaction %s has an invalid signature." id)
                                             {:status 400 :error :db/invalid-signature}))))
        ;; merge everything together into one map for transaction.
        current-time  (System/currentTimeMillis)
        {:keys [auth authority expire]} cmd-map
        expired?      (and expire (< expire current-time))
        _             (when expired?
                        (throw (ex-info (format "Transaction %s is expired. Current time: %s expire time: %s." id current-time expire)
                                        {:status 400 :error :db/expired-transaction})))
        cmd-map*      (cond
                        (and (nil? auth) (nil? authority))
                        (assoc cmd-map :auth sig-authority)

                        (and (nil? auth) authority)
                        (throw (ex-info (format "Transaction %s invalid. An authority without an auth is not allowed." id)
                                        {:status 400 :error :db/missing-auth}))

                        (and auth authority)
                        (if (= authority sig-authority)
                          cmd-map
                          (throw (ex-info (format "Transaction %s is invalid. Signing authority: %s does not match command authority: %s." id sig-authority authority)
                                          {:status 400 :error :db/invalid-authority})))

                        (and auth (nil? authority))
                        (if (= auth sig-authority)
                          cmd-map
                          (assoc cmd-map :authority sig-authority)))]
    cmd-map*))


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
          (mapv #(let [f %]
                   (vector (flake/s f) (flake/p f) (flake/o f) (flake/t f) (flake/op f) (flake/m f))))
          (json/stringify)
          (crypto/sha3-256)))))


;;;
;;; Block merkle root calculation
;;;

(defn- exp [x n]
  (loop [acc 1 n n]
    (if (zero? n)
      acc
      (recur (long (* x acc)) (dec n))))) ; long keeps recur arg primitive

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


;; TODO - moved this from the original transact namespace. Need to look at how this special treatment is handled
;; and verify it is being done in a reasonable way.
(defn create-new-ledger-tx
  [tx-map]
  (let [{:keys [ledger alias auth doc fork forkBlock]} tx-map
        ledger-name (if (sequential? ledger)
                      (str (first ledger) "/" (second ledger))
                      (str/replace ledger "/$" "/"))
        tx          (util/without-nils
                      {:_id       "db$newdb"
                       :_action   :insert
                       :id        ledger-name
                       :alias     (or alias ledger-name)
                       :root      auth
                       :doc       doc
                       :fork      fork
                       :forkBlock forkBlock})]
    [tx]))


(defn make-candidate-db
  "Assigns a tempid to all index roots, which ensures caching for this candidate db
  is independent from any 'official' db with the same block."
  [db]
  (let [tempid  (util/random-uuid)
        indexes [:spot :psot :post :opst]]
    (reduce
      (fn [db idx]
        (let [index (assoc (get db idx) :tempid tempid)]
          (assoc db idx index)))
      db indexes)))
