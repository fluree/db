(ns fluree.db.commit.storage
  (:require [clojure.core.async :as async :refer [>! chan go]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.storage :as storage]
            [fluree.db.util :as util :refer [get-first get-first-id
                                             get-first-value try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

;; TODO - validate commit signatures
(defn validate-commit-proof
  "Run proof validation, if exists.
  Return actual commit data. In the case of a VerifiableCredential this is
  the `credentialSubject`."
  [_proof]
  ;; TODO - returning true for now
  true)

(defn credential?
  [commit-data]
  (contains? commit-data const/iri-cred-subj))

(defn verify-commit
  "Given a full commit json, returns two-tuple of [commit-data commit-proof]"
  [commit-data]
  (if (credential? commit-data)
    (let [credential-subject (get-first commit-data const/iri-cred-subj)]
      (validate-commit-proof commit-data)
      [credential-subject commit-data])
    [commit-data nil]))

(defn read-verified-commit
  [storage commit-address]
  (go-try
    (log/debug "read-verified-commit: START" {:address commit-address})
    (when-let [commit-data (<? (storage/content-read-json storage commit-address))]
      (log/debug "read-verified-commit: loaded commit data from storage"
                 {:address commit-address
                  :has-id? (contains? commit-data "id")
                  :id-value (get commit-data "id")
                  :has-credentialSubject? (contains? commit-data "credentialSubject")})
      (log/trace "read-commit at:" commit-address "data:" commit-data)
      (let [commit-hash    (<? (storage/get-hash storage commit-address))
            _              (log/debug "read-verified-commit: extracted hash from address"
                                      {:address commit-address
                                       :hash commit-hash
                                       :hash-length (count commit-hash)})
            commit-id      (commit-data/hash->commit-id commit-hash)
            _              (log/debug "read-verified-commit: computed commit-id"
                                      {:address commit-address
                                       :hash commit-hash
                                       :commit-id commit-id})
            is-credential? (contains? commit-data "credentialSubject")
            addr-key-path  (if is-credential?
                             ["credentialSubject" "address"]
                             ["address"])
            id-key-path    (if is-credential?
                             ["credentialSubject" "id"]
                             ["id"])
            ;; Inject the commit-id into the compact JSON before expanding
            commit-data*   (-> commit-data
                               (assoc-in id-key-path commit-id)
                               (assoc-in addr-key-path commit-address))
            _              (log/debug "read-verified-commit: injected commit-id into compact JSON"
                                      {:commit-id commit-id
                                       :id-key-path id-key-path
                                       :addr-key-path addr-key-path
                                       :compact-has-id? (some? (get-in commit-data* id-key-path))
                                       :compact-id-value (get-in commit-data* id-key-path)
                                       :compact-has-address? (some? (get-in commit-data* addr-key-path))})
            [commit proof] (-> commit-data*
                               json-ld/expand
                               verify-commit)
            expanded-id    (-> commit (get "@id") first)
            _              (log/debug "read-verified-commit: after expansion"
                                      {:expanded-id expanded-id
                                       :expanded-id-type (type expanded-id)
                                       :matches-computed? (= expanded-id commit-id)
                                       :commit-keys (keys commit)})
            _              (log/debug "read-verified-commit: COMPLETE - returning expanded commit"
                                      {:has-expanded-id? (some? expanded-id)
                                       :expanded-id expanded-id})]
        [commit proof]))))

;; TODO: Verify hash
(defn read-commit-jsonld
  [storage commit-address]
  (go-try
    (when-let [[commit _proof] (<? (read-verified-commit storage commit-address))]
      commit)))

(defn read-data-jsonld
  [storage address]
  (go-try
    (let [jsonld (<? (storage/read-json storage address))
          hash   (<? (storage/get-hash storage address))
          db-id  (commit-data/hash->db-id hash)]
      (-> jsonld
          (assoc const/iri-id db-id)
          (assoc const/iri-address address)
          json-ld/expand))))

(defn get-commit-t
  [commit]
  (-> commit
      (get-first const/iri-data)
      (get-first-value const/iri-fluree-t)))

(defn validate-commit
  [commit last-t]
  (let [commit-t   (get-commit-t commit)
        dbid       (get-first-id commit const/iri-data)
        db-address (-> commit
                       (get-first const/iri-data)
                       (get-first-value const/iri-address))]
    (when (or (nil? commit-t)
              (and last-t (not= (dec last-t) commit-t)))
      (throw (ex-info (str "Commit t values are inconsistent. Last commit t was: " last-t
                           "and the prevCommit t value is: " commit-t " for commit: " commit)
                      {:status      500
                       :error       :db/invalid-commit
                       :commit-data (if (> (count (str commit)) 500)
                                      (str (subs (str commit) 0 500) "...")
                                      (str commit))})))

    (when-not (and dbid db-address)
      (throw (ex-info (str "Commit is not a properly formatted Fluree commit, missing db id/address: "
                           commit ".")
                      {:status      500
                       :error       :db/invalid-commit
                       :commit-data (if (> (count (str commit)) 500)
                                      (str (subs (str commit) 0 500) "...")
                                      (str commit))})))))

(defn with-index-address
  [commit index-address]
  (if index-address
    (let [index-reference {const/iri-address index-address}]
      (assoc commit const/iri-index [index-reference]))
    commit))

(defn load-commit-with-metadata
  "Loads commit from disk and merges nameservice metadata (address, index)"
  [storage commit-address index-address]
  (go-try
    (log/debug "commit.storage/load-commit-with-metadata start" {:address commit-address :index-address index-address})
    (when-let [verified-commit (<? (read-verified-commit storage commit-address))]
      (let [[commit _proof] verified-commit
            result          (with-index-address commit index-address)]
        (log/debug "commit.storage/load-commit-with-metadata done" {:address commit-address})
        result))))

(defn trace-commits
  "Returns a list of two-tuples each containing [commit proof] as applicable.
  First commit will be t value of `from-t` and increment from there."
  [storage latest-commit from-t error-ch]
  (let [resp-ch (chan)]
    (go
      (try*
        (loop [[commit proof] (verify-commit latest-commit)
               last-t         nil
               commit-tuples  (list)] ;; note 'conj' will put at beginning of list (smallest 't' first)
          (let [prev-commit-addr (-> commit
                                     (get-first const/iri-previous)
                                     (get-first-value const/iri-address))
                commit-t         (get-commit-t commit)
                commit-tuples*   (conj commit-tuples [commit proof])]

            (validate-commit commit last-t)

            (if (= from-t commit-t)
              (async/onto-chan! resp-ch commit-tuples*)
              (when-let [verified-commit (<? (read-verified-commit storage prev-commit-addr))]
                (recur verified-commit commit-t commit-tuples*)))))
        (catch* e
          (log/error e "Error tracing commits")
          (>! error-ch e)
          (async/close! resp-ch))))
    resp-ch))

(defn write-jsonld
  [storage ledger-name jsonld]
  (let [path (str/join "/" [ledger-name "commit"])]
    (storage/content-write-json storage path jsonld)))

(defn write-genesis-commit
  [storage ledger-alias publish-addresses init-time]
  (go-try
    (let [;; Use full alias for commit data, but base name for storage paths
          ledger-base-name          (util.ledger/ledger-base-name ledger-alias)
          genesis-commit            (commit-data/blank-commit ledger-alias publish-addresses init-time)
          initial-context           (get genesis-commit "@context")
          initial-db-data           (-> genesis-commit
                                        (get "data")
                                        (assoc "@context" initial-context))
          {db-address :address}     (<? (write-jsonld storage ledger-base-name initial-db-data))
          genesis-commit*           (assoc-in genesis-commit ["data" "address"] db-address)
          {commit-address :address} (<? (write-jsonld storage ledger-base-name genesis-commit*))]
      (-> genesis-commit*
          (assoc "address" commit-address)
          json-ld/expand))))
