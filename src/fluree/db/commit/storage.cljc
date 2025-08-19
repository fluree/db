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
    (when-let [commit-json (<? (storage/read-json storage commit-address))]
      (log/trace "read-commit at:" commit-address "data:" commit-json)
      (let [cred?         (contains? commit-json "credentialSubject")
            subject-path  (if cred? ["credentialSubject"] [])
            id-path       (conj subject-path "id")
            addr-path     (conj subject-path "address")
            subject-json  (if cred? (get commit-json "credentialSubject") commit-json)
            ;; Prefer commit id from address/hash; fallback to computing from JSON only if needed
            existing-id   (get subject-json "id")
            ;; Extract base32 hash-like token from the address if present
            addr-str      (str commit-address)
            hash-token    (last (clojure.string/split addr-str #"/"))
            normalized-b  (when (and hash-token (clojure.string/starts-with? hash-token "b")) hash-token)
            id-from-addr  (when normalized-b (str "fluree:commit:sha256:" normalized-b))
            computed-id   (or id-from-addr (commit-data/commit-json->commit-id subject-json))
            subject-json* (cond-> subject-json
                            (or (nil? existing-id)
                                (and (string? existing-id) (clojure.string/blank? existing-id)))
                            (assoc "id" computed-id))
            commit-json*  (if cred?
                            (assoc commit-json "credentialSubject" subject-json*)
                            subject-json*)
            commit-json** (assoc-in commit-json* addr-path commit-address)]
        (-> commit-json**
            json-ld/expand
            verify-commit)))))

;; TODO: Verify hash
(defn read-commit-jsonld
  [storage commit-address commit-hash]
  (go-try
    (when-let [[commit _proof] (<? (read-verified-commit storage commit-address))]
      (let [commit-id (commit-data/hash->commit-id commit-hash)]
        (assoc commit
               const/iri-id commit-id
               const/iri-address commit-address)))))

(defn read-data-jsonld
  [storage address]
  (go-try
    (let [jsonld (<? (storage/read-json storage address))]
      (-> jsonld
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

(defn load-commit-with-metadata
  "Loads commit from disk and merges nameservice metadata (address, index)"
  [storage commit-address index-address]
  (go-try
    (log/debug "commit.storage/load-commit-with-metadata start" {:address commit-address :index-address index-address})
    (when-let [commit-data (<? (storage/read-json storage commit-address))]
      (let [addr-key-path (if (contains? commit-data "credentialSubject")
                            ["credentialSubject" "address"]
                            ["address"])
            index-key-path (if (contains? commit-data "credentialSubject")
                             ["credentialSubject" "index" "address"]
                             ["index" "address"])
            result (-> commit-data
                       (assoc-in addr-key-path commit-address)
                       (cond-> index-address (assoc-in index-key-path index-address)))]
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
               last-t        nil
               commit-tuples (list)] ;; note 'conj' will put at beginning of list (smallest 't' first)
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
