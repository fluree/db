(ns fluree.db.commit.storage
  (:require [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.storage :as storage]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :refer [get-first get-first-id get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

;; TODO - validate commit signatures
(defn validate-commit-proof
  "Run proof validation, if exists.
  Return actual commit data. In the case of a VerifiableCredential this is
  the `credentialSubject`."
  [proof]
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

(defn read-data-jsonld
  [storage address]
  (go-try
    (let [jsonld (<? (storage/read-json storage address)) ]
      (-> jsonld
          (assoc "f:address" address)
          json-ld/expand))))

(defn read-commit-jsonld
  [storage commit-address]
  (go-try
    (let [commit-data   (<? (storage/read-json storage commit-address))
          addr-key-path (if (contains? commit-data "credentialSubject")
                          ["credentialSubject" "address"]
                          ["address"])]
      (log/trace "read-commit at:" commit-address "data:" commit-data)
      (when commit-data
        (-> commit-data
            (assoc-in addr-key-path commit-address)
            json-ld/expand
            verify-commit)))))

(defn trace-commits
  "Returns a list of two-tuples each containing [commit proof] as applicable.
  First commit will be t value of `from-t` and increment from there."
  [storage latest-commit from-t]
  (go-try
    (loop [[commit proof] (verify-commit latest-commit)
           last-t         nil
           commit-tuples  (list)]
      (let [dbid             (get-first-id commit const/iri-data)
            db-address       (-> commit
                                 (get-first const/iri-data)
                                 (get-first-value const/iri-address))
            db-data-jsonld   (<? (read-data-jsonld storage db-address))
            prev-commit-addr (-> commit
                                 (get-first const/iri-previous)
                                 (get-first-value const/iri-address))
            commit-t         (-> commit
                                 (get-first const/iri-data)
                                 (get-first-value const/iri-fluree-t))
            commit-tuples*   (conj commit-tuples [commit proof db-data-jsonld])]
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
                                          (str commit))})))
        (if (= from-t commit-t)
          commit-tuples*
          (let [verified-commit (<? (read-commit-jsonld storage prev-commit-addr))]
            (recur verified-commit commit-t commit-tuples*)))))))

(defn write-jsonld
  [storage ledger-alias jsonld]
  (let [path (str/join "/" [ledger-alias "commit"])]
    (storage/content-write-json storage path jsonld)))

(defn write-genesis-commit
  [storage ledger-alias branch publish-addresses init-time]
  (go-try
    (let [genesis-commit            (commit-data/blank-commit ledger-alias branch publish-addresses init-time)
          initial-context           (get genesis-commit "@context")
          initial-db-data           (-> genesis-commit
                                        (get "data")
                                        (assoc "@context" initial-context))
          {db-address :address}     (<? (write-jsonld storage ledger-alias initial-db-data))
          genesis-commit*           (assoc-in genesis-commit ["data" "address"] db-address)
          {commit-address :address} (<? (write-jsonld storage ledger-alias genesis-commit*))]
      (-> genesis-commit*
          (assoc "address" commit-address)
          json-ld/expand))))
