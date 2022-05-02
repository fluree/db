(ns fluree.db.json-ld.commit
  (:require [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.util.core :as util]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.conn.proto :as conn-proto]
            [clojure.walk :as walk]
            [fluree.db.ledger.proto :as ledger-proto]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const base-context ["https://ns.flur.ee/ledger/v1"])

(defn get-s-iri
  "Returns an IRI from a subject id (sid).

  Caches result in iri-map to speed up processing."
  [sid db iri-map compact-fn]
  (if-let [cached (get @iri-map sid)]
    cached
    ;; TODO following, if a retract was made there could be 2 matching flakes and want to make sure we take the latest add:true
    (let [iri (or (some-> (flake/match-spot (get-in db [:novelty :spot]) sid const/$iri)
                          first
                          :o
                          compact-fn)
                  (str "_:f" sid))]
      (vswap! iri-map assoc sid iri)
      iri)))


(defn- update-subj-prop
  "Helper fn to subject-block"
  [map property val]
  (update map property #(if %
                          (if (sequential? %)
                            (conj % val)
                            [% val])
                          val)))


(defn- subject-block
  [s-flakes {:keys [schema] :as db} iri-map ctx compact-fn]
  (loop [[flake & r] s-flakes
         assert  nil
         retract nil]
    (if flake
      (let [add?     (true? (flake/op flake))
            p-iri    (get-s-iri (flake/p flake) db iri-map compact-fn)
            ref?     (get-in schema [:pred (flake/p flake) :ref?])
            o        (if ref?
                       (do
                         (vswap! ctx assoc-in [p-iri "@type"] "@id")
                         (get-s-iri (flake/o flake) db iri-map compact-fn))
                       (flake/o flake))
            assert*  (if add?
                       (update-subj-prop assert p-iri o)
                       assert)
            retract* (if add?
                       retract
                       (update-subj-prop retract p-iri o))]
        (recur r assert* retract*))
      [assert retract])))


(defn generate-commit
  "Generates assertion and retraction flakes for a given set of flakes
  which is assumed to be for a single (t) transaction.

  Returns a map of
  :assert - assertion flakes
  :retract - retraction flakes
  :refs-ctx - context that must be included with final context, for refs (@id) values
  "
  [db flakes {:keys [compact-fn id-key type-key] :as opts}]
  (let [id->iri (volatile! (jld-ledger/predefined-sids-compact compact-fn))
        ctx     (volatile! {})]
    (loop [[s-flakes & r] (partition-by flake/s flakes)
           assert  []
           retract []]
      (if s-flakes
        (let [sid            (flake/s (first s-flakes))
              s-iri          (get-s-iri sid db id->iri compact-fn)
              non-iri-flakes (remove #(= const/$iri (flake/p %)) s-flakes)
              [s-assert s-retract ctx] (subject-block non-iri-flakes db id->iri ctx compact-fn)
              assert*        (if s-assert
                               (conj assert (assoc s-assert id-key s-iri))
                               assert)
              retract*       (if s-retract
                               (conj retract (assoc s-retract id-key s-iri))
                               retract)]
          (recur r assert* retract*))
        {:refs-ctx (dissoc @ctx type-key)                   ; @type will be marked as @type: @id, which is implied
         :assert   assert
         :retract  retract}))))


(defn- did-from-private
  [private-key]
  (let [acct-id (crypto/account-id-from-private private-key)]
    (str "did:fluree:" acct-id)))


(defn stringify-context
  "Contexts that use clojure keywords will not translate into valid JSON for
  serialization. Here we change any keywords to strings."
  [context]
  (if (sequential? context)
    (mapv stringify-context context)
    (if (map? context)
      (walk/stringify-keys context)
      context)))


(defn- commit-opts
  "Takes commit opts and merges in with defaults defined for the db."
  [db opts]
  (let [{:keys [ledger branch]} db
        {:keys [context did private message]} opts
        context*      (-> (if context
                            (json-ld/parse-context (:context ledger) context)
                            (:context ledger))
                          stringify-context)
        private*      (or private
                          (:private did)
                          (:private (ledger-proto/-did ledger)))
        did*          (or (some-> private*
                                  did-from-private)
                          did
                          (:did ledger))
        ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context* ctx-used-atom)]
    {:message       message
     :context       context*
     :private       private*
     :did           did*
     :ctx-used-atom ctx-used-atom
     :compact-fn    compact-fn
     :compact       (fn [iri] (json-ld/compact iri compact-fn))
     :branch        branch
     :id-key        (json-ld/compact "@id" compact-fn)
     :type-key      (json-ld/compact "@type" compact-fn)}))


#_(defn- add-commit-hash
    "Adds hash key to commit document"
    [doc hash-key]
    (let [normalized (normalize/normalize doc)
          hash       (->> (->> normalized
                               crypto/sha2-256
                               (str "urn:sha256:")))]
      {:normalized normalized
       :hash       hash
       :commit     (assoc doc hash-key hash)}))

#_(defn- tx-hash
    [txs]
    (let [normalized (normalize/normalize txs)
          hash       (->> normalized
                          crypto/sha2-256
                          (str "urn:sha256:"))]
      {:normalized normalized
       :hash       hash}))

#_(defn- tx-doc
    "Generates a transaction JSON-LD doc for a given 't' value.
    Does not include a context, a global context for all transactions
    and commit metadata will be included at the top level of the commit.

    Returns two-tuple of [tx-doc refs-ctx] where tx-doc is the json-ld
    document (sans context) of the transaction and refs-ctx is context
    that must be included in the final context which specifies which
    properties/predicates are @id (ref) values"
    [{:keys [novelty] :as db} t compact-fn ctx-used-atom]
    (let [flakes   (->> (:spot novelty)
                        (filter #(= t (flake/t %)))
                        reverse)
          id-key   (json-ld/compact "@id" compact-fn)
          type-key (json-ld/compact "@type" compact-fn)
          {:keys [assert retract refs-ctx]}
          (generate-commit db flakes {:compact-fn    compact-fn
                                      :id-key        id-key
                                      :type-key      type-key
                                      :ctx-used-atom ctx-used-atom})
          tx-doc   (cond-> {(compact-fn const/iri-t) (- t)}
                           (seq assert) (assoc (compact-fn const/iri-assert) assert)
                           (seq retract) (assoc (compact-fn const/iri-retract) retract))]
      (when ctx-used-atom
        (swap! ctx-used-atom (partial merge-with merge) refs-ctx))
      tx-doc))

#_(defn commit-doc
    [{:keys [ledger t] :as db} {:keys [time message context]}]
    (let [{branch-name   :name
           branch-t      :t
           branch-commit :commit} (branch/branch-meta ledger)
          ctx-used-atom (atom {})
          context*      (if context
                          (json-ld/parse-context (:context ledger) context)
                          (:context ledger))
          compact-fn    (json-ld/compact-fn context* ctx-used-atom)
          t-range       (reverse (range t branch-t))
          tx-docs       (mapv #(tx-doc db % compact-fn ctx-used-atom) t-range)
          id-key        (json-ld/compact "@id" compact-fn)
          type-key      (json-ld/compact "@type" compact-fn)
          final-ctx     (conj base-context @ctx-used-atom)]
      (cond-> {"@context"                                         final-ctx
               type-key                                           [(compact-fn "https://flur.ee/ns/block/Commit")]
               (compact-fn "https://flur.ee/ns/block/branchName") (util/keyword->str branch-name)
               (compact-fn "https://flur.ee/ns/block/t")          (- t)
               (compact-fn "https://flur.ee/ns/block/time")       (util/current-time-iso)
               (compact-fn "https://flur.ee/ns/block/tx")         tx-docs}
              ;branch-commit (assoc (compact-fn const/iri-prev) branch-commit)
              ;ledger-address (assoc (compact-fn "https://flur.ee/ns/block/ledger") ledger-address)
              message (assoc (compact-fn "https://flur.ee/ns/block/message") message))

      )

    )


#_(defn db
    "Commits a current DB's changes (since last commit) to the storage backend
    defined by the DB.

    Returns a modified DB with the last commit content-addressable storage location updates"
    [db opts]
    (let [{:keys [t novelty commit]} db
          _              (log/warn "Commit opts: " (commit-opts db opts))
          {:keys [branch message type-key compact ctx-used-atom private return queue? push publish] :as opts*} (commit-opts db opts)
          ;; TODO - tsop index can get below flakes more efficiently once exists
          flakes         (filter #(= t (flake/t %)) (:spot novelty))
          {:keys [assert retract ctx]} (generate-commit db (reverse flakes) opts*)
          final-ctx      (conj base-context (merge-with merge @ctx-used-atom ctx))
          prev-commit    (:id commit)
          branch-commit  (:branch commit)
          ledger-address (when (and (:ledger commit) (realized? (:ledger commit)))
                           @(:ledger commit))
          doc            (cond-> {"@context"                                      final-ctx
                                  type-key                                        [(compact "https://flur.ee/ns/block/Commit")]
                                  (compact "https://flur.ee/ns/block/branchName") branch
                                  (compact "https://flur.ee/ns/block/t")          (- t)
                                  (compact "https://flur.ee/ns/block/time")       (util/current-time-iso)}
                                 prev-commit (assoc (compact "https://flur.ee/ns/block/prev") prev-commit)
                                 branch-commit (assoc (compact "https://flur.ee/ns/block/branch") branch-commit)
                                 ledger-address (assoc (compact "https://flur.ee/ns/block/ledger") ledger-address)
                                 message (assoc (compact "https://flur.ee/ns/block/message") message)
                                 (seq assert) (assoc (compact "https://flur.ee/ns/block/assert") assert)
                                 (seq retract) (assoc (compact "https://flur.ee/ns/block/retract") retract))
          hash-key       (compact "https://flur.ee/ns/block/hash")
          {:keys [commit hash] :as commit-res} (add-commit-hash doc hash-key)
          {:keys [credential] :as cred-res} (when private
                                              (cred/generate commit opts*))
          commit-json    (if credential
                           (cred/credential-json cred-res)
                           (commit-json commit-res hash-key))
          ;; TODO - queue? is not yet implemented. Cannot form final commit until you have the previous object from publish so will need to modify commits
          id             (when-not queue?
                           (push commit-json))
          publish-p      (when (and (not queue?) publish)
                           (publish id))
          db*            (assoc db :t t
                                   :commit {:t      t
                                            :hash   hash
                                            :queue  (if queue? ;; queue is for offline changes until ready to publish
                                                      (conj (or (:queue commit) []) commit)
                                                      (:queue commit))
                                            :id     id
                                            :branch (or (:branch commit) id)
                                            :ledger publish-p})
          res            {:credential credential
                          :commit     commit
                          :json       commit-json
                          :id         id
                          :publish    publish-p             ;; promise with eventual result once successful
                          :hash       hash
                          :db-before  db
                          :db-after   db*}]
      (if return
        (get res return)
        res)))


(defn tx-hash
  [payload]
  (->> payload
       crypto/sha2-256
       (str "urn:sha256:")))


(defn tx-data
  "Convert the novelty flakes into the json-ld shape."
  [{:keys [novelty ledger] :as db} {:keys [compact] :as opts}]
  (let [{:keys [committed-t]} (ledger-proto/-commit ledger)
        new-flakes (->> (:tspo novelty)
                        (filter #(< (flake/t %) committed-t))
                        (not-empty))]
    (when new-flakes
      (->> new-flakes
           (group-by flake/t)
           (map (fn [[t flakes]] (-> (generate-commit db (reverse flakes) opts)
                                     (assoc :t t))))
           (map (fn [{:keys [assert retract t refs-ctx]}]
                  (cond-> {(compact const/iri-t) (- t)}
                          (seq refs-ctx) (assoc "@context" refs-ctx)
                          (seq assert) (assoc (compact const/iri-assert) assert)
                          (seq retract) (assoc (compact const/iri-retract) retract))))))))


(defn commit-data
  "Create a json-ld commit shape using tx-data."
  [updates {:keys [commit] :as db} {:keys [type-key compact branch message tag] :as opts}]
  (let [prev-commit    (:id commit)
        ledger-address (when (and (:ledger commit) (realized? (:ledger commit)))
                         @(:ledger commit))
        hash           (tx-hash (json-ld/normalize-data updates))
        commit-base    {"@context"                  base-context
                        type-key                    [(compact const/iri-Commit)]
                        (compact const/iri-branch)  (util/keyword->str branch)
                        (compact const/iri-time)    (util/current-time-iso)
                        (compact const/iri-updates) updates
                        (compact const/iri-hash)    hash}]
    (cond-> commit-base
            prev-commit (assoc (compact const/iri-prev) prev-commit)
            ledger-address (assoc (compact const/iri-ledger) ledger-address)
            message (assoc (compact const/iri-message) message)
            tag (assoc (compact const/iri-tag) tag))))


(defn commit
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ;; TODO: error handling - if a commit fails we need to stop immediately
  [db opts]
  (let [{:keys [branch commit ledger t]} db
        {:keys [did] :as opts*} (commit-opts db opts)
        jld-txs (tx-data db opts*)]
    (if jld-txs
      (let [jld-commit (commit-data jld-txs db opts*)
            credential (when did (cred/generate jld-commit opts*))

            doc        (json-ld/normalize-data (or credential commit))
            ;; TODO: can we move these side effects outside of commit?
            ;; TODO: suppose we fail while c-write? while push?
            conn       (:conn ledger)
            id         (conn-proto/c-write conn doc)
            publish-p  (conn-proto/push conn id)
            ;; TODO: should the hash be the tx-hash?
            hash       (get jld-commit const/iri-hash)]
        ;; TODO: properly update branch state
        (swap! (:state ledger) #(update-in % [:branches branch] merge {:t t :commit hash}))
        (assoc db :commit {:t      t
                           :hash   hash
                           :id     id
                           :branch branch
                           :ledger publish-p}))
      ;; No changes to commit
      db)))
