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
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.util.async :refer [<? go-try channel?]]))

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
  [flakes db {:keys [compact-fn id-key type-key] :as _opts}]
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
         :retract  retract
         :flakes   flakes}))))


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
      (reduce-kv
        (fn [acc k v]
          (let [k* (if (keyword? k)
                     (name k)
                     k)
                v* (if (and (map? v)
                            (not (contains? v :id)))
                     (stringify-context v)
                     v)]
            (assoc acc k* v*)))
        {} context)
      context)))

(defn- commit-opts
  "Takes commit opts and merges in with defaults defined for the db."
  [{:keys [ledger branch schema t commit] :as _db} {:keys [context did private message tag] :as _opts}]
  (let [context*      (-> (if context
                            (json-ld/parse-context (:context schema) context)
                            (:context schema))
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
    {:message        message
     :tag            tag
     :t              (- t)
     :prev-commit    (:id commit)
     :ledger-address nil                                    ;; TODO
     :time           (util/current-time-iso)
     :context        context*
     :private        private*
     :did            did*
     :ctx-used-atom  ctx-used-atom
     :compact-fn     compact-fn
     :compact        (fn [iri] (json-ld/compact iri compact-fn))
     :branch         branch
     :branch-name    (util/keyword->str (branch/name branch))
     :id-key         (json-ld/compact "@id" compact-fn)
     :type-key       (json-ld/compact "@type" compact-fn)
     :hash-key       (json-ld/compact const/iri-hash compact-fn)}))


(defn tx-hash
  [payload]
  (->> payload
       crypto/sha2-256
       (str "urn:sha256:")))


(defn commit-flakes
  "Returns commit flakes from novelty based on 't' value.
  Reverses natural sort order so smallest sids come first."
  [{:keys [novelty t] :as _db}]
  (-> novelty
      :tspo
      (flake/match-tspo t)
      reverse
      not-empty))


(defn tx-data
  "Convert the novelty flakes into the json-ld shape."
  [{:keys [ledger branch t] :as db} opts]
  (let [committed-t (-> ledger
                        (ledger-proto/-status (branch/name branch))
                        branch/latest-commit)
        new-flakes  (commit-flakes db)]
    (when (not= t (dec committed-t))
      (throw (ex-info (str "Cannot commit db, as committed 't' value of: " committed-t
                           " is no longer consistent with staged db 't' value of: " t ".")
                      {:status 400 :error :db/invalid-commit})))
    (when new-flakes
      (generate-commit new-flakes db opts))))


(defn commit->json-ld
  "Create a json-ld commit shape using tx-data."
  [commit-data opts]
  (let [{:keys [type-key hash-key compact message tag ctx-used-atom t
                branch-name time prev-commit ledger-address]} opts
        {:keys [assert retract refs-ctx]} commit-data
        commit-base {type-key                   [(compact const/iri-Commit)]
                     (compact const/iri-t)      t
                     (compact const/iri-branch) branch-name
                     (compact const/iri-time)   time}
        commit      (cond-> commit-base
                            (seq assert) (assoc (compact const/iri-assert) assert)
                            (seq retract) (assoc (compact const/iri-retract) retract)
                            prev-commit (assoc (compact const/iri-prev) prev-commit)
                            ledger-address (assoc (compact const/iri-ledger) ledger-address)
                            message (assoc (compact const/iri-message) message)
                            tag (assoc (compact const/iri-tag) tag)
                            true (assoc "@context" (merge-with merge @ctx-used-atom refs-ctx)))]
    (assoc commit hash-key (tx-hash (json-ld/normalize-data commit)))))


(defn commit
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ;; TODO: error handling - if a commit fails we need to stop immediately
  [{:keys [state conn] :as _ledger} db opts]
  (go-try
    (let [{:keys [branch commit t]} db
          {:keys [did] :as opts*} (commit-opts db opts)]
      (let [{:keys [flakes] :as commit-data} (tx-data db opts*)
            jld-commit  (commit->json-ld commit-data opts*)
            credential  (when did (cred/generate jld-commit opts*))

            doc         (json-ld/normalize-data (or credential commit))
            ;; TODO: can we move these side effects outside of commit?
            ;; TODO: suppose we fail while c-write? while push?
            id          (conn-proto/-c-write conn doc)
            publish-p   (conn-proto/push conn id)
            ;; TODO: should the hash be the tx-hash?
            hash        (get jld-commit const/iri-hash)
            branch-name (branch/name branch)
            db*         (assoc db :commit {:t      t
                                           :hash   hash
                                           :id     id
                                           :branch branch-name
                                           :ledger publish-p})]
        (swap! state update-in [:branches branch-name] branch/update-commit db*)
        db*))))
