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
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.json :as json]))

#?(:clj (set! *warn-on-reflection* true))

(def ledger-context "https://ns.flur.ee/ledger/v1")

(def commit-keys
  {:id          "id"
   :type        "type"
   :db          "db"
   :branch      "branch"
   :alias       "alias"
   :ledger      "ledger"
   :tag         "tag"
   :message     "message"
   :time        "time"
   :prev-commit "prevCommit"
   :type-Commit "Commit"
   })


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
  :flakes - all considered flakes, for any downstream processes that need it"
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
              [assert* retract*]
              (cond
                ;; just an IRI declaration, used internally - nothing to output
                (empty? non-iri-flakes)
                [assert retract]

                ;; we don't output auto-generated rdfs:Class definitions for classes
                ;; (they are implied when used in rdf:type statements)
                (and (= 1 (count non-iri-flakes))
                     (= const/$rdfs:Class (-> non-iri-flakes first flake/o))
                     (= const/$rdf:type (-> non-iri-flakes first flake/p)))
                [assert retract]

                :else
                (let [[s-assert s-retract ctx] (subject-block non-iri-flakes db id->iri ctx compact-fn)]
                  [(if s-assert
                     (conj assert (assoc s-assert id-key s-iri))
                     assert)
                   (if s-retract
                     (conj retract (assoc s-retract id-key s-iri))
                     retract)]))]
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
  [{:keys [ledger branch schema t commit stats] :as _db} {:keys [context did private message tag push?] :as _opts}]
  (let [context*      (-> (if context
                            (json-ld/parse-context (:context schema) context)
                            (:context schema))
                          (json-ld/parse-context {"f" "https://ns.flur.ee/ledger#"})
                          stringify-context)
        private*      (or private
                          (:private did)
                          (:private (ledger-proto/-did ledger)))
        did*          (or (some-> private*
                                  did-from-private)
                          did
                          (ledger-proto/-did ledger))
        ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context* ctx-used-atom)]
    {:alias          (ledger-proto/-alias ledger)
     :push?          (not (false? push?))
     :message        message
     :tag            tag
     :t              (- t)
     :v              1.0
     :prev-commit    (:commit commit)
     :prev-dbid      (:dbid commit)
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
     :stats          stats}))


(defn db-json->db-id
  [payload]
  (->> payload
       crypto/sha2-256
       (str "fluree:db:sha256:")))

(defn commit-json->commit-id
  [payload]
  (->> payload
       crypto/sha2-256
       (str "fluree:commit:sha256:")))


(defn commit-flakes
  "Returns commit flakes from novelty based on 't' value.
  Reverses natural sort order so smallest sids come first."
  [{:keys [novelty t] :as _db}]
  (-> novelty
      :tspo
      (flake/match-tspo t)
      reverse
      not-empty))


(defn commit-meta
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
  [new-db-id opts]
  (let [{:keys [message tag time branch-name prev-commit ledger-address alias]} opts
        commit-base {"@context"            "https://ns.flur.ee/ledger/v1"
                     (:id commit-keys)     "#commit"
                     (:type commit-keys)   [(:type-Commit commit-keys)]
                     (:db commit-keys)     new-db-id
                     (:branch commit-keys) branch-name
                     (:time commit-keys)   time}
        commit      (cond-> commit-base
                            alias (assoc (:alias commit-keys) alias)
                            prev-commit (assoc (:prev-commit commit-keys) prev-commit)
                            ledger-address (assoc (:ledger commit-keys) {(:id commit-keys) ledger-address})
                            message (assoc (:message commit-keys) message)
                            tag (assoc (:tag commit-keys) tag))]
    commit))


(defn commit->graphs
  [commit-data opts]
  (let [{:keys [type-key compact ctx-used-atom t v prev-dbid id-key stats]} opts
        {:keys [assert retract refs-ctx]} commit-data
        prev-db-key (compact const/iri-prevDB)
        assert-key  (compact const/iri-assert)
        retract-key (compact const/iri-retract)
        refs-ctx*   (cond-> refs-ctx
                            prev-dbid (assoc-in [prev-db-key "@type"] "@id")
                            (seq assert) (assoc-in [assert-key "@container"] "@graph")
                            (seq retract) (assoc-in [retract-key "@container"] "@graph"))
        db-json     (cond-> {id-key                nil      ;; comes from hash later
                             type-key              [(compact const/iri-DB)]
                             (compact const/iri-t) t
                             (compact const/iri-v) v}
                            prev-dbid (assoc prev-db-key prev-dbid)
                            (seq assert) (assoc assert-key assert)
                            (seq retract) (assoc retract-key retract)
                            (:flakes stats) (assoc (compact const/iri-flakes) (:flakes stats))
                            (:size stats) (assoc (compact const/iri-size) (:size stats)))
        ;; TODO - this is re-normalized below, can try to do it just once
        db-id       (db-json->db-id (json-ld/normalize-data db-json))
        db-json*    (-> db-json
                        (assoc id-key db-id)
                        (assoc "@context" (merge-with merge @ctx-used-atom refs-ctx*)))]
    db-json*))



(defn commit
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  [{:keys [conn state] :as ledger} db opts]
  (go-try
    (let [{:keys [branch commit t]} db
          {:keys [did id-key push? branch-name] :as opts*} (commit-opts db opts)]
      (let [commit-data (commit-meta db opts*)
            jld-graphs  (commit->graphs commit-data opts*)
            graph-res   (<? (conn-proto/-c-write conn (json-ld/normalize-data jld-graphs)))
            _           (log/info "New DB address:" (:address graph-res))
            jld-commit  (commit->json-ld (:address graph-res) opts*)
            credential  (when did (cred/generate jld-commit opts*))
            doc         (json-ld/normalize-data (or credential jld-commit))
            commit-res  (<? (conn-proto/-c-write conn doc))
            _           (log/info (str "New Commit address: " (:address commit-res)))
            commit-data {:t       t
                         :dbid    (get jld-graphs id-key)   ;; sha address for database
                         :address (:address commit-res)     ;; full address for commit (e.g. fluree:ipfs://...)
                         :meta    (assoc commit-res :db graph-res) ;; additional metadata for ledger method (e.g. ipfs)
                         :branch  branch-name}
            db*         (assoc db :commit commit-data)]     ;; branch published to
        (ledger-proto/-commit-update ledger (keyword branch-name) db*)
        (when push?
          (ledger-proto/-push! ledger (assoc commit-data :ledger-state state)))
        db*))))
