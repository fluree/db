(ns fluree.db.json-ld.commit
  (:require [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.util.core :as util]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.util.async :refer [<? go-try channel?]]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <! put!] :as async])
            [fluree.db.indexer.proto :as idx-proto]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(def ledger-context "https://ns.flur.ee/ledger/v1")

(defn get-s-iri
  "Returns an IRI from a subject id (sid).

  Caches result in iri-map to speed up processing."
  [sid db iri-map compact-fn]
  ;; TODO - if we can move cache check into calling fns, we can avoid an extra async channel here
  (go-try
    (if-let [cached (get @iri-map sid)]
      cached
      ;; TODO following, if a retract was made there could be 2 matching flakes and want to make sure we take the latest add:true
      (let [iri (or (<? (dbproto/-iri db sid compact-fn))
                    (str "_:f" sid))]
        (vswap! iri-map assoc sid iri)
        iri))))


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
  (go-try
    (loop [[flake & r] s-flakes
           assert  nil
           retract nil]
      (if flake
        (let [add?     (true? (flake/op flake))
              p-iri    (<? (get-s-iri (flake/p flake) db iri-map compact-fn))
              ref?     (get-in schema [:pred (flake/p flake) :ref?])
              o        (if ref?
                         (do
                           (vswap! ctx assoc-in [p-iri "@type"] "@id")
                           (<? (get-s-iri (flake/o flake) db iri-map compact-fn)))
                         (flake/o flake))
              assert*  (if add?
                         (update-subj-prop assert p-iri o)
                         assert)
              retract* (if add?
                         retract
                         (update-subj-prop retract p-iri o))]
          (recur r assert* retract*))
        [assert retract]))))


(defn generate-commit
  "Generates assertion and retraction flakes for a given set of flakes
  which is assumed to be for a single (t) transaction.

  Returns a map of
  :assert - assertion flakes
  :retract - retraction flakes
  :refs-ctx - context that must be included with final context, for refs (@id) values
  :flakes - all considered flakes, for any downstream processes that need it"
  [flakes db {:keys [compact-fn id-key type-key] :as _opts}]
  (go-try
    (let [id->iri (volatile! (jld-ledger/predefined-sids-compact compact-fn))
          ctx     (volatile! {})]
      (loop [[s-flakes & r] (partition-by flake/s flakes)
             assert  []
             retract []]
        (if s-flakes
          (let [sid            (flake/s (first s-flakes))
                s-iri          (<? (get-s-iri sid db id->iri compact-fn))
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
                  (let [[s-assert s-retract ctx] (<? (subject-block non-iri-flakes db id->iri ctx compact-fn))]
                    [(if s-assert
                       (conj assert (assoc s-assert id-key s-iri))
                       assert)
                     (if s-retract
                       (conj retract (assoc s-retract id-key s-iri))
                       retract)]))]
            (recur r assert* retract*))
          {:refs-ctx (dissoc @ctx type-key)                 ; @type will be marked as @type: @id, which is implied
           :assert   assert
           :retract  retract
           :flakes   flakes})))))


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

(defn- enrich-commit-opts
  "Takes commit opts and merges in with defaults defined for the db."
  [{:keys [ledger branch schema t commit stats] :as db}
   {:keys [context did private message tag push?] :as _opts}]
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
     :t              (- t)
     :v              1.0
     :prev-commit    (:address commit)
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
     :branch-name    (util/keyword->str branch)
     :id-key         (json-ld/compact "@id" compact-fn)
     :type-key       (json-ld/compact "@type" compact-fn)
     :stats          stats}))


(defn db-json->db-id
  [payload]
  (->> (crypto/sha2-256 payload :base32)
       (str "fluree:db:sha256:b")))

(defn commit-flakes
  "Returns commit flakes from novelty based on 't' value.
  Reverses natural sort order so smallest sids come first."
  [{:keys [novelty t] :as _db}]
  (-> novelty
      :tspo
      (flake/match-tspo t)
      reverse
      not-empty))

(defn commit-opts->data
  "Convert the novelty flakes into the json-ld shape."
  [{:keys [ledger branch t] :as db} opts]
  (go-try
    (let [committed-t (-> ledger
                          (ledger-proto/-status branch)
                          (branch/latest-commit-t)
                          -)
          new-flakes  (commit-flakes db)]
      (when (not= t (dec committed-t))
        (throw (ex-info (str "Cannot commit db, as committed 't' value of: " committed-t
                             " is no longer consistent with staged db 't' value of: " t ".")
                        {:status 400 :error :db/invalid-commit})))
      (when new-flakes
        (<? (generate-commit new-flakes db opts))))))

(defn ledger-update-jsonld
  "Creates the JSON-LD map containing a new ledger update"
  [db {:keys [type-key compact ctx-used-atom t v prev-dbid id-key stats] :as commit-opts}]
  (go-try
    (let [{:keys [assert retract refs-ctx]} (<? (commit-opts->data db commit-opts))
          prev-db-key (compact const/iri-prevDB)
          assert-key  (compact const/iri-assert)
          retract-key (compact const/iri-retract)
          refs-ctx*   (cond-> refs-ctx
                              prev-dbid (assoc-in [prev-db-key "@type"] "@id")
                              (seq assert) (assoc-in [assert-key "@container"] "@graph")
                              (seq retract) (assoc-in [retract-key "@container"] "@graph"))
          db-json     (cond-> {id-key                nil    ;; comes from hash later
                               type-key              [(compact const/iri-DB)]
                               (compact const/iri-t) t
                               (compact const/iri-v) v}
                              prev-dbid (assoc prev-db-key prev-dbid)
                              (seq assert) (assoc assert-key assert)
                              (seq retract) (assoc retract-key retract)
                              (:flakes stats) (assoc (compact const/iri-flakes) (:flakes stats))
                              (:size stats) (assoc (compact const/iri-size) (:size stats)))
          ;; TODO - this is re-normalized below, can try to do it just once
          dbid        (db-json->db-id (json-ld/normalize-data db-json))
          db-json*    (-> db-json
                          (assoc id-key dbid)
                          (assoc "@context" (merge-with merge @ctx-used-atom refs-ctx*)))]
      (with-meta db-json* {:dbid dbid}))))

(defn do-commit+push
  "Writes commit and pushes, kicks off indexing if necessary."
  [{:keys [ledger commit] :as db} {:keys [branch push? did private] :as _opts}]
  (go-try
    (let [{:keys [conn state]} ledger
          ledger-commit (:commit (ledger-proto/-status ledger branch))
          new-commit    (commit-data/use-latest-index commit ledger-commit)
          [new-commit* jld-commit] (commit-data/commit-jsonld new-commit)
          signed-commit (if did
                          (cred/generate jld-commit private (:id did))
                          jld-commit)
          commit-res    (<? (conn-proto/-c-write conn signed-commit))
          new-commit**  (commit-data/update-commit-address new-commit* (:address commit-res))
          db*           (assoc db :commit new-commit**)     ;; branch published to
          db**          (ledger-proto/-commit-update ledger branch db*)]
      ;; push is asynchronous!
      (when push?
        (let [address      (ledger-proto/-address ledger)
              commit-data* (assoc new-commit** :meta commit-res
                                               :ledger-state state)]
          (conn-proto/-push conn address commit-data*)))
      db**)))

(defn update-commit-fn
  "Returns a fn that receives a newly indexed db as its only argument.
  Will updated the provided committed-db with the new index, then create
  a new commit and push to the name service(s) if configured to do so."
  [committed-db commit-opts]
  (fn [indexed-db]
    (let [indexed-commit (:commit indexed-db)
          commit-newer?  (> (commit-data/t (:commit committed-db))
                            (commit-data/t indexed-commit))
          new-db         (if commit-newer?
                           (dbproto/-index-update committed-db (:index indexed-commit))
                           indexed-db)]
      (do-commit+push new-db commit-opts))))

(defn run-index
  [{:keys [ledger] :as db} commit-opts]
  (let [{:keys [indexer]} ledger
        update-fn (update-commit-fn db commit-opts)]
    ;; call indexing process with update-commit-fn to push out an updated commit once complete
    (idx-proto/-index indexer db {:update-commit update-fn})))


(defn commit
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  [{:keys [conn indexer] :as ledger} {:keys [t stats] :as db} {:keys [message tag] :as opts}]
  (go-try
    (let [{:keys [id-key] :as opts*} (enrich-commit-opts db opts)]
      (let [ledger-update     (<? (ledger-update-jsonld db opts*)) ;; writes :dbid as meta on return object for -c-write to leverage
            dbid              (get ledger-update id-key)    ;; sha address of latest "db" point in ledger
            ledger-update-res (<? (conn-proto/-c-write conn ledger-update))
            db-address        (:address ledger-update-res)  ;; may not have address (e.g. IPFS) until after writing file
            new-commit        (commit-data/new-db-commit-map (:commit db) message tag dbid t db-address (:flakes stats) (:size stats))
            db*               (assoc db :commit new-commit)
            db**              (<? (do-commit+push db* opts*))]
        (when (idx-proto/-index? indexer db**)
          (run-index db** opts*))
        db**))))
