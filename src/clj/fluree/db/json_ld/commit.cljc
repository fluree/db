(ns fluree.db.json-ld.commit
  (:require [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]
            [fluree.db.datatype :as datatype]
            [fluree.db.serde.json :as serde-json]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util :refer [vswap!]]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.util.log :as log :include-macros true])
  (:refer-clojure :exclude [vswap!]))

#?(:clj (set! *warn-on-reflection* true))

(defn get-s-iri
  "Returns an IRI from a subject id (sid)."
  [sid db compact-fn]
  (compact-fn (iri/decode-sid db sid)))

(defn- subject-block-pred
  [db compact-fn list? p-flakes]
  (loop [[p-flake & r] p-flakes
         all-refs? nil
         acc      nil]
    (let [pdt       (flake/dt p-flake)
          ref?      (= const/$xsd:anyURI pdt)
          [obj all-refs?] (if ref?
                            [{"@id" (get-s-iri (flake/o p-flake)
                                               db compact-fn)}
                             (if (nil? all-refs?) true all-refs?)]
                            [{"@value" (-> p-flake
                                           flake/o
                                           (serde-json/serialize-object pdt))}
                             false])
          obj*      (cond-> obj
                      list? (assoc :i (-> p-flake flake/m :i))

                      ;; need to retain the `@type` for times so they will be
                      ;; coerced correctly when loading
                      (datatype/time-type? pdt)
                      (assoc "@type" (get-s-iri pdt db compact-fn)))
          acc' (conj acc obj*)]
      (if (seq r)
        (recur r all-refs? acc')
        [acc' all-refs?]))))

(defn- set-refs-type-in-ctx
  [^clojure.lang.Volatile ctx p-iri refs]
  (vswap! ctx assoc-in [p-iri "@type"] "@id")
  (map #(get % "@id") refs))

(defn- handle-list-values
  [objs]
  {"@list" (->> objs (sort-by :i) (map #(dissoc % :i)))})

(defn- subject-block
  [s-flakes db ^clojure.lang.Volatile ctx compact-fn]
  (loop [[p-flakes & r] (partition-by flake/p s-flakes)
         acc            nil]
    (let [fflake           (first p-flakes)
          list?            (-> fflake flake/m :i)
          p-iri            (-> fflake flake/p (get-s-iri db compact-fn))
          [objs all-refs?] (subject-block-pred db compact-fn list?
                                               p-flakes)
          handle-all-refs  (partial set-refs-type-in-ctx ctx p-iri)
          objs*            (cond-> objs
                             ;; next line is for compatibility with json-ld/parse-type's expectations; should maybe revisit
                             (and all-refs? (not list?)) handle-all-refs
                             list?                       handle-list-values
                             (= 1 (count objs))          first)
          acc'         (assoc acc p-iri objs*)]
      (if (seq r)
        (recur r acc')
        acc'))))

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

(defn parse-commit-context
  [context]
  (let [f-context      {"f" "https://ns.flur.ee/ledger#"}
        parsed-context (if context
                         (-> context
                             json-ld/parse-context
                             (json-ld/parse-context f-context))
                         (json-ld/parse-context f-context))]
    (stringify-context parsed-context)))

(defn- enrich-commit-opts
  "Takes commit opts and merges in with defaults defined for the db."
  [ledger
   {:keys [branch t commit] :as _db}
   {:keys [context did private message tag file-data? index-files-ch] :as _opts}]
  (let [context*      (parse-commit-context context)
        private*      (or private
                          (:private did)
                          (:private (ledger/-did ledger)))
        did*          (or (some-> private*
                                  did-from-private)
                          did
                          (ledger/-did ledger))
        ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context* ctx-used-atom)
        commit-time   (util/current-time-iso)]
    (log/debug "Committing t" t "at" commit-time)
    {:message        message
     :tag            tag
     :file-data?     file-data? ;; if instead of returning just a db from commit, return also the written files (for consensus)
     :t              t
     :v              0
     :prev-commit    (:address commit)
     :prev-dbid      (:dbid commit)
     :ledger-address nil ;; TODO
     :time           commit-time
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
     :index-files-ch index-files-ch})) ;; optional async chan passed in which will stream out all new index files created (for consensus)



(defn commit-flakes
  "Returns commit flakes from novelty based on 't' value."
  [{:keys [novelty t] :as _db}]
  (-> novelty
      :tspo
      (flake/match-tspo t)
      not-empty))

(defn generate-commit
  "Generates assertion and retraction flakes for a given set of flakes
  which is assumed to be for a single (t) transaction.

  Returns a map of
  :assert - assertion flakes
  :retract - retraction flakes
  :refs-ctx - context that must be included with final context, for refs (@id) values
  :flakes - all considered flakes, for any downstream processes that need it"
  [{:keys [reasoner] :as db} {:keys [compact-fn id-key type-key] :as _opts}]
  (when-let [flakes (cond-> (commit-flakes db)
                      reasoner (reasoner/non-reasoned-flakes))]
    (log/trace "generate-commit flakes:" flakes)
    (let [ctx (volatile! {})]
      (loop [[s-flakes & r] (partition-by flake/s flakes)
             assert         []
             retract        []]
        (if s-flakes
          (let [sid   (flake/s (first s-flakes))
                s-iri (get-s-iri sid db compact-fn)
                [assert* retract*]
                (if (and (= 1 (count s-flakes))
                         (= const/$rdfs:Class (->> s-flakes first flake/o))
                         (= const/$rdf:type (->> s-flakes first flake/p)))
                  ;; we don't output auto-generated rdfs:Class definitions for classes
                  ;; (they are implied when used in rdf:type statements)
                  [assert retract]
                  (let [{assert-flakes  true
                         retract-flakes false}
                        (group-by flake/op s-flakes)

                        s-assert  (when assert-flakes
                                    (-> (subject-block assert-flakes db ctx compact-fn)
                                        (assoc id-key s-iri)))
                        s-retract (when retract-flakes
                                    (-> (subject-block retract-flakes db ctx compact-fn)
                                        (assoc id-key s-iri)))]
                    [(cond-> assert
                       s-assert (conj s-assert))
                     (cond-> retract
                       s-retract (conj s-retract))]))]
            (recur r assert* retract*))
          {:refs-ctx (dissoc @ctx type-key) ; @type will be marked as @type: @id, which is implied
           :assert   assert
           :retract  retract
           :flakes   flakes})))))

(defn db->jsonld
  "Creates the JSON-LD map containing a new ledger update"
  [{:keys [commit stats] :as db} {:keys [type-key compact ctx-used-atom t v id-key] :as commit-opts}]
  (let [prev-dbid   (commit-data/data-id commit)
        {:keys [assert retract refs-ctx]} (generate-commit db commit-opts)
        prev-db-key (compact const/iri-previous)
        assert-key  (compact const/iri-assert)
        retract-key (compact const/iri-retract)
        refs-ctx*   (cond-> refs-ctx
                      prev-dbid (assoc-in [prev-db-key "@type"] "@id")
                      (seq assert) (assoc-in [assert-key "@container"] "@graph")
                      (seq retract) (assoc-in [retract-key "@container"] "@graph"))
        db-json     (cond-> {id-key                nil ;; comes from hash later
                             type-key              [(compact const/iri-DB)]
                             (compact const/iri-t) t
                             (compact const/iri-v) v}
                      prev-dbid (assoc prev-db-key prev-dbid)
                      (seq assert) (assoc assert-key assert)
                      (seq retract) (assoc retract-key retract)
                      (:flakes stats) (assoc (compact const/iri-flakes) (:flakes stats))
                      (:size stats) (assoc (compact const/iri-size) (:size stats)))
        ;; TODO - this is re-normalized below, can try to do it just once
        dbid        (commit-data/db-json->db-id db-json)
        db-json*    (-> db-json
                        (assoc id-key dbid)
                        (assoc "@context" (merge-with merge @ctx-used-atom refs-ctx*)))]
    [dbid db-json*]))

(defn new-t?
  [ledger-commit db-commit]
  (let [ledger-t (commit-data/t ledger-commit)]
    (or (nil? ledger-t)
        (flake/t-after? (commit-data/t db-commit)
                        ledger-t))))

(defn do-commit+push
  "Writes commit and pushes, kicks off indexing if necessary."
  [{:keys [alias] :as ledger} {:keys [commit] :as db} {:keys [branch did private] :as _opts}]
  (go-try
    (let [{:keys [conn state]} ledger
          ledger-commit (:commit (ledger/-status ledger branch))
          new-commit    (commit-data/use-latest-index commit ledger-commit)
          _             (log/debug "do-commit+push new-commit:" new-commit)
          [new-commit* jld-commit] (commit-data/commit->jsonld new-commit)
          signed-commit (if did
                          (<? (cred/generate jld-commit private (:id did)))
                          jld-commit)
          commit-res    (<? (connection/-c-write conn alias signed-commit)) ; write commit credential
          new-commit**  (commit-data/update-commit-address new-commit* (:address commit-res))
          db*           (assoc db :commit new-commit**)
          db**          (if (new-t? ledger-commit commit)
                          (commit-data/add-commit-flakes (:prev-commit db) db*)
                          db*)
          db***         (ledger/-commit-update! ledger branch (dissoc db** :txns))
          push-res      (<? (nameservice/push! conn (assoc new-commit**
                                                           :meta commit-res
                                                           :json-ld jld-commit
                                                           :ledger-state state)))]
      {:commit-res  commit-res
       :push-res    push-res
       :db          db***})))

(defn newer-commit?
  [db commit]
  (flake/t-after? (commit-data/t (:commit db))
                  (commit-data/t commit)))

(defn update-commit-fn
  "Returns a fn that receives a newly indexed db as its only argument.
  Will updated the provided committed-db with the new index, then create
  a new commit and push to the name service(s) if configured to do so."
  [ledger committed-db commit-opts]
  (fn [indexed-db]
    (let [indexed-commit (:commit indexed-db)
          new-db         (if (newer-commit? committed-db indexed-commit)
                           (dbproto/-index-update committed-db (:index indexed-commit))
                           indexed-db)]
      (do-commit+push ledger new-db commit-opts))))

(defn run-index
  "Runs indexer. Will update the latest commit file with new index point
  once completed.

  If optional changes-ch is provided, will stream indexing updates to it
  so it can be replicated via consensus to other servers as needed."
  ([ledger db commit-opts]
   (run-index ledger db commit-opts nil))
  ([{:keys [indexer] :as ledger} db commit-opts changes-ch]
   (let [update-fn (update-commit-fn ledger db commit-opts)]
     ;; call indexing process with update-commit-fn to push out an updated commit once complete
     (indexer/-index indexer db {:update-commit update-fn
                                 :changes-ch    changes-ch}))))

(defn write-transactions!
  [conn {:keys [alias] :as _ledger} staged]
  (go-try
    (loop [[[txn author-did annotation] & r] staged
           results                []]
      (if txn
        (let [{txn-id :address} (<? (connection/-txn-write conn alias txn))]
          (recur r (conj results [txn-id author-did annotation])))
        results))))

(defn commit
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  [{:keys [alias conn] :as ledger} {:keys [t stats commit staged] :as db} opts]
  (go-try
    (let [{:keys [did message tag file-data? index-files-ch] :as opts*}
          (enrich-commit-opts ledger db opts)

          txns (<? (write-transactions! conn ledger staged))

          [[txn-id author annotation]] txns

          [dbid db-jsonld]  (db->jsonld db opts*)
          ledger-update-res (<? (connection/-c-write conn alias db-jsonld)) ; write commit data
          db-address        (:address ledger-update-res) ; may not have address (e.g. IPFS) until after writing file
          base-commit-map   {:old-commit commit
                             :issuer     did
                             :message    message
                             :tag        tag
                             :dbid       dbid
                             :t          t
                             :db-address db-address
                             :author     (or author "")
                             :annotation annotation
                             :txn-id     (if (= 1 (count txns)) txn-id "")
                             :flakes     (:flakes stats)
                             :size       (:size stats)}
          new-commit        (commit-data/new-db-commit-map base-commit-map)
          db*               (-> db
                                (update :staged empty)
                                (assoc :commit new-commit
                                       :prev-commit commit))

          {db**             :db
           commit-file-meta :commit-res}
          (<? (do-commit+push ledger db* opts*))]

      (run-index ledger db** opts* index-files-ch)

      (if file-data?
        {:data-file-meta   ledger-update-res
         :commit-file-meta commit-file-meta
         :db               db**}
        db**))))
