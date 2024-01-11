(ns fluree.db.json-ld.commit
  (:require [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]
            [fluree.db.serde.json :as serde-json]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.util.core :as util :refer [vswap!]]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.util.async :refer [<? go-try]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.indexer.proto :as idx-proto]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.log :as log :include-macros true])
  (:refer-clojure :exclude [vswap!]))

#?(:clj (set! *warn-on-reflection* true))

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

(defn- subject-block-pred
  [db iri-map compact-fn list? p-flakes]
  (go-try
    (loop [[p-flake & r'] p-flakes
           all-refs? nil
           acc'      nil]
      (let [pdt       (flake/dt p-flake)
            ref?      (= const/$xsd:anyURI pdt)
            [obj all-refs?] (if ref?
                              [{"@id" (<? (get-s-iri (flake/o p-flake)
                                                     db iri-map
                                                     compact-fn))}
                               (if (nil? all-refs?) true all-refs?)]
                              [{"@value" (serde-json/serialize-flake-value
                                           (flake/o p-flake)
                                           pdt)} false])
            obj*      (cond-> obj
                        list? (assoc :i (-> p-flake flake/m :i))
                        (contains? serde-json/time-types pdt)
                        ;;need to retain the `@type` for times
                        ;;so they will be coerced correctly when loading
                        (assoc "@type"
                               (<? (get-s-iri pdt
                                              db iri-map
                                              compact-fn))))
            next-acc' (conj acc' obj*)]
        (if (seq r')
          (recur r' all-refs? next-acc')
          [next-acc' all-refs?])))))

(defn- set-refs-type-in-ctx
  [^clojure.lang.Volatile ctx p-iri refs]
  (vswap! ctx assoc-in [p-iri "@type"] "@id")
  (map #(get % "@id") refs))

(defn- handle-list-values
  [objs]
  {"@list" (->> objs (sort-by :i) (map #(dissoc % :i)))})

(defn- subject-block
  [s-flakes db iri-map ^clojure.lang.Volatile ctx compact-fn]
  (go-try
    (loop [[p-flakes & r] (partition-by flake/p s-flakes)
           acc nil]
      (let [fflake          (first p-flakes)
            list?           (-> fflake flake/m :i)
            p-iri           (-> fflake flake/p (get-s-iri db iri-map compact-fn) <?)
            [objs all-refs?] (<? (subject-block-pred db iri-map compact-fn
                                                     list? p-flakes))
            handle-all-refs (partial set-refs-type-in-ctx ctx p-iri)
            objs*           (cond-> objs
                                    ;; next line is for compatibility with json-ld/parse-type's expectations; should maybe revisit
                                    (and all-refs? (not list?)) handle-all-refs
                                    list? handle-list-values
                                    (= 1 (count objs)) first)
            next-acc        (assoc acc p-iri objs*)]
        (if (seq r)
          (recur r next-acc)
          next-acc)))))

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
    (log/trace "generate-commit flakes:" flakes)
    (let [id->iri (volatile! (jld-ledger/predefined-sids-compact compact-fn))
          ctx     (volatile! {})]
      (loop [[s-flakes & r] (partition-by flake/s flakes)
             assert  []
             retract []]
        (if s-flakes
          (let [sid            (flake/s (first s-flakes))
                s-iri          (<? (get-s-iri sid db id->iri compact-fn))
                non-iri-flakes (remove #(= const/$xsd:anyURI (flake/p %)) s-flakes)
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
                  (let [{assert-flakes  true,
                         retract-flakes false} (group-by flake/op non-iri-flakes)
                        s-assert  (when assert-flakes
                                    (-> (<? (subject-block assert-flakes db
                                                           id->iri ctx compact-fn))
                                        (assoc id-key s-iri)))
                        s-retract (when retract-flakes
                                    (-> (<? (subject-block retract-flakes db
                                                           id->iri ctx compact-fn))
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
  [{:keys [ledger branch schema t commit stats] :as _db}
   {:keys [context did private message tag file-data? index-files-ch] :as _opts}]
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
        compact-fn    (json-ld/compact-fn context* ctx-used-atom)
        commit-time   (util/current-time-iso)]
    (log/debug "Committing t" t "at" commit-time)
    {:message        message
     :tag            tag
     :file-data?     file-data? ;; if instead of returning just a db from commit, return also the written files (for consensus)
     :alias          (ledger-proto/-alias ledger)
     :t              (- t)
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
     :index-files-ch index-files-ch ;; optional async chan passed in which will stream out all new index files created (for consensus)
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
  [{:keys [commit] :as db} {:keys [type-key compact ctx-used-atom t v id-key stats] :as commit-opts}]
  (go-try
    (let [prev-dbid   (commit-data/data-id commit)
          {:keys [assert retract refs-ctx]} (<? (commit-opts->data db commit-opts))
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
          dbid        (db-json->db-id (json-ld/normalize-data db-json))
          db-json*    (-> db-json
                          (assoc id-key dbid)
                          (assoc "@context" (merge-with merge @ctx-used-atom refs-ctx*)))]
      (with-meta db-json* {:dbid dbid}))))

(defn do-commit+push
  "Writes commit and pushes, kicks off indexing if necessary."
  [{:keys [ledger commit] :as db} {:keys [branch did private] :as _opts}]
  (go-try
    (let [{:keys [conn state]} ledger
          ledger-commit (:commit (ledger-proto/-status ledger branch))
          new-t?        (or (nil? (commit-data/t ledger-commit))
                            (> (commit-data/t commit) (commit-data/t ledger-commit)))
          new-commit    (commit-data/use-latest-index commit ledger-commit)
          _             (log/debug "do-commit+push new-commit:" new-commit)
          [new-commit* jld-commit] (commit-data/commit-jsonld new-commit)
          signed-commit (if did
                          (<? (cred/generate jld-commit private (:id did)))
                          jld-commit)
          commit-res    (<? (conn-proto/-c-write conn ledger signed-commit)) ;; write commit credential
          new-commit**  (commit-data/update-commit-address new-commit* (:address commit-res))
          db*           (assoc db :commit new-commit**
                                  :new-context? false)
          db**          (if new-t?
                          (<? (commit-data/add-commit-flakes (:prev-commit db) db*))
                          db*)
          db***         (ledger-proto/-commit-update ledger branch (dissoc db** :txns))
          push-res      (<? (nameservice/push! conn (assoc new-commit**
                                                           :meta commit-res
                                                           :ledger-state state)))]
      {:commit-res  commit-res
       :push-res    push-res
       :db          db***})))

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
  "Runs indexer. Will update the latest commit file with new index point
  once completed.

  If optional changes-ch is provided, will stream indexing updates to it
  so it can be replicated via consensus to other servers as needed."
  [{:keys [ledger] :as db} commit-opts changes-ch]
  (let [{:keys [indexer]} ledger
        update-fn (update-commit-fn db commit-opts)]
    ;; call indexing process with update-commit-fn to push out an updated commit once complete
    (idx-proto/-index indexer db {:update-commit update-fn
                                  :changes-ch    changes-ch})))


(defn commit
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  [{:keys [conn indexer] :as ledger} {:keys [t stats commit txns] :as db} opts]
  (go-try
    (let [{:keys [id-key did message tag file-data? index-files-ch] :as opts*} (enrich-commit-opts db opts)
          ledger-update     (<? (ledger-update-jsonld db opts*)) ;; writes :dbid as meta on return object for -c-write to leverage
          dbid              (get ledger-update id-key) ;; sha address of latest "db" point in ledger
          ledger-update-res (<? (conn-proto/-c-write conn ledger ledger-update)) ;; write commit data
          db-address        (:address ledger-update-res) ;; may not have address (e.g. IPFS) until after writing file
          [[txn-id author]] txns
          base-commit-map   {:old-commit commit, :issuer did
                             :message    message, :tag tag, :dbid dbid, :t t
                             :db-address db-address
                             :author     (or author did "")
                             :txn-id     (if (= 1 (count txns)) txn-id "")
                             :flakes     (:flakes stats)
                             :size       (:size stats)}
          new-commit        (commit-data/new-db-commit-map base-commit-map)
          db*               (assoc db
                                   :commit new-commit
                                   :prev-commit commit)
          {db**              :db
           commit-file-meta  :commit-res
           context-file-meta :context-res} (<? (do-commit+push db* opts*))
          ;; if an indexing process is kicked off, returns a channel that contains a stream of updates for consensus
          indexing-ch       (if (idx-proto/-index? indexer db**)
                              (run-index db** opts* index-files-ch)
                              (when index-files-ch (async/close! index-files-ch)))]
      (if file-data?
        {:data-file-meta    ledger-update-res
         :commit-file-meta  commit-file-meta
         :context-file-meta context-file-meta
         :indexing-ch       indexing-ch
         :db                db**}
        db**))))
