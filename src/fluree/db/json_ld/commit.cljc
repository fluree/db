(ns fluree.db.json-ld.commit
  (:require [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.util.core :as util :refer [vswap!]]
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
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.json-ld.vocab :as vocab])
  (:refer-clojure :exclude [vswap!]))

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

(defn get-ref-iris
  "Returns a list of object IRIs from a set of flakes.
  Only to be used for ref? predicates"
  [db iri-map compact-fn flakes]
  (go-try
    (loop [[flake & r] flakes
           acc []]
      (if flake
        (recur r (conj acc (<? (get-s-iri (flake/o flake) db iri-map compact-fn))))
        acc))))

(defn- subject-block
  [s-flakes {:keys [schema] :as db} iri-map ^clojure.lang.Volatile ctx compact-fn]
  (go-try
    (loop [[p-flakes & r] (partition-by flake/p s-flakes)
           acc nil]
      (if p-flakes
        (let [fflake    (first p-flakes)
              p-iri     (<? (get-s-iri (flake/p fflake) db iri-map compact-fn))
              ref?      (get-in schema [:pred (flake/p fflake) :ref?])
              list?     (:i (flake/m fflake))
              p-flakes* (if list?
                          (sort-by #(:i (flake/m %)) p-flakes)
                          p-flakes)
              objs      (if ref?
                          (do
                            (vswap! ctx assoc-in [p-iri "@type"] "@id")
                            (<? (get-ref-iris db iri-map compact-fn p-flakes*)))
                          (mapv flake/o p-flakes*))
              objs*     (cond
                          list?
                          {"@list" objs}

                          (= 1 (count objs))
                          (first objs)

                          :else
                          objs)]
          (recur r (assoc acc p-iri objs*)))
        acc))))

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
                  (let [{assert-flakes  true,
                         retract-flakes false} (group-by flake/op non-iri-flakes)
                        s-assert  (when assert-flakes
                                    (-> (<? (subject-block assert-flakes db id->iri ctx compact-fn))
                                        (assoc id-key s-iri)))
                        s-retract (when retract-flakes
                                    (-> (<? (subject-block retract-flakes db id->iri ctx compact-fn))
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
   {:keys [context did private push?] :as _opts}]
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
    {:alias          (ledger-proto/-alias ledger)
     :push?          (not (false? push?))
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

(defn add-commit-flakes-to-db
  "ecount and sid must be updated prior to calling this."
  [db flakes]
  (let [{:keys [novelty]} db
        {:keys [spot psot post opst tspo]} novelty
        size (flake/size-bytes flakes)]
    (assoc db :novelty {:spot (into spot flakes)
                        :psot (into psot flakes)
                        :post (into post flakes)
                        :opst opst
                        :tspo (into tspo flakes)
                        :size (+ (:size novelty) size)}
              :stats (-> (:stats db)
                         (update :size + size)
                         (update :flakes + (count flakes))))))

(defn add-commit-schema-flakes
  [{:keys [schema] :as db} t]
  (let [schema-flakes [(flake/create const/$_block:hash const/$iri const/iri-time const/$xsd:anyURI t true nil)
                       (flake/create const/$_block:hash const/$rdf:type const/$iri const/$xsd:anyURI t true nil)
                       (flake/create const/$_block:transactions const/$iri const/iri-commit const/$xsd:anyURI t true nil)
                       (flake/create const/$_block:transactions const/$rdf:type const/$iri const/$xsd:anyURI t true nil)
                       ;(flake/create const/$_block:prevHash const/$iri const/iri-previous const/$xsd:anyURI t true nil)
                       ;(flake/create const/$_block:prevHash const/$rdf:type const/$iri  const/$xsd:anyURIt true nil)
                       (flake/create const/$_commit:time const/$iri const/iri-time const/$xsd:anyURI t true nil)
                       (flake/create const/$_block:ledgers const/$iri const/iri-message const/$xsd:anyURI t true nil) ;; reused $_block:ledgers as commit message
                       (flake/create const/$_block:number const/$iri const/iri-tag const/$xsd:anyURI t true nil) ;; reused $_block:number as commit tags
                       (flake/create const/$_block:sigs const/$iri const/iri-issuer const/$xsd:anyURI t true nil)
                       (flake/create const/$_block:sigs const/$rdf:type const/$iri const/$xsd:anyURI t true nil)]
        db*           (add-commit-flakes-to-db db schema-flakes)]
    (assoc db* :schema (vocab/update-with* schema t schema-flakes))))

(defn add-commit-flakes
  [{:keys [commit] :as db}]
  (go-try
    (let [last-sid       (volatile! (jld-ledger/last-commit-sid db))
          next-sid       (fn [] (vswap! last-sid inc))
          {:keys [message tag time id data previous issuer]} commit
          epoch-time     (util/str->epoch-ms time)
          {db-id :id, db-address :address, db-t :t} data
          t              (- db-t)
          db*            (if (= 1 db-t)
                           (add-commit-schema-flakes db t)
                           db)
          db-address-sid (next-sid)
          commit-sid     (next-sid)
          tag-flakes     (when tag
                           (let [tags (if (sequential? tag) tag [tag])]
                             (loop [[tag & r] tags
                                    flakes []]
                               (if tag
                                 (if-let [existing-sid (<? (dbproto/-subid db* tag))]
                                   (recur r (conj flakes (flake/create commit-sid const/$_block:number existing-sid const/$xsd:anyURI t true nil)))
                                   (let [new-sid (next-sid)]
                                     (recur r (conj flakes
                                                    (flake/create new-sid const/$iri tag const/$xsd:string t true nil)
                                                    (flake/create t const/$_block:number new-sid const/$xsd:anyURI t true nil)))))
                                 flakes))))
          issuer-flakes  (when-let [issuer-iri (:id issuer)]
                           (let [issuer-sid   (<? (dbproto/-subid db* issuer-iri))
                                 issuer-flake (when-not issuer-sid
                                                (flake/create (next-sid) const/$iri issuer-iri const/$xsd:string t true nil))]

                             (cond-> [(flake/create commit-sid const/$_block:sigs issuer-sid const/$xsd:anyURI t true nil)]
                                     issuer-flake (conj issuer-flake))))
          flakes         (cond-> [(flake/create t const/$iri db-id const/$xsd:string t true nil)
                                  ;; TODO - add @type: DB into default data flakes?
                                  ;; link db to associated commit meta
                                  (flake/create t const/$_block:transactions commit-sid const/$xsd:anyURI t true nil)
                                  ;; commit flakes below
                                  (flake/create commit-sid const/$_commit:time epoch-time const/$xsd:dateTime t true nil)]
                                 ;; if address for db exists
                                 db-address (into [(flake/create t const/$_block:hash db-address-sid const/$xsd:anyURI t true nil)
                                                   (flake/create db-address-sid const/$iri db-address const/$xsd:string t true nil)])
                                 ;; additional commit meta if applicable
                                 issuer-flakes (into issuer-flakes)
                                 message (conj (flake/create commit-sid const/$_block:ledgers message const/$xsd:string t true nil)) ;; reused $_block:ledgers as commit message
                                 tag-flakes (into tag-flakes))
          db**           (assoc-in db* [:ecount const/$_shard] @last-sid)]
      (add-commit-flakes-to-db db** flakes))))

(defn link-context-to-commit
  "Takes a commit with an embedded :context and pulls it out, saves it to
  storage separately (content-addressed), and puts the address of that data
  back into the commit under the :context key."
  [{:keys [conn] :as ledger} commit]
  (go-try
    (let [context (get commit (keyword const/iri-default-context))
          stringify? (-> context keys first keyword?) ; (too?) simple check if we need to stringify the keys before storing
          context-str (if stringify?
                        (util/stringify-keys context)
                        context)
          {:keys [address]} (<? (conn-proto/-ctx-write conn ledger context-str))]
      (assoc commit (keyword const/iri-default-context) address))))

(defn do-commit+push
  "Writes commit and pushes, kicks off indexing if necessary."
  [{:keys [ledger commit] :as db} {:keys [branch push? did private] :as _opts}]
  (go-try
    (let [{:keys [conn state]} ledger
          ledger-commit (:commit (ledger-proto/-status ledger branch))
          new-t?        (or (nil? (commit-data/t ledger-commit))
                            (> (commit-data/t commit) (commit-data/t ledger-commit)))
          new-commit    (commit-data/use-latest-index commit ledger-commit)
          _             (log/debug "do-commit+push new-commit:" new-commit)
          new-commit*   (<? (link-context-to-commit ledger new-commit))
          _             (log/debug "do-commit+push new-commit w/ linked context:"
                                   new-commit*)
          [new-commit** jld-commit] (commit-data/commit-jsonld new-commit*)
          signed-commit (if did
                          (cred/generate jld-commit private (:id did))
                          jld-commit)
          commit-res    (<? (conn-proto/-c-write conn ledger signed-commit)) ;; write commit credential
          new-commit*** (commit-data/update-commit-address new-commit** (:address commit-res))
          db*           (assoc db :commit new-commit***) ;; branch published to
          db**          (if new-t?
                          (<? (add-commit-flakes db*))
                          db*)
          db***         (ledger-proto/-commit-update ledger branch db**)]
      ;; push is asynchronous!
      (when push?
        (let [address     (ledger-proto/-address ledger)
              commit-data (assoc new-commit*** :meta commit-res
                                               :ledger-state state)]
          (conn-proto/-push conn address commit-data)))
      db***)))

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
  [{:keys [conn indexer context] :as ledger} {:keys [t stats commit] :as db}
   {:keys [message tag] :as opts}]
  (go-try
    (let [{:keys [id-key did] :as opts*} (enrich-commit-opts db opts)]
      (let [ledger-update     (<? (ledger-update-jsonld db opts*)) ;; writes :dbid as meta on return object for -c-write to leverage
            dbid              (get ledger-update id-key) ;; sha address of latest "db" point in ledger
            ledger-update-res (<? (conn-proto/-c-write conn ledger ledger-update)) ;; write commit data
            db-address        (:address ledger-update-res) ;; may not have address (e.g. IPFS) until after writing file
            context-key       (keyword const/iri-default-context)
            base-commit-map   {:old-commit commit, :issuer did
                               :message    message, :tag tag, :dbid dbid, :t t
                               :db-address db-address
                               :flakes     (:flakes stats)
                               :size       (:size stats)
                               context-key context}
            new-commit        (commit-data/new-db-commit-map base-commit-map)
            db*               (assoc db :commit new-commit)
            db**              (<? (do-commit+push db* opts*))]
        (when (idx-proto/-index? indexer db**)
          (run-index db** opts*))
        db**))))
