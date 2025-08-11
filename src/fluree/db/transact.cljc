(ns fluree.db.transact
  (:require [clojure.string :as str]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.did :as did]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.json-ld.credential :as credential]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.track :as track]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.context :as context]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(def f-context {"f" "https://ns.flur.ee/ledger#"})

(defn nested-nodes?
  "Returns truthy if the provided node has any nested nodes."
  [node]
  (->> node
       (into []
             (comp (remove (fn [[k _]] (keyword? k)))  ; remove :id :idx :type
                   (mapcat rest)                      ; discard keys
                   (mapcat (partial remove
                                    (fn [v]
                                      ;; remove value objects unless they have type @id
                                      (and
                                       (some? (:value v))
                                       (not= (:type v) const/iri-id)))))))
       not-empty))

(defn expand-annotation
  [_parsed-txn parsed-opts context]
  (some-> (:annotation parsed-opts)
          (json-ld/expand context)
          util/sequential))

(defn validate-annotation
  [[annotation :as expanded]]
  (when-let [specified-id (:id annotation)]
    (throw (ex-info "Commit annotation cannot specify a subject identifier."
                    {:status 400, :error :db/invalid-annotation :id specified-id})))
  (when (> (count expanded) 1)
    (throw (ex-info "Commit annotation must only have a single subject."
                    {:status 400, :error :db/invalid-annotation})))
  (when (nested-nodes? annotation)
    (throw (ex-info "Commit annotation cannot reference other subjects."
                    {:status 400, :error :db/invalid-annotation})))
  expanded)

(defn extract-annotation
  [context parsed-txn parsed-opts]
  (-> parsed-txn
      (expand-annotation parsed-opts context)
      validate-annotation))

(defn stage
  ([db identity txn parsed-opts]
   (stage db nil identity txn parsed-opts))
  ([db tracker identity parsed-txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn author]} parsed-opts

           annotation (extract-annotation context parsed-txn parsed-opts)]
       (<? (flake.transact/-stage-txn db tracker context identity author annotation raw-txn parsed-txn))))))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db parsed-txn]
  (go-try
    (let [parsed-opts    (:opts parsed-txn)
          parsed-context (:context parsed-opts)
          identity       (:identity parsed-opts)]
      (if (track/track-txn? parsed-opts)
        (let [tracker   (track/init parsed-opts)
              policy-db (if (policy/policy-enforced-opts? parsed-opts)
                          (<? (policy/policy-enforce-db db tracker parsed-context parsed-opts))
                          db)]
          (track/register-policies! tracker policy-db)
          (try*
            (let [staged-db     (<? (stage policy-db tracker identity parsed-txn parsed-opts))
                  tally         (track/tally tracker)]
              (assoc tally :status 200, :db staged-db))
            (catch* e
              (throw (ex-info (ex-message e)
                              (let [tally (track/tally tracker)]
                                (merge (ex-data e) tally))
                              e)))))
        (let [policy-db (if (policy/policy-enforced-opts? parsed-opts)
                          (<? (policy/policy-enforce-db db parsed-context parsed-opts))
                          db)]
          (<? (stage policy-db identity parsed-txn parsed-opts)))))))

(defn sanitize-commit-options
  "Parses the commit options and removes non-public opts."
  [opts]
  (if (string? opts)
    {:message opts}
    (select-keys opts [:context :did :private :message :tag :index-files-ch])))

(defn parse-commit-context
  [context]
  (let [parsed-context (if context
                         (-> context
                             json-ld/parse-context
                             (json-ld/parse-context f-context))
                         (json-ld/parse-context f-context))]
    (context/stringify parsed-context)))

(defn parse-keypair
  [ledger {:keys [did private] :as opts}]
  (let [private* (or private
                     (:private did)
                     (-> ledger :did :private))
        did*     (or (some-> private* did/private->did)
                     did
                     (-> ledger :did :id))]
    (assoc opts :did did*, :private private*)))

(defn parse-data-helpers
  [{:keys [context] :as opts}]
  (let [ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context ctx-used-atom)]
    (assoc opts
           :commit-data-opts {:compact-fn    compact-fn
                              :compact       (fn [iri] (json-ld/compact iri compact-fn))
                              :id-key        (json-ld/compact "@id" compact-fn)
                              :type-key      (json-ld/compact "@type" compact-fn)
                              :ctx-used-atom ctx-used-atom})))

(defn parse-commit-opts
  [ledger opts]
  (-> opts
      (update :context parse-commit-context)
      (->> (parse-keypair ledger))
      parse-data-helpers))

(defn save-txn!
  ([{:keys [commit-catalog alias] :as _ledger} txn]
   (let [ledger-name (first (str/split alias #"@" 2))]
     (save-txn! commit-catalog ledger-name txn)))
  ([commit-catalog ledger-name txn]
   (let [path (str/join "/" [ledger-name "txn"])]
     (storage/content-write-json commit-catalog path txn))))

;; TODO - as implemented the db handles 'staged' data as per below (annotation, raw txn)
;; TODO - however this is really a concern of "commit", not staging and I don't think the db should be handling any of it
(defn write-transaction!
  [ledger db-alias staged]
  (go-try
    (let [{:keys [txn author annotation]} staged
          {:keys [commit-catalog]} ledger]
      (if txn
        (let [{txn-id :address} (<? (save-txn! commit-catalog db-alias txn))]
          {:txn-id     txn-id
           :author     author
           :annotation annotation})
        staged))))

(defn update-commit-address
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-address]
  [(assoc commit-map :address commit-address)
   (assoc commit-jsonld "address" commit-address)])

(defn update-commit-id
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-hash]
  (let [commit-id (commit-data/hash->commit-id commit-hash)]
    [(assoc commit-map :id commit-id)
     (assoc commit-jsonld "id" commit-id)]))

(defn write-commit
  [commit-storage alias {:keys [did private]} commit]
  (go-try
    (let [commit-jsonld (commit-data/->json-ld commit)
          ;; For credential/generate, we need a DID map with public key
          did-map (when (and did private)
                    (if (map? did)
                      did
                      (did/private->did-map private)))
          signed-commit (if did-map
                          (<? (credential/generate commit-jsonld private did-map))
                          commit-jsonld)
          commit-res    (<? (commit-storage/write-jsonld commit-storage alias signed-commit))

          [commit* commit-jsonld*]
          (-> [commit commit-jsonld]
              (update-commit-id (:hash commit-res))
              (update-commit-address (:address commit-res)))]
      {:commit-map    commit*
       :commit-jsonld commit-jsonld*
       :write-result  commit-res})))

(defn publish-commit
  "Publishes commit to all nameservices registered with the ledger."
  [{:keys [primary-publisher secondary-publishers] :as _ledger} commit-jsonld]
  (go-try
    (let [result (<? (nameservice/publish primary-publisher commit-jsonld))]
      (nameservice/publish-to-all commit-jsonld secondary-publishers)
      result)))

(defn formalize-commit
  [{prev-commit :commit :as staged-db} new-commit]
  (let [max-ns-code (-> staged-db :namespace-codes iri/get-max-namespace-code)]
    (-> staged-db
        (assoc :commit new-commit
               :staged nil
               :prev-commit prev-commit
               :max-namespace-code max-ns-code)
        (commit-data/add-commit-flakes))))

(defn commit!
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ([ledger db]
   (commit! ledger db {}))
  ([{ledger-alias :alias :as ledger}
    {:keys [alias branch t stats commit] :as staged-db}
    opts]
   (go-try
     (let [{:keys [commit-catalog]} ledger
           ledger-name (first (str/split ledger-alias #"@" 2))

           {:keys [tag time message did private commit-data-opts index-files-ch]
            :or   {time (util/current-time-iso)}}
           (parse-commit-opts ledger opts)

           {:keys [db-jsonld staged-txn]}
           (commit-data/db->jsonld staged-db commit-data-opts)

           {:keys [txn-id author annotation]}
           (<? (write-transaction! ledger ledger-name staged-txn))

           data-write-result (<? (commit-storage/write-jsonld commit-catalog ledger-name db-jsonld))
           db-address        (:address data-write-result) ; may not have address (e.g. IPFS) until after writing file
           dbid              (commit-data/hash->db-id (:hash data-write-result))
           keypair           {:did did, :private private}

           new-commit (commit-data/new-db-commit-map {:old-commit commit
                                                      :issuer     did
                                                      :message    message
                                                      :tag        tag
                                                      :dbid       dbid
                                                      :t          t
                                                      :time       time
                                                      :db-address db-address
                                                      :author     author
                                                      :annotation annotation
                                                      :txn-id     txn-id
                                                      :flakes     (:flakes stats)
                                                      :size       (:size stats)})

           {:keys [commit-map commit-jsonld write-result]}
           (<? (write-commit commit-catalog ledger-name keypair new-commit))

           db  (formalize-commit staged-db commit-map)
           db* (ledger/update-commit! ledger branch db index-files-ch)]

       (log/debug "Committing t" t "at" time)

       (<? (publish-commit ledger commit-jsonld))

       (if (track/track-txn? opts)
         (let [indexing-disabled? (-> ledger
                                      (ledger/get-branch-meta branch)
                                      :indexing-opts
                                      :indexing-disabled)
               index-t (commit-data/index-t commit-map)
               novelty-size (get-in db* [:novelty :size] 0)
               reindex-min-bytes (:reindex-min-bytes db*)
               indexing-needed? (>= novelty-size reindex-min-bytes)]
           (-> write-result
               (select-keys [:address :hash :size])
               (assoc :ledger-id ledger-alias
                      :t t
                      :db db*
                      :indexing-needed indexing-needed?
                      :index-t index-t
                      :indexing-disabled indexing-disabled?
                      :novelty-size novelty-size)))
         db*)))))

(defn transact-ledger!
  [ledger parsed-txn]
  (go-try
    (let [{:keys [branch] :as parsed-opts,
           :or   {branch commit-data/default-branch}}
          (:opts parsed-txn)

          db       (ledger/current-db ledger branch)
          staged   (<? (stage-triples db parsed-txn))
          ;; commit API takes a did-map and parsed context as opts
          ;; whereas stage API takes a did IRI and unparsed context.
          ;; Dissoc them until deciding at a later point if they can carry through.
          cmt-opts (dissoc parsed-opts :context :identity)]
      (if (track/track-txn? parsed-opts)
        (let [staged-db     (:db staged)
              commit-result (<? (commit! ledger staged-db cmt-opts))]
          (merge staged commit-result))
        (<? (commit! ledger staged cmt-opts))))))
