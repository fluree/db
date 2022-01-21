(ns fluree.db.json-ld.commit
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.util.log :as log]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.util.core :as util]
            [fluree.db.json-ld.credential :as cred]
            [fluree.json-ld.normalize :as normalize])
  #?(:clj (:import (fluree.db.flake Flake))))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const base-context ["https://flur.ee/ns/block"])

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
  (loop [[^Flake flake & r] s-flakes
         assert  nil
         retract nil]
    (if flake
      (let [add?     (true? (.-op flake))
            p-iri    (get-s-iri (.-p flake) db iri-map compact-fn)
            ref?     (get-in schema [:pred (.-p flake) :ref?])
            o        (if ref?
                       (do
                         (vswap! ctx assoc-in [p-iri "@type"] "@id")
                         (get-s-iri (.-o flake) db iri-map compact-fn))
                       (.-o flake))
            assert*  (if add?
                       (update-subj-prop assert p-iri o)
                       assert)
            retract* (if add?
                       retract
                       (update-subj-prop retract p-iri o))]
        (recur r assert* retract*))
      [assert retract])))


(defn generate-commit
  [db flakes {:keys [compact-fn id-key type-key context] :as opts}]
  (let [id->iri (volatile! (jld-ledger/predefined-sids-compact compact-fn))
        ctx     (volatile! {})]
    (loop [[s-flakes & r] (partition-by #(.-s ^Flake %) flakes)
           assert  []
           retract []]
      (if s-flakes
        (let [sid            (.-s ^Flake (first s-flakes))
              s-iri          (get-s-iri sid db id->iri compact-fn)
              non-iri-flakes (remove #(= const/$iri (.-p ^Flake %)) s-flakes)
              [s-assert s-retract ctx] (subject-block non-iri-flakes db id->iri ctx compact-fn)
              assert*        (if s-assert
                               (conj assert (assoc s-assert id-key s-iri))
                               assert)
              retract*       (if s-retract
                               (conj retract (assoc s-retract id-key s-iri))
                               retract)]
          (recur r assert* retract*))
        {:ctx     (dissoc @ctx type-key)                    ;; @type will be marked as @type: @id, which is implied
         :assert  assert
         :retract retract}))))


(defn- did-from-private
  [private-key]
  (let [acct-id (crypto/account-id-from-private private-key)]
    (str "did:fluree:" acct-id)))


(defn- commit-opts
  "Takes commit opts and merges in with defaults defined for the db."
  [db opts]
  (let [{:keys [context branch did private] :as opts*} (merge (:config db) opts)
        context*      (or (some->> (or context (:context db)) ;; local context overrides db context
                                   (conj base-context))
                          base-context)
        ctx-used-atom (atom {})
        compact-fn    (-> context*
                          json-ld/parse-context
                          (json-ld/compact-fn ctx-used-atom))
        private*      (or private (:private did))
        did*          (or (:id did) (some-> private*
                                            did-from-private))]
    (assoc opts* :context context*
                 :did did*
                 :private private*
                 :ctx-used-atom ctx-used-atom
                 :compact-fn compact-fn
                 :compact (fn [iri] (json-ld/compact iri compact-fn))
                 :branch (or branch "main")
                 :id-key (json-ld/compact "@id" compact-fn)
                 :type-key (json-ld/compact "@type" compact-fn))))


(defn- add-commit-hash
  "Adds hash key to commit document"
  [doc hash-key]
  (let [normalized (normalize/normalize doc)
        hash       (->> (->> normalized
                             crypto/sha2-256
                             (str "urn:sha256:")))]
    {:normalized normalized
     :hash       hash
     :commit     (assoc doc hash-key hash)}))


(defn- commit-json
  "Takes final commit object (as returned by add-commit-hash) and returns
  formatted json ready for publishing."
  [commit-object hash-key]
  (let [{:keys [normalized hash]} commit-object]
    (str (subs normalized 0 (dec (count normalized)))       ;; remove trailing '}', then add back
         ",\"" hash-key "\":" hash "}")))


(defn db
  "Commits a current DB's changes (since last commit) to the storage backend
  defined by the DB.

  Returns a modified DB with the last commit content-addressable storage location updates"
  [db opts]
  (let [{:keys [t novelty commit]} db
        {:keys [branch message type-key compact ctx-used-atom private return queue? push publish] :as opts*} (commit-opts db opts)
        ;; TODO - tsop index can get below flakes more efficiently once exists
        flakes         (filter #(= t (.-t ^Flake %)) (:spot novelty))
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
                        :publish    publish-p               ;; promise with eventual result once successful
                        :hash       hash
                        :db-before  db
                        :db-after   db*}]
    (if return
      (get res return)
      res)))
