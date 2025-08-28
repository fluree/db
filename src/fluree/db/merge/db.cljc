(ns fluree.db.merge.db
  "Database preparation and staging functions for merge operations."
  (:require [clojure.core.async :as async]
            [fluree.db.async-db :as async-db]
            [fluree.db.connection :as connection]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.ledger :as ledger]
            [fluree.db.query.range :as query-range]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn prepare-target-db-namespaces
  "Adds only truly new namespaces from source into the target-db using target's code space."
  [target-db source-namespaces]
  (let [existing-iris (set (keys (:namespaces target-db)))
        new-namespaces (remove existing-iris source-namespaces)]
    (log/debug "prepare-target-db-namespaces: existing=" existing-iris
               "source=" source-namespaces
               "new=" new-namespaces)
    (if (seq new-namespaces)
      (flake-db/with-namespaces target-db new-namespaces)
      target-db)))

(defn- ensure-sync-db-internal
  "Ensures we have a synchronous database, dereferencing async if needed."
  [db]
  (go-try
    (if (async-db/db? db)
      (<? (async-db/deref-async db))
      db)))

(defn stage-flakes
  "Stages flakes directly into a database.
  Handles retractions by finding and removing matching assertions from the database."
  [db flakes opts]
  (go-try
    (if (empty? flakes)
      db
      (let [db* (<? (ensure-sync-db-internal db))
            next-t (flake/next-t (:t db*))
            ;; retime all flakes to the new t
            retimed (into [] (map (fn [f]
                                    (flake/create (flake/s f)
                                                  (flake/p f)
                                                  (flake/o f)
                                                  (flake/dt f)
                                                  next-t
                                                  (flake/op f)
                                                  (flake/m f)))) flakes)
            {adds true rems false} (group-by flake/op retimed)
            ;; For retractions, we need to find the actual flakes to remove
            ;; Look in both novelty AND the indexed data
            root-db (policy/root db*)
            flakes-to-remove (when rems
                               (<? (async/go
                                     (loop [to-remove []
                                            remaining rems]
                                       (if-let [retraction (first remaining)]
                                         (let [s (flake/s retraction)
                                               p (flake/p retraction)
                                               o (flake/o retraction)
                                               dt (flake/dt retraction)
                                              ;; Find matching flakes in the database
                                               existing (<? (query-range/index-range root-db nil :spot = [s p]
                                                                                     {:flake-xf (filter #(and (= (flake/o %) o)
                                                                                                              (= (flake/dt %) dt)
                                                                                                              (true? (flake/op %))))}))
                                               to-remove* (into to-remove existing)]
                                           (recur to-remove* (rest remaining)))
                                         to-remove)))))
            db-after (-> db*
                         (assoc :t next-t
                                :staged {:txn (:message opts "Rebase merge")
                                         :author (:author opts "system/merge")
                                         :annotation (:annotation opts)})
                         (commit-data/update-novelty (or adds []) flakes-to-remove))]
        db-after))))

(defn get-db-at-state
  "Gets database at a specific state (t-value or SHA)."
  [conn branch-spec state-spec]
  (go-try
    (let [ledger (<? (connection/load-ledger conn branch-spec))
          current-db (ledger/current-db ledger)]
      (cond
        (:t state-spec)
        ;; Simply set the t value to travel back in time
        ;; The commit info will still be from current, but that's OK
        ;; We'll compute the changes needed based on the data state
        (assoc current-db :t (:t state-spec))

        (:sha state-spec)
        ;; Use the sha->t method from the TimeTravel protocol
        (let [target-t (<? (time-travel/sha->t current-db (:sha state-spec)))]
          (assoc current-db :t target-t))

        :else
        (throw (ex-info "Invalid state specification. Must provide :t or :sha"
                        {:status 400 :error :db/invalid-state-spec}))))))