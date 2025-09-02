(ns fluree.db.merge.graph
  "Branch graph visualization for Fluree databases."
  (:require [clojure.string :as str]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.connection :as connection]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]))

(defn- get-all-branches
  "Gets all branches for a ledger."
  [conn ledger-spec]
  (go-try
    (let [[ledger-id _] (util.ledger/ledger-parts ledger-spec)]
      (if-some [primary-publisher (:primary-publisher conn)]
        ;; Get all nameservice records and filter for this ledger's branches
        (let [records (<? (nameservice/all-records primary-publisher))
              ;; Filter for this ledger's branches
              branches (distinct
                        (for [record records
                              :let [;; The ledger field is an object with @id in the nameservice records
                                    ledger-obj (get record "f:ledger")
                                    ledger-name (if (map? ledger-obj)
                                                  (get ledger-obj "@id")
                                                  ledger-obj)
                                    branch-name (get record "f:branch")]
                              :when (and (= ledger-name ledger-id) branch-name)]
                          {:name branch-name
                           :spec (str ledger-id ":" branch-name)
                           :t (get record "f:t")
                           :commit (get-in record ["f:commit" "@id"])}))
              ;; For each branch, get additional info
              branches-with-info (<? (go-try
                                       (loop [result []
                                              remaining branches]
                                         (if-let [branch (first remaining)]
                                           (let [branch-ledger (<? (connection/load-ledger conn (:spec branch)))
                                                 branch-info (<? (ledger/branch-info branch-ledger))]
                                             (recur (conj result (assoc branch :info branch-info))
                                                    (rest remaining)))
                                           result))))]
          branches-with-info)
        ;; No nameservice available
        (throw (ex-info "No nameservice available for querying branches"
                        {:status 400 :error :db/no-nameservice}))))))

(defn- load-commit-chain
  "Loads commit chain for a branch up to specified depth."
  [conn branch-spec depth]
  (go-try
    (let [ledger (<? (connection/load-ledger conn branch-spec))
          db (ledger/current-db ledger)
          commit-catalog (:commit-catalog conn)]
      (loop [commits []
             current-commit (:commit db)
             remaining-depth depth]
        (if (or (nil? current-commit)
                (and (not= depth :all) (zero? remaining-depth)))
          commits
          (let [;; current-commit might already be a map or need expansion
                commit-map (if (map? current-commit)
                             current-commit
                             (commit-data/json-ld->map current-commit nil))
                ;; Get parent commit address
                parent-address (get-in commit-map [:previous :address])
                ;; Load parent commit if exists
                parent-commit (when (and parent-address commit-catalog)
                                (try
                                  (let [;; Extract hash from parent address (address format is usually hash-based)
                                        parent-hash (last (str/split parent-address #"/"))
                                        parent-jsonld (<? (commit-storage/read-commit-jsonld
                                                           commit-catalog
                                                           parent-address
                                                           parent-hash))]
                                    (commit-data/json-ld->map parent-jsonld nil))
                                  (catch #?(:clj Exception :cljs js/Error) _
                                    ;; If we can't load parent, stop the chain
                                    nil)))]
            (recur (conj commits commit-map)
                   parent-commit
                   (if (= depth :all) :all (dec remaining-depth)))))))))

(defn- build-graph-data
  "Builds the graph data structure from branches and commits."
  [branches commit-chains]
  (let [;; Create a map of commit-id -> branches that contain it
        commit-to-branches (reduce (fn [acc [branch commits]]
                                     (reduce (fn [acc2 commit]
                                               (update acc2 (:id commit)
                                                       (fnil conj [])
                                                       (:name branch)))
                                             acc commits))
                                   {}
                                   (map vector branches commit-chains))

        ;; Collect all unique commits
        all-commits (reduce (fn [acc commits]
                              (reduce (fn [acc2 commit]
                                        (assoc acc2 (:id commit) commit))
                                      acc commits))
                            {}
                            commit-chains)

        ;; Build branch info
        branch-info (reduce (fn [acc [branch _]]
                              (let [head (:commit branch)]
                                (assoc acc (:name branch)
                                       {:head head
                                        :created-from (get-in branch [:info :created-from])
                                        :created-at (get-in branch [:info :created-at])})))
                            {}
                            (map vector branches commit-chains))]

    {:branches branch-info
     :commits (map (fn [[id commit]]
                     {:id id
                      :branches (get commit-to-branches id [])
                      :parents (if-let [prev (:previous commit)]
                                 [(:id prev)]
                                 [])
                      :message (or (:message commit) "")
                      :t (get-in commit [:data :t])
                      :timestamp (:timestamp commit)})
                   all-commits)
     :merges []})) ; TODO: Detect merges from commit data

(defn- render-ascii-graph
  "Renders the graph data as ASCII art similar to git log --graph."
  [graph-data]
  (let [commits (sort-by :t > (:commits graph-data))
        output (atom [])
        ;; Track active branches and their columns
        branch-cols (atom {})
        active-cols (atom #{})
        next-col (atom 0)]

    (doseq [[_ commit] (map-indexed vector commits)]
      (let [branches (set (:branches commit))
            commit-id (:id commit)
            short-id (if (str/includes? commit-id ":")
                       (let [parts (str/split commit-id #":")
                             hash-part (last parts)]
                         (subs hash-part 0 (min 7 (count hash-part))))
                       (subs commit-id 0 (min 7 (count commit-id))))
            message (:message commit)

            ;; Assign columns to new branches
            _ (doseq [branch branches]
                (when-not (contains? @branch-cols branch)
                  (swap! branch-cols assoc branch @next-col)
                  (swap! next-col inc)))

            ;; Get the leftmost column for this commit
            commit-col (apply min (map #(get @branch-cols %) branches))

            ;; Check if this is a merge (multiple branches converging)
            is-merge? (> (count branches) 1)

            ;; Build the graph prefix
            max-col (if (empty? @active-cols) 0 (apply max @active-cols))

            ;; Create the commit line
            commit-line (atom [])
            _ (dotimes [col (inc max-col)]
                (swap! commit-line conj
                       (cond
                         (= col commit-col) "*"
                         (contains? @active-cols col) "|"
                         :else " ")))

            graph-prefix (str/join " " @commit-line)

            ;; Add merge indicators if needed
            merge-line (when is-merge?
                         (let [merge-chars (atom [])]
                           (dotimes [col (inc max-col)]
                             (swap! merge-chars conj
                                    (cond
                                      (= col commit-col) "|\\"
                                      (contains? @active-cols col) "|"
                                      :else " ")))
                           (str/join " " @merge-chars)))

            ;; Format the commit info
            commit-info (str "commit " short-id " (t: " (:t commit) ")")
            branch-info (when (> (count branches) 1)
                          (str "Branches: " (str/join ", " branches)))
            message-info (when (seq message)
                           (str "    " message))

            ;; Update active columns for next iteration
            branch-columns (set (map #(get @branch-cols %) branches))
            _ (reset! active-cols branch-columns)]

        ;; Add merge line if needed
        (when merge-line
          (swap! output conj merge-line))

        ;; Add commit line
        (swap! output conj (str graph-prefix " " commit-info))

        ;; Add branch info if multiple branches
        (when branch-info
          (let [info-prefix (str/join " " (repeat (inc max-col) "|"))]
            (swap! output conj (str info-prefix " " branch-info))))

        ;; Add message if present
        (when message-info
          (let [info-prefix (str/join " " (repeat (inc max-col) "|"))]
            (swap! output conj (str info-prefix " "))
            (swap! output conj (str info-prefix " " message-info))
            (swap! output conj (str info-prefix " "))))))

    (str/join "\n" @output)))

(defn branch-graph
  "Returns a graph representation of branches and their relationships.
  
  Parameters:
    conn - Connection object
    ledger-spec - Ledger specification (e.g., 'myledger')
    opts - Options map:
      :format - :json (default) or :ascii
      :depth - Number of commits to show (default 20, :all for everything)
      :branches - Specific branches to include (default: all)
  
  Returns promise resolving to graph data in requested format."
  [conn ledger-spec opts]
  (go-try
    (let [{:keys [format depth branches]
           :or {format :json
                depth 20
                branches :all}} opts

          ;; Get all branches or filter to requested ones
          all-branches (<? (get-all-branches conn ledger-spec))
          filtered-branches (if (= branches :all)
                              all-branches
                              (filter #(contains? (set branches) (:name %))
                                      all-branches))

          ;; Load commit chains for each branch
          commit-chains (<? (go-try
                              (loop [chains []
                                     remaining filtered-branches]
                                (if-let [branch (first remaining)]
                                  (let [commits (<? (load-commit-chain conn
                                                                       (:spec branch)
                                                                       depth))]
                                    (recur (conj chains commits)
                                           (rest remaining)))
                                  chains))))

          ;; Build the graph data structure
          graph-data (build-graph-data filtered-branches commit-chains)]

      (case format
        :ascii (render-ascii-graph graph-data)
        :json graph-data
        ;; Default to JSON
        graph-data))))