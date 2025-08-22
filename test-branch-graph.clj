(require '[fluree.db.api :as fluree])
(require '[fluree.db.merge :as merge])

(let [conn @(fluree/connect-memory {})]
  (try
    @(fluree/create conn "test" {})
    (println "Testing branch-graph function...")
    (let [result @(merge/branch-graph conn "test" {:format :json})]
      (println "Result:" result))
    (catch Exception e
      (println "Error:" (.getMessage e))
      (.printStackTrace e))
    (finally
      @(fluree/disconnect conn))))