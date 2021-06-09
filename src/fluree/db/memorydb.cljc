(ns fluree.db.memorydb
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.query.schema :as schema]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.session :as session]
            [fluree.db.util.async :as async-util]))

(declare bootstrap-flakes genesis-ecount)

(defn fake-conn []
  "Returns a fake connection object that is suitable for use with the memorydb if
  no other conn is available."
  {:transactor? false})

(defn new-db
  "Creates a local, in-memory but bootstrapped db (primarily for testing)."
  ([conn ledger] (new-db conn ledger nil))
  ([conn ledger bootstrap-opts]
   (let [pc (async/promise-chan)]
     (async/go
       (let [sess     (session/session conn ledger {:connect? false})
             blank-db (:blank-db sess)
             {:keys [spot psot post opst]} (:novelty blank-db)
             db*      (update blank-db :novelty merge {:spot (into spot bootstrap-flakes)
                                                       :psot (into psot bootstrap-flakes)
                                                       :post (into post bootstrap-flakes)
                                                       :opst (into opst (filter #(number? (:o %)) bootstrap-flakes))})
             schema   (<? (schema/schema-map db*))]
         (async/put! pc (assoc db* :schema schema
                                   :ecount genesis-ecount
                                   :block 1))))
     pc)))


(defn transact-flakes
  "Transacts a series of preformatted flakes into the in-memory db."
  [db flakes]
  (let [pc (async/promise-chan)]
    (async/go
      (let [db*   (if (async-util/channel? db)
                    (async/<! db)
                    db)
            block (inc (:block db*))
            db**  (async/<! (dbproto/-with db* block flakes))]
        (async/put! pc db**)))
    pc))


(defn transact-tuples
  "Transacts tuples which includes s, p, o and optionally op.
  If op is not explicitly false, it is assumed to be true.

  Does zero validation that tuples are accurate"
  [db tuples]
  (let [pc (async/promise-chan)]
    (async/go
      (let [db*    (if (async-util/channel? db)
                     (async/<! db)
                     db)
            t      (dec (:t db*))
            flakes (->> tuples
                        (map (fn [[s p o op]]
                               (flake/->Flake s p o t (if (false? op) false true) nil))))
            db**   (async/<! (transact-flakes db* flakes))]
        (async/put! pc db**)))
    pc))


(comment

  ;; sample usage
  ;; use fake-conn
  (def db (new-db (fake-conn) "blah/two"))

  ;; a normal query will work
  @(fluree.db.api/query db {:select [:*] :from "_collection"})

  ;; must manually set new predicate subject ids for now
  (def db2 (transact-tuples db
                            [[(flake/->sid const/$_user 1001) const/$_user:username "brian"]
                             [(flake/->sid const/$_user 1002) const/$_user:username "lois"]]))

  ;; able to use new db (db2) like any normal db.
  @(fluree.db.api/query db2 {:select [:*] :from "_user"})


  )

;; TODO - this is now duplicated with fluree.db.ledger.bootstrap - consolidate when this becomes an actually supported db.
(def ^:const genesis-ecount
  {const/$_predicate  (flake/->sid const/$_predicate 1000)
   const/$_collection (flake/->sid const/$_collection 19)
   const/$_tag        (flake/->sid const/$_tag 1000)
   const/$_fn         (flake/->sid const/$_fn 1000)
   const/$_user       (flake/->sid const/$_user 1000)
   const/$_auth       (flake/->sid const/$_auth 1000)
   const/$_role       (flake/->sid const/$_role 1000)
   const/$_rule       (flake/->sid const/$_rule 1000)
   const/$_setting    (flake/->sid const/$_setting 1000)
   const/$_shard      (flake/->sid const/$_shard 1000)})

(def ^:const bootstrap-flakes
  [#Flake[158329674399744 111 105553116266496 -1 true nil]
   #Flake[158329674399744 116 "root" -1 true nil]
   #Flake[158329674399744 117 52776558133313 -1 true nil]
   #Flake[140737488355328 80 "root" -1 true nil]
   #Flake[140737488355328 81 "Root rule, gives full access" -1 true nil]
   #Flake[140737488355328 82 "*" -1 true nil]
   #Flake[140737488355328 83 "*" -1 true nil]
   #Flake[140737488355328 84 70368744177664 -1 true nil]
   #Flake[140737488355328 85 52776558133278 -1 true nil]
   #Flake[123145302310912 70 "root" -1 true nil]
   #Flake[123145302310912 71 "Root role." -1 true nil]
   #Flake[123145302310912 72 140737488355328 -1 true nil]
   #Flake[105553116266496 60 "TfBnkwYe8V3TQ4hWe6Z173a34iz9EbuNoHU" -1 true nil]
   #Flake[105553116266496 65 123145302310912 -1 true nil]
   #Flake[70368744177665 90 "false" -1 true nil]
   #Flake[70368744177665 92 "false" -1 true nil]
   #Flake[70368744177665 93 "Denies access to any rule or spec this is attached to." -1 true nil]
   #Flake[70368744177664 90 "true" -1 true nil]
   #Flake[70368744177664 92 "true" -1 true nil]
   #Flake[70368744177664 93 "Allows access to any rule or spec this is attached to." -1 true nil]
   #Flake[52776558133318 30 "_setting/language:ru" -1 true nil]
   #Flake[52776558133318 31 "Russian" -1 true nil]
   #Flake[52776558133317 30 "_setting/language:id" -1 true nil]
   #Flake[52776558133317 31 "Indonesian" -1 true nil]
   #Flake[52776558133316 30 "_setting/language:hi" -1 true nil]
   #Flake[52776558133316 31 "Hindi" -1 true nil]
   #Flake[52776558133315 30 "_setting/language:fr" -1 true nil]
   #Flake[52776558133315 31 "French" -1 true nil]
   #Flake[52776558133314 30 "_setting/language:es" -1 true nil]
   #Flake[52776558133314 31 "Spanish" -1 true nil]
   #Flake[52776558133313 30 "_setting/language:en" -1 true nil]
   #Flake[52776558133313 31 "English" -1 true nil]
   #Flake[52776558133312 30 "_setting/language:cn" -1 true nil]
   #Flake[52776558133312
          31
          "Chinese. FullText search uses the Apache Lucene Smart Chinese Analyzer for Chinese and mixed Chinese-English text, https://lucene.apache.org/core/4_0_0/analyzers-smartcn/org/apache/lucene/analysis/cn/smart/SmartChineseAnalyzer.html"
          -1
          true
          nil]
   #Flake[52776558133311 30 "_setting/language:br" -1 true nil]
   #Flake[52776558133311 31 "Brazilian Portuguese" -1 true nil]
   #Flake[52776558133310 30 "_setting/language:bn" -1 true nil]
   #Flake[52776558133310 31 "Bengali" -1 true nil]
   #Flake[52776558133309 30 "_setting/language:ar" -1 true nil]
   #Flake[52776558133309 31 "Arabic" -1 true nil]
   #Flake[52776558133303 30 "_auth/type:password-secp256k1" -1 true nil]
   #Flake[52776558133298 30 "_auth/type:secp256k1" -1 true nil]
   #Flake[52776558133289 30 "_setting/consensus:pbft" -1 true nil]
   #Flake[52776558133288 30 "_setting/consensus:raft" -1 true nil]
   #Flake[52776558133282 30 "_rule/ops:token" -1 true nil]
   #Flake[52776558133281 30 "_rule/ops:logs" -1 true nil]
   #Flake[52776558133280 30 "_rule/ops:query" -1 true nil]
   #Flake[52776558133279 30 "_rule/ops:transact" -1 true nil]
   #Flake[52776558133278 30 "_rule/ops:all" -1 true nil]
   #Flake[52776558133265 30 "_predicate/type:geojson" -1 true nil]
   #Flake[52776558133264 30 "_predicate/type:json" -1 true nil]
   #Flake[52776558133263 30 "_predicate/type:tag" -1 true nil]
   #Flake[52776558133262 30 "_predicate/type:bigdec" -1 true nil]
   #Flake[52776558133261 30 "_predicate/type:double" -1 true nil]
   #Flake[52776558133260 30 "_predicate/type:float" -1 true nil]
   #Flake[52776558133259 30 "_predicate/type:bigint" -1 true nil]
   #Flake[52776558133258 30 "_predicate/type:long" -1 true nil]
   #Flake[52776558133257 30 "_predicate/type:int" -1 true nil]
   #Flake[52776558133256 30 "_predicate/type:bytes" -1 true nil]
   #Flake[52776558133255 30 "_predicate/type:uri" -1 true nil]
   #Flake[52776558133254 30 "_predicate/type:uuid" -1 true nil]
   #Flake[52776558133253 30 "_predicate/type:instant" -1 true nil]
   #Flake[52776558133252 30 "_predicate/type:boolean" -1 true nil]
   #Flake[52776558133250 30 "_predicate/type:ref" -1 true nil]
   #Flake[52776558133249 30 "_predicate/type:string" -1 true nil]
   #Flake[17592186044425 40 "_setting" -1 true nil]
   #Flake[17592186044425 41 "Database settings." -1 true nil]
   #Flake[17592186044425 42 "1" -1 true nil]
   #Flake[17592186044424 40 "_rule" -1 true nil]
   #Flake[17592186044424 41 "Permission rules" -1 true nil]
   #Flake[17592186044424 42 "1" -1 true nil]
   #Flake[17592186044423 40 "_role" -1 true nil]
   #Flake[17592186044423
          41
          "Roles group multiple permission rules to an assignable category, like 'employee', 'customer'."
          -1
          true
          nil]
   #Flake[17592186044423 42 "1" -1 true nil]
   #Flake[17592186044422 40 "_auth" -1 true nil]
   #Flake[17592186044422
          41
          "Auth records. Every db interaction is performed by an auth record which governs permissions."
          -1
          true
          nil]
   #Flake[17592186044422 42 "1" -1 true nil]
   #Flake[17592186044421 40 "_user" -1 true nil]
   #Flake[17592186044421 41 "Database users" -1 true nil]
   #Flake[17592186044421 42 "1" -1 true nil]
   #Flake[17592186044420 40 "_fn" -1 true nil]
   #Flake[17592186044420 41 "Database functions" -1 true nil]
   #Flake[17592186044420 42 "1" -1 true nil]
   #Flake[17592186044419 40 "_tag" -1 true nil]
   #Flake[17592186044419 41 "Tags" -1 true nil]
   #Flake[17592186044419 42 "1" -1 true nil]
   #Flake[17592186044418 40 "_shard" -1 true nil]
   #Flake[17592186044418 41 "Shard settings." -1 true nil]
   #Flake[17592186044418 42 "1" -1 true nil]
   #Flake[17592186044417 40 "_collection" -1 true nil]
   #Flake[17592186044417 41 "Schema collections list" -1 true nil]
   #Flake[17592186044417 42 "1" -1 true nil]
   #Flake[17592186044416 40 "_predicate" -1 true nil]
   #Flake[17592186044416 41 "Schema predicate definition" -1 true nil]
   #Flake[17592186044416 42 "1" -1 true nil]
   #Flake[122 10 "_shard/mutable" -1 true nil]
   #Flake[122
          11
          "Whether this shard is mutable. If not specified, defaults to 'false', meaning the data is immutable."
          -1
          true
          nil]
   #Flake[122 12 52776558133252 -1 true nil]
   #Flake[121 10 "_shard/miners" -1 true nil]
   #Flake[121 11 "Miners (auth records) assigned to this shard" -1 true nil]
   #Flake[121 12 52776558133250 -1 true nil]
   #Flake[121 14 true -1 true nil]
   #Flake[121 19 "_auth" -1 true nil]
   #Flake[120 10 "_shard/name" -1 true nil]
   #Flake[120 11 "Name of this shard" -1 true nil]
   #Flake[120 12 52776558133249 -1 true nil]
   #Flake[120 13 true -1 true nil]
   #Flake[117 10 "_setting/language" -1 true nil]
   #Flake[117
          11
          "Default database language. Used for full-text search. See docs for valid options."
          -1
          true
          nil]
   #Flake[117 12 52776558133263 -1 true nil]
   #Flake[117 26 true -1 true nil]
   #Flake[116 10 "_setting/id" -1 true nil]
   #Flake[116 11 "Unique setting id." -1 true nil]
   #Flake[116 12 52776558133249 -1 true nil]
   #Flake[116 13 true -1 true nil]
   #Flake[115 10 "_setting/txMax" -1 true nil]
   #Flake[115 11 "Maximum transaction size in bytes." -1 true nil]
   #Flake[115 12 52776558133258 -1 true nil]
   #Flake[114 10 "_setting/passwords" -1 true nil]
   #Flake[114 11 "Whether password-based authentication is enabled on this db." -1 true nil]
   #Flake[114 12 52776558133252 -1 true nil]
   #Flake[113 10 "_setting/doc" -1 true nil]
   #Flake[113 11 "Optional docstring for the db." -1 true nil]
   #Flake[113 12 52776558133249 -1 true nil]
   #Flake[112 10 "_setting/consensus" -1 true nil]
   #Flake[112 11 "Consensus type for this db." -1 true nil]
   #Flake[112 12 52776558133263 -1 true nil]
   #Flake[112 26 true -1 true nil]
   #Flake[111 10 "_setting/ledgers" -1 true nil]
   #Flake[111
          11
          "Reference to auth identities that are allowed to act as ledgers for this database."
          -1
          true
          nil]
   #Flake[111 12 52776558133250 -1 true nil]
   #Flake[111 14 true -1 true nil]
   #Flake[111 19 "_auth" -1 true nil]
   #Flake[110 10 "_setting/anonymous" -1 true nil]
   #Flake[110 11 "Reference to auth identity to use for anonymous requests to this db." -1 true nil]
   #Flake[110 12 52776558133250 -1 true nil]
   #Flake[110 19 "_auth" -1 true nil]
   #Flake[109 10 "_tx/error" -1 true nil]
   #Flake[109 11 "Error type and message, if an error happened for this transaction." -1 true nil]
   #Flake[109 12 52776558133249 -1 true nil]
   #Flake[108 10 "_tx/tempids" -1 true nil]
   #Flake[108 11 "Tempid JSON map for this transaction." -1 true nil]
   #Flake[108 12 52776558133249 -1 true nil]
   #Flake[107 10 "_tx/sig" -1 true nil]
   #Flake[107 11 "Signature of original JSON transaction command." -1 true nil]
   #Flake[107 12 52776558133249 -1 true nil]
   #Flake[106 10 "_tx/tx" -1 true nil]
   #Flake[106 11 "Original JSON transaction command." -1 true nil]
   #Flake[106 12 52776558133249 -1 true nil]
   #Flake[105 10 "_tx/doc" -1 true nil]
   #Flake[105 11 "Optional docstring for the transaction." -1 true nil]
   #Flake[105 12 52776558133249 -1 true nil]
   #Flake[104 10 "_tx/altId" -1 true nil]
   #Flake[104
          11
          "Alternative Unique ID for the transaction that the user can supply. Transaction will throw if not unique."
          -1
          true
          nil]
   #Flake[104 12 52776558133249 -1 true nil]
   #Flake[104 13 true -1 true nil]
   #Flake[103 10 "_tx/nonce" -1 true nil]
   #Flake[103
          11
          "A nonce that helps ensure identical transactions have unique txids, and also can be used for logic within smart functions. Note this nonce does not enforce uniqueness, use _tx/altId if uniqueness must be enforced."
          -1
          true
          nil]
   #Flake[103 12 52776558133258 -1 true nil]
   #Flake[103 15 true -1 true nil]
   #Flake[102 10 "_tx/authority" -1 true nil]
   #Flake[102 11 "If this transaction utilized an authority, reference to it." -1 true nil]
   #Flake[102 12 52776558133250 -1 true nil]
   #Flake[102 19 "_auth" -1 true nil]
   #Flake[101 10 "_tx/auth" -1 true nil]
   #Flake[101 11 "Reference to the auth id for this transaction." -1 true nil]
   #Flake[101 12 52776558133250 -1 true nil]
   #Flake[101 19 "_auth" -1 true nil]
   #Flake[100 10 "_tx/id" -1 true nil]
   #Flake[100 11 "Unique transaction ID." -1 true nil]
   #Flake[100 12 52776558133249 -1 true nil]
   #Flake[100 13 true -1 true nil]
   #Flake[99 10 "_tx/hash" -1 true nil]
   #Flake[99 11 "Error type and message, if an error happened for this transaction." -1 true nil]
   #Flake[99 12 52776558133249 -1 true nil]
   #Flake[95 10 "_fn/language" -1 true nil]
   #Flake[95 11 "Programming language used." -1 true nil]
   #Flake[95 12 52776558133263 -1 true nil]
   #Flake[94 10 "_fn/spec" -1 true nil]
   #Flake[94
          11
          "Optional spec for parameters. Spec should be structured as a map, parameter names are keys and the respective spec is the value."
          -1
          true
          nil]
   #Flake[94 12 52776558133264 -1 true nil]
   #Flake[93 10 "_fn/doc" -1 true nil]
   #Flake[93 11 "Doc string describing this function." -1 true nil]
   #Flake[93 12 52776558133249 -1 true nil]
   #Flake[92 10 "_fn/code" -1 true nil]
   #Flake[92 11 "Actual database function code." -1 true nil]
   #Flake[92 12 52776558133249 -1 true nil]
   #Flake[91 10 "_fn/params" -1 true nil]
   #Flake[91 11 "List of parameters this function supports." -1 true nil]
   #Flake[91 12 52776558133249 -1 true nil]
   #Flake[90 10 "_fn/name" -1 true nil]
   #Flake[90 11 "Function name" -1 true nil]
   #Flake[90 12 52776558133249 -1 true nil]
   #Flake[90 13 true -1 true nil]
   #Flake[87 10 "_rule/errorMessage" -1 true nil]
   #Flake[87 11 "The error message that should be displayed if this rule makes a transaction fail." -1 true nil]
   #Flake[87 12 52776558133249 -1 true nil]
   #Flake[86 10 "_rule/collectionDefault" -1 true nil]
   #Flake[86 11 "Default rule applies to collection only if no other more specific rule matches." -1 true nil]
   #Flake[86 12 52776558133252 -1 true nil]
   #Flake[86 15 true -1 true nil]
   #Flake[85 10 "_rule/ops" -1 true nil]
   #Flake[85 11 "Operations (using tags) that this rule applies to." -1 true nil]
   #Flake[85 12 52776558133263 -1 true nil]
   #Flake[85 14 true -1 true nil]
   #Flake[85 26 true -1 true nil]
   #Flake[84 10 "_rule/fns" -1 true nil]
   #Flake[84 11 "Ref to functions, which resolve to true or false." -1 true nil]
   #Flake[84 12 52776558133250 -1 true nil]
   #Flake[84 14 true -1 true nil]
   #Flake[84 19 "_fn" -1 true nil]
   #Flake[83 10 "_rule/predicates" -1 true nil]
   #Flake[83
          11
          "Specific predicate this rule applies to, or wildcard '*' predicate which will be run only if no specific predicate rules match."
          -1
          true
          nil]
   #Flake[83 12 52776558133249 -1 true nil]
   #Flake[83 14 true -1 true nil]
   #Flake[83 15 true -1 true nil]
   #Flake[82 10 "_rule/collection" -1 true nil]
   #Flake[82 11 "Stream name/glob that should match." -1 true nil]
   #Flake[82 12 52776558133249 -1 true nil]
   #Flake[82 15 true -1 true nil]
   #Flake[81 10 "_rule/doc" -1 true nil]
   #Flake[81 11 "Optional docstring for rule." -1 true nil]
   #Flake[81 12 52776558133249 -1 true nil]
   #Flake[80 10 "_rule/id" -1 true nil]
   #Flake[80 11 "Optional rule unique id" -1 true nil]
   #Flake[80 12 52776558133249 -1 true nil]
   #Flake[80 13 true -1 true nil]
   #Flake[72 10 "_role/rules" -1 true nil]
   #Flake[72
          11
          "Reference to rules this role contains. Multi-cardinality. Rules define actual permissions."
          -1
          true
          nil]
   #Flake[72 12 52776558133250 -1 true nil]
   #Flake[72 14 true -1 true nil]
   #Flake[72 19 "_rule" -1 true nil]
   #Flake[71 10 "_role/doc" -1 true nil]
   #Flake[71 11 "Optional docstring for role." -1 true nil]
   #Flake[71 12 52776558133249 -1 true nil]
   #Flake[70 10 "_role/id" -1 true nil]
   #Flake[70
          11
          "Unique role id. A role contains a collection of rule permissions. This role id can be used to easily get a set of permission for a role like 'customer', 'employee', etc."
          -1
          true
          nil]
   #Flake[70 12 52776558133249 -1 true nil]
   #Flake[70 13 true -1 true nil]
   #Flake[69 10 "_auth/fuel" -1 true nil]
   #Flake[69 11 "Fuel this auth record has." -1 true nil]
   #Flake[69 12 52776558133258 -1 true nil]
   #Flake[69 15 true -1 true nil]
   #Flake[68 10 "_auth/authority" -1 true nil]
   #Flake[68 11 "Authorities for this auth record. References another _auth record." -1 true nil]
   #Flake[68 12 52776558133250 -1 true nil]
   #Flake[68 14 true -1 true nil]
   #Flake[68 19 "_auth" -1 true nil]
   #Flake[67 10 "_auth/type" -1 true nil]
   #Flake[67 11 "Tag to identify underlying auth record type, if necessary." -1 true nil]
   #Flake[67 12 52776558133263 -1 true nil]
   #Flake[67 19 "_auth" -1 true nil]
   #Flake[67 26 true -1 true nil]
   #Flake[66 10 "_auth/doc" -1 true nil]
   #Flake[66 11 "Optional docstring for auth record." -1 true nil]
   #Flake[66 12 52776558133249 -1 true nil]
   #Flake[65 10 "_auth/roles" -1 true nil]
   #Flake[65 11 "Reference to roles that this authentication record is governed by." -1 true nil]
   #Flake[65 12 52776558133250 -1 true nil]
   #Flake[65 14 true -1 true nil]
   #Flake[65 19 "_role" -1 true nil]
   #Flake[62 10 "_auth/salt" -1 true nil]
   #Flake[62 11 "Salt used for auth record, if the auth type requires it." -1 true nil]
   #Flake[62 12 52776558133256 -1 true nil]
   #Flake[61 10 "_auth/password" -1 true nil]
   #Flake[61 11 "Encrypted password." -1 true nil]
   #Flake[61 12 52776558133249 -1 true nil]
   #Flake[61 15 true -1 true nil]
   #Flake[60 10 "_auth/id" -1 true nil]
   #Flake[60 11 "Unique auth id. Used to store derived public key (but doesn't have to)." -1 true nil]
   #Flake[60 12 52776558133249 -1 true nil]
   #Flake[60 13 true -1 true nil]
   #Flake[53 10 "_user/doc" -1 true nil]
   #Flake[53 11 "Optional docstring for user." -1 true nil]
   #Flake[53 12 52776558133249 -1 true nil]
   #Flake[52 10 "_user/roles" -1 true nil]
   #Flake[52
          11
          "Default roles to use for this user. If roles are specified via an auth record, those will over-ride these roles."
          -1
          true
          nil]
   #Flake[52 12 52776558133250 -1 true nil]
   #Flake[52 14 true -1 true nil]
   #Flake[52 19 "_role" -1 true nil]
   #Flake[51 10 "_user/auth" -1 true nil]
   #Flake[51 11 "User's auth records" -1 true nil]
   #Flake[51 12 52776558133250 -1 true nil]
   #Flake[51 13 true -1 true nil]
   #Flake[51 14 true -1 true nil]
   #Flake[51 19 "_auth" -1 true nil]
   #Flake[50 10 "_user/username" -1 true nil]
   #Flake[50 11 "Unique account ID (string). Emails are nice for business apps." -1 true nil]
   #Flake[50 12 52776558133249 -1 true nil]
   #Flake[50 13 true -1 true nil]
   #Flake[45 10 "_collection/shard" -1 true nil]
   #Flake[45
          11
          "The shard that this collection is assigned to. If none assigned, defaults to 'default' shard."
          -1
          true
          nil]
   #Flake[45 12 52776558133250 -1 true nil]
   #Flake[45 19 "_shard" -1 true nil]
   #Flake[44 10 "_collection/specDoc" -1 true nil]
   #Flake[44 11 "Optional docstring for _collection/spec." -1 true nil]
   #Flake[44 12 52776558133249 -1 true nil]
   #Flake[43 10 "_collection/spec" -1 true nil]
   #Flake[43
          11
          "Spec for the collection. All entities within this collection must meet this spec. Spec is run post-transaction, but before committing a new block."
          -1
          true
          nil]
   #Flake[43 12 52776558133250 -1 true nil]
   #Flake[43 14 true -1 true nil]
   #Flake[43 19 "_fn" -1 true nil]
   #Flake[42 10 "_collection/version" -1 true nil]
   #Flake[42 11 "Version number for this collection's schema." -1 true nil]
   #Flake[42 12 52776558133249 -1 true nil]
   #Flake[42 15 true -1 true nil]
   #Flake[41 10 "_collection/doc" -1 true nil]
   #Flake[41 11 "Optional docstring for collection." -1 true nil]
   #Flake[41 12 52776558133249 -1 true nil]
   #Flake[40 10 "_collection/name" -1 true nil]
   #Flake[40 11 "Schema collection name" -1 true nil]
   #Flake[40 12 52776558133249 -1 true nil]
   #Flake[40 13 true -1 true nil]
   #Flake[31 10 "_tag/doc" -1 true nil]
   #Flake[31 11 "Optional docstring for tag." -1 true nil]
   #Flake[31 12 52776558133249 -1 true nil]
   #Flake[30 10 "_tag/id" -1 true nil]
   #Flake[30 11 "Namespaced tag id" -1 true nil]
   #Flake[30 12 52776558133249 -1 true nil]
   #Flake[30 13 true -1 true nil]
   #Flake[30 16 true -1 true nil]
   #Flake[27 10 "_predicate/fullText" -1 true nil]
   #Flake[27 11 "If true, full text search is enabled on this predicate." -1 true nil]
   #Flake[27 12 52776558133252 -1 true nil]
   #Flake[26 10 "_predicate/restrictTag" -1 true nil]
   #Flake[26
          11
          "If true, a tag, which corresponds to the predicate object must exist before adding predicate-object pair."
          -1
          true
          nil]
   #Flake[26 12 52776558133252 -1 true nil]
   #Flake[25 10 "_predicate/txSpecDoc" -1 true nil]
   #Flake[25 11 "Optional docstring for _predicate/spec." -1 true nil]
   #Flake[25 12 52776558133249 -1 true nil]
   #Flake[24 10 "_predicate/txSpec" -1 true nil]
   #Flake[24
          11
          "Spec performed on all of this predicate in a txn. Specs are run post-transaction, before a new block is committed."
          -1
          true
          nil]
   #Flake[24 12 52776558133250 -1 true nil]
   #Flake[24 14 true -1 true nil]
   #Flake[24 19 "_fn" -1 true nil]
   #Flake[23 10 "_predicate/specDoc" -1 true nil]
   #Flake[23 11 "Optional docstring for _predicate/spec." -1 true nil]
   #Flake[23 12 52776558133249 -1 true nil]
   #Flake[22 10 "_predicate/deprecated" -1 true nil]
   #Flake[22
          11
          "Boolean flag if this predicate has been deprecated. This is primarily informational, however a warning may be issued with query responses."
          -1
          true
          nil]
   #Flake[22 12 52776558133252 -1 true nil]
   #Flake[21 10 "_predicate/encrypted" -1 true nil]
   #Flake[21
          11
          "Boolean flag if this predicate is stored encrypted. Transactions will ignore the _predicate/type and ensure it is a string. Query engines should have the decryption key."
          -1
          true
          nil]
   #Flake[21 12 52776558133252 -1 true nil]
   #Flake[20 10 "_predicate/spec" -1 true nil]
   #Flake[20
          11
          "Spec performed on this predicate. Specs are run post-transaction, before a new block is committed."
          -1
          true
          nil]
   #Flake[20 12 52776558133250 -1 true nil]
   #Flake[20 14 true -1 true nil]
   #Flake[20 19 "_fn" -1 true nil]
   #Flake[19 10 "_predicate/restrictCollection" -1 true nil]
   #Flake[19
          11
          "When an predicate is a reference type (ref), it can be optionally restricted to this collection."
          -1
          true
          nil]
   #Flake[19 12 52776558133249 -1 true nil]
   #Flake[18 10 "_predicate/noHistory" -1 true nil]
   #Flake[18
          11
          "Does not retain any history, making historical queries always use the current value."
          -1
          true
          nil]
   #Flake[18 12 52776558133252 -1 true nil]
   #Flake[17 10 "_predicate/component" -1 true nil]
   #Flake[17
          11
          "If the sub-entities for this predicate should always be deleted if this predicate is deleted. Only applies for predicates that refer to another collection."
          -1
          true
          nil]
   #Flake[17 12 52776558133252 -1 true nil]
   #Flake[16 10 "_predicate/upsert" -1 true nil]
   #Flake[16
          11
          "Only valid for unique predicates. When adding a new subject, will upsert existing subject instead of throwing an exception if the value already exists."
          -1
          true
          nil]
   #Flake[16 12 52776558133252 -1 true nil]
   #Flake[15 10 "_predicate/index" -1 true nil]
   #Flake[15 11 "If this predicate should be indexed." -1 true nil]
   #Flake[15 12 52776558133252 -1 true nil]
   #Flake[14 10 "_predicate/multi" -1 true nil]
   #Flake[14 11 "If this predicate supports multiple cardinality, or many values." -1 true nil]
   #Flake[14 12 52776558133252 -1 true nil]
   #Flake[13 10 "_predicate/unique" -1 true nil]
   #Flake[13
          11
          "If uniqueness for this predicate should be enforced. Unique predicates can be used as an identity."
          -1
          true
          nil]
   #Flake[13 12 52776558133252 -1 true nil]
   #Flake[12 10 "_predicate/type" -1 true nil]
   #Flake[12 11 "The specific type for this predicate has to be a valueType." -1 true nil]
   #Flake[12 12 52776558133263 -1 true nil]
   #Flake[12 26 true -1 true nil]
   #Flake[11 10 "_predicate/doc" -1 true nil]
   #Flake[11 11 "Optional docstring for predicate." -1 true nil]
   #Flake[11 12 52776558133249 -1 true nil]
   #Flake[10 10 "_predicate/name" -1 true nil]
   #Flake[10 11 "Predicate name" -1 true nil]
   #Flake[10 12 52776558133249 -1 true nil]
   #Flake[10 13 true -1 true nil]
   #Flake[7 10 "_block/sigs" -1 true nil]
   #Flake[7
          11
          "List if ledger signatures that signed this block (signature of _block/hash). Not included in block hash."
          -1
          true
          nil]
   #Flake[7 12 52776558133249 -1 true nil]
   #Flake[7 14 true -1 true nil]
   #Flake[6 10 "_block/number" -1 true nil]
   #Flake[6 11 "Block number for this block." -1 true nil]
   #Flake[6 12 52776558133258 -1 true nil]
   #Flake[6 13 true -1 true nil]
   #Flake[5 10 "_block/instant" -1 true nil]
   #Flake[5 11 "Instant this block was created, per the ledger." -1 true nil]
   #Flake[5 12 52776558133253 -1 true nil]
   #Flake[5 15 true -1 true nil]
   #Flake[4 10 "_block/ledgers" -1 true nil]
   #Flake[4
          11
          "Reference to ledger auth identities that signed this block. Not included in block hash."
          -1
          true
          nil]
   #Flake[4 12 52776558133250 -1 true nil]
   #Flake[4 14 true -1 true nil]
   #Flake[4 19 "_auth" -1 true nil]
   #Flake[3 10 "_block/transactions" -1 true nil]
   #Flake[3 11 "Reference to transactions included in this block." -1 true nil]
   #Flake[3 12 52776558133250 -1 true nil]
   #Flake[3 14 true -1 true nil]
   #Flake[3 19 "_tx" -1 true nil]
   #Flake[2 10 "_block/prevHash" -1 true nil]
   #Flake[2 11 "Previous block's hash" -1 true nil]
   #Flake[2 12 52776558133249 -1 true nil]
   #Flake[1 10 "_block/hash" -1 true nil]
   #Flake[1 11 "Merkle root of all _tx/hash in this block." -1 true nil]
   #Flake[1 12 52776558133249 -1 true nil]
   #Flake[-1 100 "401d6b58275788cddf451f139bc81c794091ac5c2bee35a56fd43320c232aebc" -1 true nil]
   #Flake[-1 103 1597383559372 -1 true nil]
   #Flake[-2 1 "b718b5fcebd9a0e587d456ab1409156dcc6ba431ad2fffd502b27c78da2cbfc7" -2 true nil]
   #Flake[-2 3 -2 -2 true nil]
   #Flake[-2 3 -1 -2 true nil]
   #Flake[-2 4 105553116266496 -2 true nil]
   #Flake[-2 5 1597383559372 -2 true nil]
   #Flake[-2 6 1 -2 true nil]])