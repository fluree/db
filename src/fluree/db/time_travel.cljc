(ns fluree.db.time-travel
  (:require [clojure.core.async :as async]
            [clojure.string :as string]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<? go-try into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

;; TODO - add duration support for javascript
(defn duration-parse
  "Given a duration, returns a ISO-8601 formatted time string of now minus duration"
  [time-str]
  #?(:clj  (let [now       (java.time.ZonedDateTime/now)
                 upper-str (string/upper-case time-str)
                 [date time] (string/split upper-str #"T")
                 minusTime (if (nil? time)
                             now
                             (let [time-str (java.time.Duration/parse (str "PT" time))]
                               (.minus now time-str)))
                 minusDate (if (= 1 (count date))
                             minusTime
                             (.minus minusTime (java.time.Period/parse date)))]
             (.toEpochMilli (.toInstant minusDate)))
     :cljs (throw (ex-info "Duration timeframes not yet supported in javascript."
                           {:status 400 :error :db/platform-support}))))

;; TODO - FC-462
;; TODO - with a large number of blocks, the below range query will be very inefficient
;; TODO - if we support a 'reverse index range' capability (FC-461), we could find the
;; TODO - requested time in :post index and work backwards, taking just a single result.
(defn- time-to-t
  [db time-str]
  (go-try
    (let [epoch-as-of (if (string? time-str) (util/str->epoch-ms time-str) time-str)
          ;; find the first 't' that is < epoch-as-of, then get the min t
          ts          (some->
                        (dbproto/-rootdb db)
                        (query-range/index-range :post
                                                 > ["_block/instant" 0]
                                                 < ["_block/instant" epoch-as-of])
                        (<?))
          _           (if (empty? ts)
                        (throw (ex-info (str "There is no data as of " epoch-as-of)
                                        {:status 400
                                         :error  :db/invalid-block})))
          t           (apply min-key #(flake/s %) ts)]
      (or (flake/s t) t (:t db)))))

(defn- t-to-block
  [db t]
  (go-try
    (let [block (some-> (dbproto/-rootdb db)
                        (query-range/index-range :psot >= ["_block/number" t] <= ["_block/number"] {:limit 1})
                        (<?)
                        (first)
                        (#(let [f %] (flake/o f))))]
      (if (> block 1)
        block 1))))

(defn non-border-t-to-block
  "Returns the block that any given 't' is contained within."
  [db t]
  (go-try (let [border-t (some-> (dbproto/-rootdb db)
                                 (query-range/index-range :opst = [t "_block/transactions"])
                                 (<?)
                                 (first)
                                 (#(let [f %] (flake/s f))))
                block    (<? (t-to-block db border-t))]
            (if (> block 1)
              block 1))))


(defn block-to-int-format
  "Returns the block for a given time as a string (ISO-8601 formatted time or a duration).
  If a block (positive integer) is provided, returns it unmodified."
  [db time-str]
  (go-try
    (let [block (cond
                  (pos-int? time-str)                       ;; assume a block number, don't modify
                  time-str

                  ; If string start with P - it's a duration
                  (and (string? time-str) (= "P" (str (get time-str 0))))
                  (let [parsed-time-str (duration-parse time-str)
                        t               (<? (time-to-t db parsed-time-str))]
                    (<? (t-to-block db t)))

                  (string? time-str)
                  (let [t (<? (time-to-t db time-str))]
                    (<? (t-to-block db t)))

                  :else
                  (throw (ex-info (str "Invalid block key provided: " (pr-str time-str))
                                  {:status 400
                                   :error  :db/invalid-time})))]
      block)))


(defn block-to-t
  "Given a positive integer block, returns the t (negative integer) associated.
  If block does not exist, throws."
  [db block]
  (go-try
    (let [block-t (some->
                    (dbproto/-rootdb db)
                    (query-range/index-range :post = ["_block/number" block])
                    (<?)
                    (first)
                    (#(let [f %] (flake/t f))))]
      (when-not block-t
        (throw (ex-info (str "Invalid block key provided: " (pr-str block))
                        {:status 400
                         :error  :db/invalid-time})))
      block-t)))

(defn to-t
  "Takes any time value: block, ISO-8601 time or duration string, or t
  and returns the exact 't' as of that value into a core async channel."
  [db block-or-t-or-time]
  (go-try
    (let [latest-db (<? (dbproto/-latest-db db))]
      (cond
        (pos-int? block-or-t-or-time)     ;; specified block
        (try*
          (<? (block-to-t latest-db block-or-t-or-time))
          ;; exception if block doesn't exist... use latest 't'
          (catch* _ (:t latest-db)))

        (neg-int? block-or-t-or-time)     ;; specified tx identifier
        block-or-t-or-time

        (string? block-or-t-or-time)      ;; ISO 8601-string
        (if (= "P" (str (get block-or-t-or-time 0))) ; If string start with P - it's a duration
          (<? (time-to-t latest-db (duration-parse block-or-t-or-time)))
          (<? (time-to-t latest-db block-or-t-or-time)))

        :else
        (throw (ex-info (str "Invalid time value provided: " (pr-str block-or-t-or-time))
                        {:status 400
                         :error  :db/invalid-time}))))))

(defn datetime->t
  "Takes an ISO-8601 datetime string and returns a core.async channel with the
  latest 't' value that is not more recent than that datetime."
  [db datetime]
  (go-try
    (log/debug "datetime->t db:" (pr-str db))
    (let [epoch-datetime (util/str->epoch-ms datetime)
          current-time #?(:clj (System/currentTimeMillis)
                          :cljs (js/Date.now))
          flakes (some-> db
                         dbproto/-rootdb
                         (query-range/index-range :post
                                                  >= [const/$_commit:time epoch-datetime]
                                                  <  [const/$_commit:time current-time])
                         <?)]
      (log/debug "datetime->t index-range:" (pr-str flakes))
      (if (empty? flakes)
        (:t db)
        (-> flakes first flake/t inc)))))

(defn as-of
  "Gets database as of a specific moment. Resolves 't' value provided to internal Fluree indexing
  negative 't' long integer value."
  [db t]
  (let [pc (async/promise-chan)]
    (async/go
      (try*
        (let [t* (cond
                   (string? t)  (<? (datetime->t db t)) ; ISO-8601 datetime
                   (pos-int? t) (- t)
                   (neg-int? t) t
                   :else (throw (ex-info (str "Time travel to t value of: " t " not yet supported.")
                                         {:status 400 :error :db/invalid-query})))]
          (log/debug "as-of t:" t*)
          (async/put! pc (assoc db :t t*)))
        (catch* e
          ;; return exception into promise-chan
          (async/put! pc e))))
    pc))


(defn as-of-block
  "Gets the database as-of a specified block. Either block number or a time string in ISO-8601 format.
  Returns db as a promise channel"
  [db block-or-t-or-time]
  (let [pc (async/promise-chan)]
    (async/go
      (try*
        (let [latest-db (<? (dbproto/-latest-db db))
              t         (<? (to-t latest-db block-or-t-or-time))
              block     (<? (t-to-block latest-db t))]
          (async/put! pc (assoc db :t t
                                   :block block)))
        (catch* e
                ;; return exception into promise-chan
                (async/put! pc e))))
    pc))
