(ns fluree.db.query.analytical-wikidata
  (:require
    [clojure.string :as str]
    [fluree.db.util.core :as util]
    [fluree.db.util.xhttp :as xhttp]
    [fluree.db.util.log :as log]
    #?(:clj  [clojure.core.async :as async]
       :cljs [cljs.core.async :as async])
    [fluree.db.util.async :refer [<? go-try merge-into?]]))

#?(:clj (set! *warn-on-reflection* true))

(defn variable? [form]
  (when (and (or (string? form) (symbol? form)) (= (first (name form)) \?))
    (symbol form)))

(defn replacementVars->ValuesSmt
  "Takes replacement vars, retrieves values from ctx, and puts into Wikidata VALUES statement,
  i.e. VALUES (?name ?countryName) {(\"Vincent van Gogh\" \"Kingdom of the Netherlands\") (\"Edvard Munch\" \"Norway\")}"
  [vars values]
  (let [value-groups    (map
                          (fn [value-group]
                            (let [stringified  (map #(str "\"" % "\"") value-group)
                                  joined-group (str/join " " stringified)]
                              (str " ( " joined-group " ) ")))
                          values)
        value-group-str (str/join " " value-groups)]
    (str "VALUES ( " (str/join " " vars) " ) { " value-group-str " } ")))

(defn get-next-wd-clauses
  [coll]
  (loop [[clause & r] coll
         res []]
    (if clause (if (= "$wd" (first clause))
                 (recur r (conj res clause))
                 res)
               res)))

(defn get-all-wd-clauses
  [coll]
  (loop [[clause & r] coll
         res []]
    (if clause
      (if (= "$wd" (first clause))
        (recur r (conj res (rest clause)))
        (recur r res))
      res)))

(defn drop-all-wd-clauses
  [coll]
  (loop [[clause & r] coll
         res []]
    (if clause
      (if (= "$wd" (first clause))
        (recur r res)
        (recur r (conj res clause)))
      res)))

(defn ad-hoc-clause-to-wikidata
  [clause optional?]
  (cond->> clause
           (= "$wd" (first clause)) (drop 1)
           true                     (str/join " ")
           true                     (#(str % " ."))
           optional?                (#(str "OPTIONAL {" % "}"))))

(defn parse-prefixes
  [prefixes]
  (reduce (fn [acc-str prefix]
            (let [pfx    (-> (key prefix)
                             util/keyword->str
                             (str ":")
                             symbol)
                  source (->> (val prefix)
                              symbol)]
              (str acc-str " PREFIX " pfx " " source " \n ")))
          "" prefixes))

(defn generateWikiDataQuery
  [q-map clauses select-vars value-clause optional-clauses]
  (let [opts         (merge {:limit 100 :offset 0 :distinct false :language "en"}
                            (:wikidataOpts q-map) (get-in [:opts :wikidataOpts] q-map))
        {:keys [limit offset distinct language prefixes]} opts
        prefixes     (when prefixes (parse-prefixes prefixes))
        select-smt   (str "SELECT " (if distinct "DISTINCT ") " " (str/join " " (map #(str % "Label") select-vars)) " " (str/join " " select-vars))
        where-smt    (->> (mapv #(ad-hoc-clause-to-wikidata % false) clauses)
                          (str/join " "))
        optional-smt (->> (mapv #(ad-hoc-clause-to-wikidata % true) optional-clauses)
                          (str/join " "))
        serviceLabel (str "SERVICE wikibase:label { bd:serviceParam wikibase:language \"" (or language "en") "\" . }")
        full-query   (str prefixes " " select-smt " WHERE { " value-clause " "
                          where-smt " " optional-smt " " serviceLabel " } " (if limit (str "
                          LIMIT " limit)) " OFFSET " offset)] full-query)) >

(def wikidataURL "https://query.wikidata.org/bigdata/namespace/wdq/sparql?format=json&query=")

(defn submit-wikidata-query
  [query]
  (async/go
    (let [url     (str wikidataURL (util/url-encode query))
          headers {"User-Agent"
                   ;(if (System/getProperty "java.version")
                   ;      (str "Java/" (System/getProperty "java.version"))
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
                   ;)
                   "Accept" "application/sparql-results+json"}]
      (<? (xhttp/get url {:headers         headers
                          :request-timeout 30000
                          :output-format   :wikidata})))))

(defn submit+parse-wikidata-query
  [query]
  (async/go
    (let [body (<? (submit-wikidata-query query))]
      (if (not= 200 (:status body))
        body
        (->> (second (first (second (second body))))
             (mapv (fn [item]
                     (reduce
                       (fn [acc k-v]
                         (assoc acc (key k-v) (:value (val k-v))))
                       {} item))))))))

(defn wikiDataResp->tuples
  [wikidataRes vars]
  (let [labelVars      (map #(-> (str % "Label") symbol) vars)
        headers        (concat (into [] vars) labelVars)
        headers-as-kws (map #(-> (subs (str %) 1) util/str->keyword) headers)
        results        (:bindings (:results wikidataRes))
        tuples         (map (fn [res]
                              (map #(:value (% res)) headers-as-kws)) results)]
    {:headers headers
     :vars    {}
     :tuples  (into #{} tuples)}))


(defn get-wikidata-tuples
  [q-map clauses matching-vars matching-vals all-vars optional-clauses]
  (go-try
    (let [value-smt      (when-not (empty? matching-vars)
                           (replacementVars->ValuesSmt matching-vars matching-vals))
          wikidata-query (generateWikiDataQuery q-map clauses all-vars value-smt optional-clauses)
          {:keys [status message] :as wikidata-res} (<? (submit+parse-wikidata-query wikidata-query))]
      (if (= 400 status)
        (let [msg-len (count message)]
          (throw (ex-info (subs (:message wikidata-res)
                                0
                                (if (< msg-len 1000) msg-len 1000))
                          {:status status
                           :error  (:error wikidata-res)})))
        (wikiDataResp->tuples wikidata-res all-vars)))))