(ns fluree.db.query.exec
  (:require [clojure.spec.alpha :as spec]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(def rdf-type-preds #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                      "a"
                      :a
                      "rdf:type"
                      :rdf/type
                      "@type"})

(defn rdf-type?
  [p]
  (contains? rdf-type-preds p))

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(defn query-fn?
  "Query function as positioned in a :where statement"
  [x]
  (and (string? x)
       (re-matches #"^#\(.+\)$" x)))

(def ^:const default-recursion-depth 100)

(defn recursion-predicate
  "A predicate that ends in a '+', or a '+' with some integer afterwards is a recursion
  predicate. e.g.: person/follows+3

  Returns a two-tuple of predicate followed by # of times to recur.

  If not a recursion predicate, returns nil."
  [predicate context]
  (when (or (string? predicate)
            (keyword? predicate))
    (when-let [[_ pred recur-n] (re-find #"(.+)\+(\d+)?$" (name predicate))]
      [(json-ld/expand (keyword (namespace predicate) pred)
                       context)
       (if recur-n
         (util/str->int recur-n)
         default-recursion-depth)])))

(defn pred-id-strict
  "Returns predicate ID for a given predicate, else will throw with an invalid
  predicate error."
  [db predicate]
  (or (dbproto/-p-prop db :id predicate)
      (throw (ex-info (str "Invalid predicate: " predicate)
                      {:status 400 :error :db/invalid-query}))))

(defn parse-subject
  [s context]
  (cond
    (util/pred-ident? s)
    {::ident s}

    (variable? s)
    {::var (symbol s)}

    (nil? s)
    nil

    context
    {::val (if (int? s)
             s
             (json-ld/expand-iri s context))}

    :else
    (if (not (int? s))
      (throw (ex-info (str "Subject values in where statement must be integer subject IDs or two-tuple identies. "
                           "Provided: " s ".")
                      {:status 400 :error :db/invalid-query}))
      {::val s})))

(defn parse-predicate
  [p db context]
  (cond
    (rdf-type? p)
    {::val const/$rdf:type}

    (= "@id" p)
    {::val const/$iri}

    (variable? p)
    {::var (symbol p)}

    (recursion-predicate p context)
    (let [[p-iri recur-n] (recursion-predicate p context)]
      {::val (pred-id-strict db p-iri)
       ::recur (or recur-n util/max-integer)}) ;; default recursion depth

    (and (string? p)
         (str/starts-with? p "fullText:"))
    {::full-text (->> (json-ld/expand-iri (subs p 9) context)
                      (pred-id-strict db))}

    :else
    {::val (->> (json-ld/expand-iri p context)
                (pred-id-strict db))}))

(defn parse-object
  [o context]
  (cond
    (util/pred-ident? o)
    {::ident o}

    (variable? o)
    {::var (symbol o)}

    (nil? o)
    nil

    context
    {::val (if (int? o)
             o
             (json-ld/expand-iri o context))}

    :else
    {::val o}))

(defn parse-tuple
  [[s p o] db context]
  [(parse-subject s context)
   (parse-predicate p db context)
   (parse-object o context)])

(defn idx-for
  [s p o]
  (cond
    s         :spot
    (and p o) :post
    p         :psot
    o         :opst
    :else     :spot))

(defn resolve-flake-range
  [{:keys [conn t] :as db} error-ch [s p o]]
  (let [idx         (idx-for s p o)
        idx-root    (get db idx)
        novelty     (get-in db [:novelty idx])
        start-flake (flake/create s p o nil nil nil util/min-integer)
        end-flake   (flake/create s p o nil nil nil util/max-integer)
        #_#_obj-filter  (some-> o :filter filter/extract-combined-filter)
        opts        (cond-> {:idx         idx
                             :from-t      t
                             :to-t        t
                             :start-test  >=
                             :start-flake start-flake
                             :end-test    <=
                             :end-flake   end-flake}
                      #_#_obj-filter (assoc :object-fn obj-filter))]
    (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)))

(defmulti constrain
  (fn [db constraint error-ch result]
    (if (map? constraint)
      (-> constraint keys first)
      :tuple)))

(defn with-values
  [tuple values]
  (mapv (fn [component]
          (if-let [variable (::var component)]
            (let [value (get values variable)]
              (cond-> component
                value (assoc ::val value)))
            component))
        tuple))

(defn unbound?
  [component]
  (and (::var component)
       (not (::val component))))

(defn bind-flake
  [result constraint flake]
  (let [[s p o] constraint]
    (cond-> result
      (unbound? s) (assoc (::var s) (flake/s flake))
      (unbound? p) (assoc (::var p) (flake/p flake))
      (unbound? o) (assoc (::var o) (flake/o flake)))))

(defmethod constrain :tuple
  [db constraint error-ch result]
  (let [flake-ch      (->> (with-values constraint result)
                           (mapv ::val)
                           (resolve-flake-range db error-ch))
        constraint-ch (async/chan 4 (comp cat
                                          (map (fn [flake]
                                                 (bind-flake result constraint flake)))))]
    (async/pipe flake-ch constraint-ch)))

(defn with-constraint
  [db constraint error-ch result-ch]
  (let [out-ch (async/chan 4)]
    (async/pipeline-async 4
                          out-ch
                          (fn [result ch]
                            (async/pipe (constrain db constraint error-ch result)
                                        ch))
                          result-ch)
    out-ch))

(def empty-result {})

(defn where
  [db context error-ch tuples]
  (let [initial-ch (async/to-chan! [empty-result])]
    (reduce (fn [result-ch tuple]
              (let [constraint (parse-tuple tuple db context)]
                (with-constraint db constraint error-ch result-ch)))
            initial-ch tuples)))

(defn select-values
  [result selectors]
  (reduce (fn [values selector]
            (log/info "selecting" selector "from" values "in" result)
            (conj values (get result selector)))
          [] selectors))

(defn select
  [selectors result-ch]
  (let [select-ch (async/chan 4 (map (fn [result]
                                       (select-values result selectors))))]
    (async/pipe result-ch select-ch)))

(defn query
  [db q]
  (let [error-ch    (async/chan)
        context     (json-ld/parse-context (get-in db [:schema :context])
                                           (or (:context q) (get q "@context")))
        constraints (:where q)
        selectors   (:select q)]
    (->> (where db context error-ch constraints)
         (select selectors)
         (async/reduce conj []))))
