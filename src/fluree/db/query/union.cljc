(ns fluree.db.query.union
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]))

(defn merge-vars
  "Unions act as 'or'. If they use different variables, their output
  needs to be unified - so we need to output 'nil' for any variable used
  in one statement but not the other.

  Here we identify different variables between the two union clauses,
  and add any vars that should output 'nil' into the *last* where clause
  of each union, placed into [:vars :nil]. We do not affect any other where
  clauses in the union (if there are more than one).

  We also merge together all the variables used across both union
  statements into a single [:vars :all].

  For reference, the :vars value of each last union statement will end up looking
  like this after this step:
  {:flake-in (?s),
   :flake-out [?s nil ?email1], ;; always as an [s p o] tuple, nil means the var need not be output
   :all {?s :s, ?email1 :o, ?email2 :o}, ;; <<< this will be a merge of both union1 & union2
   :others [],
   :nils #{?email2}} ;; <<< this will be the vars in the statement that should output nil vals
  "
  [union1 union2]
  (let [u1-last     (last union1)
        u2-last     (last union2)
        u1-all-vars (-> u1-last :vars :all)
        u2-all-vars (-> u2-last :vars :all)
        u1-vars-set (-> u1-all-vars keys set)
        u2-vars-set (-> u2-all-vars keys set)
        u1-unique   (reduce disj u1-vars-set u2-vars-set)
        u2-unique   (reduce disj u2-vars-set u1-vars-set)
        all-vars    (merge u1-all-vars u2-all-vars)
        u1-last*    (-> u1-last
                        (assoc-in [:vars :nils] u2-unique)
                        (assoc-in [:vars :all] all-vars))
        u2-last*    (-> u2-last
                        (assoc-in [:vars :nils] u1-unique)
                        (assoc-in [:vars :all] all-vars))]
    ;; replace updated 'last' where statement for each union clause
    [(-> union1 butlast vec (conj u1-last*))
     (-> union2 butlast vec (conj u2-last*))]))


(defn gen-nils-fn
  "When we need to output nil values related to a union statement
  we need a function to rearrange the output variables and inject nils into
  the correct locations.

  This isn't incredible efficient, but generally union statements will use the
  same output variables and therefore not have to sub in 'nil' values - so it
  is not expected this is a heavily used capability."
  [out-vars {:keys [nils] :as _vars}]
  (let [nils-pos (sort (map #(util/index-of out-vars %) nils))
        output-n (count out-vars)]
    (fn [out-vals]
      (mapv
        (fn [out-val]
          (loop [i         0
                 next-nil  (first nils-pos)
                 rest-nils (rest nils-pos)
                 out-val   out-val
                 acc       []]
            (if (< i output-n)
              (if (= i next-nil)
                (recur (inc i) (first rest-nils) (rest rest-nils)
                       out-val
                       (conj acc nil))
                (recur (inc i) next-nil rest-nils
                       (rest out-val)
                       (conj acc (first out-val))))
              acc)))
        out-vals))))


(defn order-out-vars
  "In the case of a :union, we want to ensure the final output variables for flakes for each
  union statement come before any passthrough variables.

  This helps ensure minimal output rearrangement when we inject nil values - we can
  still assume passthrough variables will all be at the end of the output and flake
  variables (for both unions) will be at the beginning of the variables.

  Nil values could be anywhere."
  [select-out-vars union-clause {:keys [parsed] :as _order-by}]
  (let [[union1 union2] (:where union-clause)
        {u1-flake-out :flake-out} (:vars (last union1))
        {u2-flake-out :flake-out, u2-others :others, u2-all :all} (:vars (last union2))
        _              (when-let [illegal-var (some #(when-not (contains? u2-all %) %) select-out-vars)]
                         (throw (ex-info (str "Variable " illegal-var " used in select statement but does not exist in the query.")
                                         {:status 400 :error :db/invalid-query})))
        order-by       (->> (map :variable parsed)
                            (remove nil?))
        out-vars-s     (into (set select-out-vars) order-by)
        u2-flake-vars  (filter out-vars-s u2-flake-out)     ;; only keep flake-out vars needed in final output
        u1-flake-vars  (filter out-vars-s u1-flake-out)
        ;; concatenate into union2's flake variables only union1 flake variables that are not duplicates
        all-flake-vars (concat u2-flake-vars (remove (set u2-flake-vars) u1-flake-vars))
        u2-others-vars (filter out-vars-s u2-others)]       ;; only keep other vars needed in final output
    (into [] (concat all-flake-vars u2-others-vars))))