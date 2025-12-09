(ns fluree.db.json-ld.policy.enforce
  (:require [clojure.core.async :refer [go]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy :refer [root]]
            [fluree.db.track :as track]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.parse :as util.parse]))

#?(:clj (set! *warn-on-reflection* true))

(defn unrestricted-modify?
  [policy]
  (true? (get-in policy [:modify :root?])))

(defn unrestricted-view?
  [policy]
  (true? (get-in policy [:view :root?])))

(defn modify-property-policy-map
  [policy]
  (get-in policy [:modify :property]))

(defn view-property-policy-map
  [policy]
  (get-in policy [:view :property]))

(defn modify-policies-for-property
  [policy-map property]
  (let [prop-policies (modify-property-policy-map policy-map)]
    (get prop-policies property)))

(defn view-policies-for-property
  [policy-map property]
  (let [prop-policies (view-property-policy-map policy-map)]
    (get prop-policies property)))

(defn view-subject-policy-map
  [policy]
  (get-in policy [:view :subject]))

(defn modify-subject-policy-map
  [policy]
  (get-in policy [:modify :subject]))

(defn view-policies-for-subject
  "O(1) lookup for subject-targeted view policies."
  [policy-map subject]
  (let [subj-policies (view-subject-policy-map policy-map)]
    (get subj-policies subject)))

(defn modify-policies-for-subject
  "O(1) lookup for subject-targeted modify policies."
  [policy-map subject]
  (let [subj-policies (modify-subject-policy-map policy-map)]
    (get subj-policies subject)))

(defn default-view-policies
  [policy-map]
  (get-in policy-map [:view :default]))

(defn default-modify-policies
  [policy-map]
  (get-in policy-map [:modify :default]))

(defn applies-to-flake?
  [{:keys [s-targets p-targets default?] :as _policy} [s p _o :as _flake]]
  (or (and (or (nil? s-targets) (contains? s-targets s))
           (or (nil? p-targets) (contains? p-targets p)))
      default?))

(defn- get-subject-classes
  "Gets subject's class IDs, using cache if available.
   Cache is an atom at (:cache (:policy db)) or passed as class-cache parameter."
  [db tracker class-cache sid]
  (go-try
    (or (get @class-cache sid)
        (let [classes (<? (dbproto/-class-ids db tracker sid))]
          (swap! class-cache assoc sid classes)
          classes))))

(defn- policy-applies-to-subject?
  "Checks if a policy applies to a subject.
   Only class policies with :class-check-needed? true require verification
   that the subject is an instance of one of the target classes."
  [subject-classes {:keys [class-policy? class-check-needed? for-classes] :as _policy}]
  (if (and class-policy? class-check-needed?)
    (some (set subject-classes) for-classes)
    true))

(defn view-policies-for-flake
  [{:keys [policy] :as _db} flake]
  (filter (fn [policy]
            (applies-to-flake? policy flake))
          (default-view-policies policy)))

(defn modify-policies-for-flake
  [{:keys [policy] :as _db} flake]
  (filter (fn [policy]
            (applies-to-flake? policy flake))
          (default-modify-policies policy)))

(defn policy-query
  [db sid query]
  (let [policy-values (-> db :policy :policy-values)
        this-val      (iri/decode-sid db sid)
        values        (-> (util.parse/normalize-values policy-values)
                          (policy/inject-value-binding "?$this" {"@value" this-val "@type" const/iri-id}))]
    (policy/inject-where-pattern query ["values" values])))

(def ^:const deny-query-result false)

(defn- needs-class-lookup?
  "Returns true if any policies require class membership lookup.
   Only class policies with :class-check-needed? true require lookup.
   Class policies on exclusive properties (only used by target classes)
   don't need lookup since we know the subject must be of the target class."
  [policies]
  (some (fn [{:keys [class-policy? class-check-needed?]}]
          (and class-policy? class-check-needed?))
        policies))

(defn- filter-applicable-policies
  "Filters policies to only those that apply to the subject.
   For class policies with :class-check-needed? true, checks class membership."
  [db tracker class-cache sid policies]
  (go-try
    (if (needs-class-lookup? policies)
      (let [subject-classes (<? (get-subject-classes db tracker class-cache sid))]
        (filter #(policy-applies-to-subject? subject-classes %) policies))
      policies)))

(defn- evaluate-policies
  "Evaluates a list of policies, returning first truthy result or false if all deny."
  [db tracker sid policies]
  (go-try
    (loop [[{:keys [id query allow?] :as policy} & r] policies]
      (if policy
        (let [result (cond
                       (true? allow?) true
                       (false? allow?) false
                       query (seq (<? (dbproto/-query (root db) tracker (policy-query db sid query))))
                       :else deny-query-result)]
          (track/policy-exec! tracker id)
          (if result
            (do (track/policy-allow! tracker id)
                true)
            (recur r)))
        false))))

(defn policies-allow-viewing?
  "Evaluates view policies for a subject. Handles class filtering and required
   policy selection internally. Returns immediately if no policies to check."
  [db tracker sid candidate-policies]
  (if (empty? candidate-policies)
    (go (get-in db [:policy :default-allow?] false))
    (let [class-cache (get-in db [:policy :cache])]
      (go-try
        (let [applicable (<? (filter-applicable-policies db tracker class-cache sid candidate-policies))
              to-eval (if-some [required (not-empty (filter :required? applicable))]
                        required
                        applicable)]
          (if (empty? to-eval)
            (get-in db [:policy :default-allow?] false)
            (<? (evaluate-policies db tracker sid to-eval))))))))

(defn policies-allow-modification?
  "Evaluates modify policies for a subject. Handles class filtering and required
   policy selection internally. Returns immediately if no policies to check."
  [db tracker class-cache sid candidate-policies]
  (if (empty? candidate-policies)
    (go (get-in db [:policy :default-allow?] false))
    (go-try
      (let [applicable (<? (filter-applicable-policies db tracker class-cache sid candidate-policies))
            to-eval (if-some [required (not-empty (filter :required? applicable))]
                      required
                      applicable)]
        (if (empty? to-eval)
          (get-in db [:policy :default-allow?] false)
          (or (<? (evaluate-policies db tracker sid to-eval))
              (ex-info (or (some :ex-message to-eval)
                           "Policy enforcement prevents modification.")
                       {:status 403 :error :db/policy-exception})))))))
