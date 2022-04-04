(ns fluree.db.spec
  (:require [fluree.db.util.json :as json]
            [alphabase.core :as alphabase]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [clojure.string :as str]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:private EMAIL #"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")

(defn safe-name
  [x]
  (try*
    (name x)
    (catch* e
            (str x))))

(defn type-check
  "(type-check type object) transforms an object to match the type. If it
  cannot be transformed, it throws an ex-info with a map from paths into the
  object to errors encountered at those paths."
  ([object spec]
   (let [errors    (atom {})
         conformed (type-check object spec [] errors)
         error-seq (reduce-kv (fn [e k v] (conj e (assoc v :path k))) [] @errors)]
     (if (empty? error-seq)
       conformed
       (throw
         (ex-info (str "Could not conform object: " (pr-str object) " to type " (pr-str spec))
                  {:status 400
                   :body   {:message (apply print-str "Validation error."
                                            (mapv
                                              #(str (:message %) " at " (mapv safe-name (:path %))
                                                    ". Expected " (safe-name (:spec %))
                                                    " but got \"" (:object %) "\".")
                                              error-seq))
                            :errors  error-seq}})))))
  ([object spec path errors-atom]
   (try*
     (let [error (fn [& [message]] (throw (ex-info (or message "Invalid object") {})))
           spec  (if (keyword? spec) (name spec) spec)]
       (cond
         (nil? spec)
         (if (nil? object) object (error))

         (string? spec)
         (let [optional  (str/ends-with? spec "?")
               base-spec (if optional
                           (subs spec 0 (dec (count spec)))
                           spec)]
           (cond
             (and optional (nil? object))
             nil

             (= base-spec "any")
             object

             (= base-spec "boolean")
             (if (boolean? object) object (error))

             (= base-spec "int")
             (int object)

             ;; longs can exceed what JavaScript/JSON supports, so we allow them to come over as a string.
             (= base-spec "long")
             (cond
               (number? object)
               (long object)

               (string? object)
               #?(:clj  (Long/parseLong object)
                  :cljs (let [i (js/parseInt object)]
                          (if (<= util/min-long i util/max-long)
                            i
                            (error (str "Long " object " is outside of javascript max integer size of 2^53 - 1.")))))

               :else
               (error))

             ; Is this what we want?
             (= base-spec "bigint")
             #?(:clj  (bigint object)
                :cljs (let [i (if (string? object)
                                (js/parseInt object)
                                object)]
                        (if (and (number? i)
                                 (<= util/min-long i util/max-long))
                          i
                          (error (str "Bigintegers are not supported in javascript. max integer size of 2^53 - 1, provided: " object)))))


             (= base-spec "float")
             (cond
               (number? object)
               (float object)

               (string? object)
               #?(:clj  (Float/parseFloat object)
                  :cljs (js/parseFloat object))

               :else
               (error))

             ;; Doubles can exceed what JavaScript/JSON supports, so we allow them to come over as a string.
             (= base-spec "double")
             (cond
               (number? object)
               (double object)

               (string? object)
               #?(:clj  (Double/parseDouble object)
                  :cljs (js/parseFloat object))

               :else
               (error))

             ; bigDec to string
             (= base-spec "bigdec")
             #?(:clj  (bigdec object)
                :cljs (error (str "Javascript does not support big decimals. Provided: " object)))

             (= base-spec "string")
             (cond
               (keyword? object) (subs (str object) 1)
               :else (str object))

             (= base-spec "bytes")
             (cond
               (string? object) (let [uc (.toLowerCase ^String object)]
                                  (if (re-matches #"^[0-9a-f]+$" uc)
                                    uc
                                    (error "Bytes type must be in hex string form.")))
               #?@(:clj  [(bytes? object) (alphabase/bytes->hex object)]
                   :cljs [(sequential? (js->clj object)) (alphabase/bytes->hex object)])

               :else (error))

             (= base-spec "instant")
             (try*
               (util/date->millis object)
               (catch* e
                       (error)))

             ; URI to string
             (= base-spec "uri")
             (str object)

             (= base-spec "email")
             (if (and (string? object) (re-find EMAIL object)) object (error))

             (or (= base-spec "tag") (= base-spec "ref"))
             (long object)

             ;UUID to string
             (= base-spec "uuid")
             (cond
               (string? object)
               object

               (uuid? object)
               (str object)

               :else
               (error))

             (= base-spec "json")
             (try*
               (if (string? object)                         ;;confirm parsable
                 (do (json/parse object)
                     object)
                 ;; try to convert to json
                 (json/stringify object))
               (catch* _ (error)))

             (= base-spec "geojson")
             (try*
               (let [parsed (if (string? object)
                              (json/parse object)
                              object)]
                 (if (json/valid-geojson? parsed)
                   (if (string? object)
                     object
                     (json/stringify object))
                   (error)))
               (catch* _ (error)))

             :else
             (error (str "Unknown base spec " base-spec))))

         :else
         (error (str "Unknown type " spec))))

     (catch* e
             (swap! errors-atom assoc path
                    {:message #?(:clj (.getMessage e) :cljs (str e))
                     :spec    spec
                     :object  object})
             object))))

