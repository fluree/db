(ns fluree.db.query.json-ld.response
  (:require [fluree.db.util.async :refer [<?]]
            [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.permissions-validate :as validate]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.query.dataset :as dataset]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.json-ld.iri :as iri]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol NodeFormatter
  (-forward-properties [db iri select-spec context compact-fn cache fuel-tracker error-ch])
  (-reverse-property [db iri reverse-spec compact-fn cache fuel-tracker error-ch]))

(defn cache-sid->iri
  [db cache compact-fn sid]
  (let [cache-key [(:alias db) sid]]
    (or (get @cache cache-key)
        (when-let [iri (or (some-> db :schema :pred (get sid) :iri compact-fn)
                           (some-> (iri/decode-sid db sid) compact-fn))]
          (vswap! cache assoc cache-key {:as iri})
          {:as iri}))))

(defn wildcard-spec
  [db cache compact-fn iri]
  (or (get @cache iri)
      (when-let [spec (get-in db [:schema :pred iri])]
        (let [spec* (assoc spec :as (compact-fn (:iri spec)))]
          (vswap! cache assoc iri spec*)
          spec*))))

(defn combine-objects
  [obj1 obj2]
  (if (sequential? obj1)
    (if (sequential? obj2)
      (into obj1 obj2)
      (conj obj1 obj2))
    (if (sequential? obj2)
      (into [obj1] obj2)
      [obj1 obj2])))

(defn merge-subgraphs
  [sg1 sg2]
  (merge-with combine-objects sg1 sg2))

(declare format-node)

(defn append-id
  ([ds iri cache compact-fn error-ch]
   (append-id ds iri nil cache compact-fn error-ch nil))
  ([ds iri {:keys [wildcard?] :as select-spec} cache compact-fn error-ch node-ch]
   (go
     (try*
       (let [node  (if (nil? node-ch)
                     {}
                     (<! node-ch))
             node* (if (or (nil? select-spec)
                           wildcard?
                           (contains? select-spec const/iri-id))
                     (if-let [allowing-db (loop [[db & r] (dataset/all ds)]
                                            (if db
                                              (let [sid (iri/encode-iri db iri)]
                                                (if (<? (validate/allow-iri? db sid))
                                                  db
                                                  (recur r)))
                                              nil))]
                       (let [;; TODO: we generate id-key here every time, this
                             ;; should be done in the :spec once beforehand and
                             ;; used from there
                             id-key (:as (or (wildcard-spec allowing-db cache compact-fn const/$id)
                                             (cache-sid->iri allowing-db cache compact-fn const/$id)))
                             iri*   (compact-fn iri)]
                         (assoc node id-key iri*))
                       node)
                     node)]
         (not-empty node*))
       (catch* e
               (log/error e "Error appending subject iri")
               (>! error-ch e))))))

(defn encode-reference
  [iri spec]
  {::reference {:iri  iri
                :spec spec}})

(defn reference?
  [v]
  (some? (::reference v)))

(defn display-reference
  [ds o-iri spec select-spec cache context compact-fn current-depth fuel-tracker error-ch]
  (let [max-depth (:depth select-spec)
        subselect (:spec spec)]
    (cond
      ;; have a specified sub-selection (graph crawl)
      subselect
      (format-node ds o-iri context compact-fn subselect cache (inc current-depth)
                   fuel-tracker error-ch)

      ;; requested graph crawl depth has not yet been reached
      (< current-depth max-depth)
      (format-node ds o-iri context compact-fn select-spec cache (inc current-depth)
                   fuel-tracker error-ch)

      :else
      (append-id ds o-iri cache compact-fn error-ch))))

(defn resolve-reference
  [ds cache context compact-fn select-spec current-depth fuel-tracker error-ch v]
  (let [{:keys [iri spec]} (::reference v)]
    (display-reference ds iri spec select-spec cache context compact-fn current-depth
                       fuel-tracker error-ch)))

(defn resolve-references
  [ds cache context compact-fn select-spec current-depth fuel-tracker error-ch attr-ch]
  (go (when-let [attrs (<! attr-ch)]
        (loop [[[prop v] & r] attrs
               resolved-attrs {}]
          (if prop
            (let [v' (if (sequential? v)
                       (loop [[value & r]     v
                              resolved-values []]
                         (if value
                           (if (reference? value)
                             (if-let [resolved (<! (resolve-reference ds cache context compact-fn select-spec current-depth fuel-tracker error-ch value))]
                               (recur r (conj resolved-values resolved))
                               (recur r resolved-values))
                             (recur r (conj resolved-values value)))
                           (not-empty resolved-values)))
                       (if (reference? v)
                         (<! (resolve-reference ds cache context compact-fn select-spec current-depth fuel-tracker error-ch v))
                         v))]
              (if (some? v')
                (recur r (assoc resolved-attrs prop v'))
                (recur r resolved-attrs)))
            resolved-attrs)))))

(defn format-reverse-property
  [ds o-iri reverse-spec compact-fn cache fuel-tracker error-ch]
  (if (dataset/dataset? ds)
    (let [db-ch   (->> ds dataset/all async/to-chan!)
          prop-ch (async/chan)]
      (async/pipeline-async 2
                            prop-ch
                            (fn [db ch]
                              (-> (-reverse-property db o-iri reverse-spec compact-fn cache fuel-tracker error-ch)
                                  (async/pipe ch)))
                            db-ch)
      (async/reduce (fn [combined-prop db-prop]
                      (let [[as results] combined-prop]
                        (if results
                          (let [[_as next-result] db-prop]
                            [as (combine-objects results next-result)])
                          db-prop)))
                    []
                    prop-ch))
    (-reverse-property ds o-iri reverse-spec compact-fn cache fuel-tracker error-ch)))

(defn format-reverse-properties
  [ds iri reverse-map compact-fn cache fuel-tracker error-ch]
  (let [out-ch (async/chan 32)]
    (async/pipeline-async 32
                          out-ch
                          (fn [reverse-spec ch]
                            (-> ds
                                (format-reverse-property iri reverse-spec compact-fn cache fuel-tracker error-ch)
                                (async/pipe ch)))
                          (async/to-chan! (vals reverse-map)))

    (async/reduce conj {} out-ch)))

(defn format-forward-properties
  [ds iri select-spec context compact-fn cache fuel-tracker error-ch]
  (if (dataset/dataset? ds)
    (let [db-ch   (->> ds dataset/all async/to-chan!)
          prop-ch (async/chan)]
      (async/pipeline-async 4
                            prop-ch
                            (fn [db ch]
                              (-> (-forward-properties db iri select-spec context compact-fn cache fuel-tracker error-ch)
                                  (async/pipe ch)))
                            db-ch)
      (async/reduce merge-subgraphs {} prop-ch))
    (-forward-properties ds iri select-spec context compact-fn cache fuel-tracker error-ch)))

(defn format-node
  ([ds iri context compact-fn select-spec cache fuel-tracker error-ch]
   (format-node ds iri context compact-fn select-spec cache 0 fuel-tracker error-ch))
  ([ds iri context compact-fn {:keys [reverse] :as select-spec} cache current-depth fuel-tracker error-ch]
   (let [forward-ch (format-forward-properties ds iri select-spec context compact-fn cache fuel-tracker error-ch)
         subject-ch (if reverse
                      (let [reverse-ch (format-reverse-properties ds iri reverse compact-fn cache fuel-tracker error-ch)]
                        (->> [forward-ch reverse-ch]
                             async/merge
                             (async/reduce merge {})))
                      forward-ch)]
     (->> subject-ch
          (resolve-references ds cache context compact-fn select-spec current-depth fuel-tracker error-ch)
          (append-id ds iri select-spec cache compact-fn error-ch)))))
