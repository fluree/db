(ns fluree.db.query.exec.select.subject
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.select.literal :as literal]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol SubjectFormatter
  (-forward-properties [db iri select-spec context compact-fn cache tracker error-ch])
  (-reverse-property [db iri reverse-spec context tracker error-ch])
  (-iri-visible? [db tracker iri]))

(defn subject-formatter?
  [x]
  (satisfies? SubjectFormatter x))

(declare format-subject)

(defn append-id
  ([ds tracker iri compact-fn error-ch]
   (append-id ds tracker iri nil compact-fn error-ch nil))
  ([ds tracker iri {:keys [wildcard?] :as select-spec} compact-fn error-ch node-ch]
   (go
     (try*
       (let [node  (if (nil? node-ch)
                     {}
                     (<! node-ch))
             node* (if (or (nil? select-spec)
                           wildcard?
                           (contains? select-spec const/iri-id))
                     (if (<? (-iri-visible? ds tracker iri))
                       (let [;; TODO: we generate id-key here every time, this
                             ;; should be done in the :spec once beforehand and
                             ;; used from there
                             id-key (compact-fn const/iri-id)
                             iri*   (compact-fn iri)]
                         (assoc node id-key iri*))
                       node)
                     node)]
         (not-empty node*))
       (catch* e
         (log/error! ::id-formatting-error e {:msg "Error appending subject iri"
                                              :iri iri})
         (log/error e "Error appending subject iri")
         (>! error-ch e))))))

(defn encode-reference
  [iri spec]
  {::reference {::iri  iri
                ::spec spec}})

(defn reference?
  [v]
  (some? (::reference v)))

(defn encode-literal
  [value datatype language spec]
  (let [attr-map (literal/attribute-map value datatype language)]
    {::literal (assoc attr-map ::spec spec)}))

(defn literal?
  [v]
  (some? (::literal v)))

(defn display-reference
  [ds o-iri spec select-spec cache context compact-fn current-depth tracker error-ch]
  (let [max-depth (:depth select-spec)
        subselect (:spec spec)]
    (cond
      ;; have a specified sub-selection (graph crawl)
      subselect
      (format-subject ds o-iri context compact-fn subselect cache (inc current-depth)
                      tracker error-ch)

      ;; requested graph crawl depth has not yet been reached
      (< current-depth max-depth)
      (format-subject ds o-iri context compact-fn select-spec cache (inc current-depth)
                      tracker error-ch)

      :else
      (append-id ds tracker o-iri compact-fn error-ch))))

(defn resolve-reference
  [ds cache context compact-fn select-spec current-depth tracker error-ch v]
  (let [{::keys [iri spec]} (::reference v)]
    (display-reference ds iri spec select-spec cache context compact-fn current-depth
                       tracker error-ch)))

(defn display-literal
  [attrs spec cache compact-fn]
  (let [subselect (:spec spec)]
    (if subselect
      (literal/format-literal attrs compact-fn subselect cache)
      (literal/get-value attrs))))

(defn resolve-literal
  [cache compact-fn v]
  (let [{::keys [spec] :as attrs} (::literal v)]
    (display-literal attrs spec cache compact-fn)))

(defn resolve-seq
  [ds cache context compact-fn select-spec current-depth tracker error-ch values]
  (go-loop [[value & r]     values
            resolved-values []]
    (if value
      (cond
        (reference? value)
        (if-let [resolved (<! (resolve-reference ds cache context compact-fn select-spec current-depth tracker error-ch value))]
          (recur r (conj resolved-values resolved))
          (recur r resolved-values))

        (literal? value)
        (recur r (conj resolved-values (resolve-literal cache compact-fn value)))

        :else value)
      (not-empty resolved-values))))

(defn resolve-rdf-type
  "Special handling for JSON-LD @type values to extract IRIs directly
   and bypass policy restriction."
  [compact-fn v]
  (if (sequential? v)
    (mapv (fn [value]
            (let [{::keys [iri]} (::reference value)]
              (compact-fn iri)))
          v)
    (let [{::keys [iri]} (::reference v)] (compact-fn iri))))

(defn resolve-properties
  [ds cache context compact-fn select-spec current-depth tracker error-ch attr-ch]
  (go (when-let [attrs (<! attr-ch)]
        (let [type-key (compact-fn const/iri-type)]
          (loop [[[prop v] & r] attrs
                 resolved-attrs {}]
            (if prop
              (let [v' (cond
                         (= prop type-key) ;; @type: skip policy resolution; extract IRIs directly
                         (resolve-rdf-type compact-fn v)

                         (sequential? v)
                         (<! (resolve-seq ds cache context compact-fn select-spec current-depth tracker error-ch v))

                         (reference? v)
                         (<! (resolve-reference ds cache context compact-fn select-spec current-depth tracker error-ch v))

                         (literal? v)
                         (resolve-literal cache compact-fn v)

                         :else v)]
                (if (some? v')
                  (recur r (assoc resolved-attrs prop v'))
                  (recur r resolved-attrs)))
              resolved-attrs))))))

(defn format-reverse-properties
  [ds iri reverse-map context tracker error-ch]
  (let [out-ch (async/chan 32)]
    (async/pipeline-async 32
                          out-ch
                          (fn [reverse-spec ch]
                            (-> ds
                                (-reverse-property iri reverse-spec context tracker error-ch)
                                (async/pipe ch)))
                          (async/to-chan! (vals reverse-map)))

    (async/reduce conj {} out-ch)))

(defn format-subject
  ([ds iri context compact-fn select-spec cache tracker error-ch]
   (format-subject ds iri context compact-fn select-spec cache 0 tracker error-ch))
  ([ds iri context compact-fn {:keys [reverse] :as select-spec} cache current-depth tracker error-ch]
   (let [forward-ch (-forward-properties ds iri select-spec context compact-fn cache tracker error-ch)
         subject-ch (if reverse
                      (let [reverse-ch (format-reverse-properties ds iri reverse context tracker error-ch)]
                        (->> [forward-ch reverse-ch]
                             async/merge
                             (async/reduce merge {})))
                      forward-ch)]
     (->> subject-ch
          (resolve-properties ds cache context compact-fn select-spec current-depth tracker error-ch)
          (append-id ds tracker iri select-spec compact-fn error-ch)))))
