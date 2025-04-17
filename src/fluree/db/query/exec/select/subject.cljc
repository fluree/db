(ns fluree.db.query.exec.select.subject
  (:require [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol SubjectFormatter
  (-forward-properties [db iri select-spec context compact-fn cache fuel-tracker error-ch])
  (-reverse-property [db iri reverse-spec context compact-fn cache fuel-tracker error-ch])
  (-iri-visible? [db fuel-tracker iri]))

(defn subject-formatter?
  [x]
  (satisfies? SubjectFormatter x))

(declare format-subject)

(defn append-id
  ([ds fuel-tracker iri compact-fn error-ch]
   (append-id ds fuel-tracker iri nil compact-fn error-ch nil))
  ([ds fuel-tracker iri {:keys [wildcard?] :as select-spec} compact-fn error-ch node-ch]
   (go
     (try*
       (let [node  (if (nil? node-ch)
                     {}
                     (<! node-ch))
             node* (if (or (nil? select-spec)
                           wildcard?
                           (contains? select-spec const/iri-id))
                     (if (<? (-iri-visible? ds fuel-tracker iri))
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
      (format-subject ds o-iri context compact-fn subselect cache (inc current-depth)
                      fuel-tracker error-ch)

      ;; requested graph crawl depth has not yet been reached
      (< current-depth max-depth)
      (format-subject ds o-iri context compact-fn select-spec cache (inc current-depth)
                      fuel-tracker error-ch)

      :else
      (append-id ds fuel-tracker o-iri compact-fn error-ch))))

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

(defn format-reverse-properties
  [ds iri reverse-map context compact-fn cache fuel-tracker error-ch]
  (let [out-ch (async/chan 32)]
    (async/pipeline-async 32
                          out-ch
                          (fn [reverse-spec ch]
                            (-> ds
                                (-reverse-property iri reverse-spec context compact-fn cache fuel-tracker error-ch)
                                (async/pipe ch)))
                          (async/to-chan! (vals reverse-map)))

    (async/reduce conj {} out-ch)))

(defn format-subject
  ([ds iri context compact-fn select-spec cache fuel-tracker error-ch]
   (format-subject ds iri context compact-fn select-spec cache 0 fuel-tracker error-ch))
  ([ds iri context compact-fn {:keys [reverse] :as select-spec} cache current-depth fuel-tracker error-ch]
   (let [forward-ch (-forward-properties ds iri select-spec context compact-fn cache fuel-tracker error-ch)
         subject-ch (if reverse
                      (let [reverse-ch (format-reverse-properties ds iri reverse context compact-fn cache fuel-tracker error-ch)]
                        (->> [forward-ch reverse-ch]
                             async/merge
                             (async/reduce merge {})))
                      forward-ch)]
     (->> subject-ch
          (resolve-references ds cache context compact-fn select-spec current-depth fuel-tracker error-ch)
          (append-id ds fuel-tracker iri select-spec compact-fn error-ch)))))
