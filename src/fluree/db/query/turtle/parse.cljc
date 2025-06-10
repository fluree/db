(ns fluree.db.query.turtle.parse
  (:require [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as exec.where]
            [quoll.raphael.core :as raphael]))

#?(:clj (set! *warn-on-reflection* true))

(defrecord Generator [namespaces]
  raphael/NodeGenerator
  (new-node [this]
    [this (exec.where/match-iri (iri/new-blank-node-id))])
  (new-node [this label]
    [this (exec.where/match-iri (str "_:" label))])
  (add-base [this iri]
    (update this :namespaces assoc :base (str iri)))
  (add-prefix [this prefix iri]
    (update this :namespaces assoc prefix (str iri)))
  (iri-for [_ prefix]
    (get namespaces prefix))
  (get-namespaces [_]
    (dissoc namespaces :base))
  (get-base [_]
    (:base namespaces))
  (new-qname [_ prefix local]
    (exec.where/match-iri (str (get namespaces prefix) local)))
  (new-iri [_ iri]
    iri)
  (new-literal [_ s]
    (-> exec.where/unmatched
        (exec.where/match-value s)))
  (new-literal [_ s t]
    (let [datatype (-> t ::exec.where/iri)]
      (-> exec.where/unmatched
          (exec.where/match-value s datatype))))
  (new-lang-string [_ s lang]
    (-> exec.where/unmatched
        (exec.where/match-lang s lang)))
  (rdf-type [_]
    (exec.where/match-iri const/iri-rdf-type))
  (rdf-first [_]
    (exec.where/match-iri const/iri-rdf-first))
  (rdf-rest [_]
    (exec.where/match-iri const/iri-rdf-rest))
  (rdf-nil [_]
    (exec.where/match-iri const/iri-rdf-nil)))

(defn parse
  [ttl]
  (let [gen (->Generator {})]
    (-> ttl
        (raphael/parse gen)
        :triples)))
