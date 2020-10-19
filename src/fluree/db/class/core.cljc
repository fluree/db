(ns fluree.db.class.core
  (:require [fluree.db.query.fql :as fql]
            ;[fluree.db.api :as fdb]
            [fluree.db.util.async :refer [go-try <?]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])))

;; functions for dealing with classes.


(defn modify-class-hierarchy
  "When we modify a class' place within the class hierarchy, it will trigger modifying existing
  flakes"
  []

  )


(defn add-class-flakes
  "Given a specified and optional classes, returns a set of flakes for all of this member's classes."
  [conn ledger]

  )


(defn parent->children
  "Provided a parent class id, finds all children (subclassOf parent class id),
  and updates existing class map or creates new class map for each child."
  [parent-class-id class-tuples class-map]
  (let [children   (filter #(= parent-class-id (nth % 2)) class-tuples)
        pcoll-id   (get-in class-map [parent-class-id :coll])
        pcoll-name (get-in class-map [parent-class-id :coll-name])
        class-map* (reduce
                     (fn [acc ctuple]
                       (let [[cid cname _ child-coll-id child-coll-name] ctuple
                             parents    (get-in acc [cid :parents] #{})
                             coll*      (or child-coll-id pcoll-id (get-in acc [cid :coll]))
                             coll-name* (or child-coll-name pcoll-name (get-in acc [cid :coll-name]))]
                         (assoc acc cid {:name      cname
                                         :coll      coll*
                                         :coll-name coll-name*
                                         :parents   (if (nil? parent-class-id) ;; top level parents
                                                      parents
                                                      (conj parents parent-class-id))})))
                     class-map children)]
    [class-map* children]))


(defn classes->collections
  "Returns a map of all classes mapped to their respective collections."
  [db]
  (go-try
    (let [class-query {:select ["?cid" "?cname" "?subclassOf" "?coll-id" "?coll-name"]
                       :where  [["?cid" "_class/name" "?cname"]
                                {:optional [["?cid" "_class/subclassOf" "?subclassOf"]
                                            ["?cid" "_class/collection" "?coll-id"]
                                            ["?coll-id" "_collection/name" "?coll-name"]]}]
                       :opts   {:cache true}}
          qresult     (<? (fql/query db class-query))
          ;; initial call with 'nil' parent class id will return top-level classes
          [class-map top-children] (parent->children nil qresult {})]
      ;; loop does one level of hierarchy at a time, starting with parents, then all parents' children, etc.
      (loop [[n & r] top-children
             children nil
             acc      class-map]
        (if (nil? n)
          acc
          (let [parent-class-id (first n)
                ;; returns updated class map and all children
                [acc* n-children] (parent->children parent-class-id qresult acc)
                ;; combine all children, they will become the parents once the current parent level is complete
                children*       (concat children n-children)]
            (if (empty? r)
              (recur (sort-by first children*) nil acc*)
              (recur r children* acc*))))))))


(defn sub-classes
  "Given a class name and a db, returns a core async channel with a list of all sub-classes."
  [db class-name]
  (fql/query db {:select "?cname"
                 :where  [["?sc" "_class/subclassOf+" "?c"]
                          ["?c" "_class/name" class-name]
                          ["?sc" "_class/name" "?cname"]]
                 :opts   {:cache true}}))


(defn parent-classes
  "Given a class name and a db, returns a core async channel with a list of all parent classes."
  [db class-name]
  (fql/query db {:select "?cname"
                 :where  [["?c" "_class/name" class-name]
                          ["?c" "_class/subclassOf+" "?pc"]
                          ["?pc" "_class/name" "?cname"]]}))


(defn collection-for-class
  "Given a connection, ledger and class name, returns the collection for the provided class.
  Always uses latest version of the DB."
  [conn ledger class-name]
  (go-try
    #_(let [db (<? (fdb/db conn ledger))
          class-map (<? (classes->collections db))]
      (some #(when (= (:name %) class-name) %) (vals class-map)))))






(comment

  (def conn (-> user/system
                :conn))

  @(fluree.db.api/ledger-list conn)
  (def ledger :bp/t3)
  (def db (fluree.db.api/db conn ledger))

  (time (async/<!! (sub-classes (async/<!! db) "Person")))
  (time (async/<!! (parent-classes (async/<!! db) "FullTimeEmployee")))
  (time (async/<!! (classes->collections (async/<!! db))))
  (time (async/<!! (collection-for-class conn ledger "TestSub1")))


  @(fluree.db.api/query db {:select ["?child"]
                            :where  [["?child" "_class/subclassOf" ["_class/name" "Person"]]]
                            :union  [["?sub-child" "_class/subclassOf" ["_class/name" "Person"]]
                                     ["?child" "_class/subclassOf" "?sub-child"]]})

  @(fluree.db.api/query db {:select ["?child"]
                            :where  [["?child" "_class/subclassOf" ["_class/name" "Person"]]]
                            :union  [
                                     [["?sub-child" "_class/subclassOf" ["_class/name" "Person"]]]
                                     [["?child" "_class/subclassOf" "?sub-child"]]
                                     ]
                            })

  @(fluree.db.api/query db {:select ["?child"]
                            :where  [["?child" "_class/subclassOf+5" ["_class/name" "Person"]]]
                            })


  @(fluree.db.api/query db {:select ["_class/name" {"_class/_subclassOf" ["_class/name" {"_class/_subclassOf" ["*"]}]}]
                            :from   ["_class/name" "Person"]
                            })

  @(fluree.db.api/query db {:select ["_class/name" {"_class/_subclassOf" {"_class/name" nil
                                                                          "_recur"      100}}]
                            :from   ["_class/name" "Person"]
                            })


  @(fluree.db.api/query db {:select ["?x"]
                            :where  [["?x" "_collection/name" "?y"]]
                            :union  [
                                     ["?x" "_predicate/name" "?y"]
                                     ]
                            })


  (async/<!! (find-sub-classes (async/<!! db) "Person"))

  )

