(ns fluree.db.class.core
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log]))


(defprotocol IClass
  (-flakes [this] [this class] "Returns all class flakes")
  (-subclass-flakes [this] "Returns all subclass flakes")
  (-sid [this class] "Returns subject id of a class")
  (-subclasses [this class] "Returns all subclasses of given class in order of hierarchy level")
  (-superclasses [this class] "Returns all parents of given class")
  (-collection [this class] "Returns collection specified class should be stored in"))


(defn- class-crawl
  "Return subject ids of next level of subclasses (immediate children)
  or superclasses (immediate parents) if super? is true.
  Returns 'nil' if none exist"
  [class-sid super? subclass-flakes]
  (let [xf (if super?                                       ;;  super/sub flip .-s and .-o
             (comp (filter #(= class-sid (.-s %)))          ;; filter for flakes with same class-sid
                   (map #(.-o %)))                          ;; extract class-sid of matches
             (comp (filter #(= class-sid (.-o %)))
                   (map #(.-s %))))]
    (sequence xf subclass-flakes)))


(defn- class-levels
  "Returns map of subclasses or superclasses (when super? = true)
  with the super/subclass subject ids as the key and the level (distance)
  of each subject id as the value. Level starts at 1, and increments for
  every child's children (or parent's parent) relationship.

  If the same parent/child exists along multiple paths,
  level will reflect the shortest path.

  Returns nil when no parent/children exist."
  ([class-sid cache super? subclass-flakes]
   (class-levels class-sid cache super? subclass-flakes 1))
  ([class-sid cache super? subclass-flakes level]
   (let [l1-subs (->> subclass-flakes
                      (class-crawl class-sid super?)
                      (reduce #(assoc %1 %2 level) {})
                      (not-empty))
         l2+subs (some->> l1-subs
                          keys
                          (map #(class-levels %1 cache super? subclass-flakes (inc level))))]
     (apply merge-with min l1-subs l2+subs))))


(defn- subclass-flake-filter
  [class-flakes]
  (filter #(= const/$rdfs:subClassOf (.-p %)) class-flakes))


(defn- class-map-sort
  "Sorting function for class maps, used by sort-class-map."
  [a b]
  (let [[class-a level-a] a
        [class-b level-b] b
        level-cmp (compare level-a level-b)]
    (if (= 0 level-cmp)
      (compare class-a class-b)
      level-cmp)))


(defn- superclasses
  "Returns list of superclasses of class-sid in order of:
    - lowest level (closest parent/child), then if equal:
    - class id (sid) - lowest first (defined by sort-class-map)"
  [class-sid cache class-flakes]
  (->> class-flakes
       subclass-flake-filter
       (class-levels class-sid cache true)
       (sort class-map-sort)
       (map first)))


(defn- collection
  "Returns collection of a class if it exists directly on the class, else nil."
  [class-flakes class-sid]
  (->> class-flakes
       (some #(when (and (= (.-s %) class-sid)
                         (= (.-p %) const/$fluree:partition))
                (.-o %)))))


(defn- nearest-collection
  "Returns nearest collection to class-sid. If class-sid does not
  directly have a named collection, it crawls superclasses until it
  finds the nearest one as defined by sort-class-map."
  [class-sid cache class-flakes]
  (or (collection class-flakes class-sid)
      (->> (superclasses class-sid cache class-flakes)
           (some #(collection class-flakes %)))))


(defrecord RdfClasses [class-flakes cache-atom]
  IClass
  (-flakes [_] class-flakes)
  (-flakes [this class]
    (let [sid (-sid this class)]
      (filter #(= sid (.-s %)) class-flakes)))
  (-subclass-flakes [_] (subclass-flake-filter class-flakes))
  (-sid [_ class]
    (some #(when (= (.-o %) class) (.-s %)) class-flakes))
  (-subclasses [this class]
    (->> (-subclass-flakes this)
         (class-levels (-sid this class) cache-atom false)))
  (-superclasses [this class]
    (->> (-subclass-flakes this)
         (class-levels (-sid this class) cache-atom true)))
  (-collection [this class]
    (nearest-collection (-sid this class) cache-atom class-flakes)))


(comment


  (def sample-flakes
    [(flake/->Flake :person const/$rdf:iri "ex:Person" -1 true nil) ;; class id 1
     (flake/->Flake :person const/$fluree:partition :part-person -1 true nil)
     (flake/->Flake :med-professional const/$rdf:iri "ex:MedicalProfessional" -1 true nil)
     (flake/->Flake :med-professional const/$rdfs:subClassOf :person -1 true nil)
     (flake/->Flake :doctor const/$rdf:iri "ex:Doctor" -1 true nil)
     (flake/->Flake :doctor const/$rdfs:subClassOf :med-professional -1 true nil)

     (flake/->Flake :student const/$rdf:iri "ex:Student" -1 true nil)
     (flake/->Flake :student const/$rdfs:subClassOf :person -1 true nil)
     (flake/->Flake :student const/$fluree:partition :part-student -1 true nil)
     (flake/->Flake :college-student const/$rdf:iri "ex:CollegeStudent" -1 true nil)
     (flake/->Flake :college-student const/$rdfs:subClassOf :student -1 true nil)
     (flake/->Flake :college-student const/$fluree:partition :part-college-student -1 true nil)
     (flake/->Flake :med-student const/$rdf:iri "ex:MedStudent" -1 true nil)
     (flake/->Flake :med-student const/$rdfs:subClassOf :college-student -1 true nil)
     (flake/->Flake :med-student const/$rdfs:subClassOf :med-professional -1 true nil)
     (flake/->Flake :resident const/$rdf:iri "ex:Resident" -1 true nil)
     (flake/->Flake :resident const/$rdfs:subClassOf :med-student -1 true nil)
     ])

  (def classes (map->RdfClasses {:class-flakes sample-flakes
                                 :cache-atom   (atom nil)}))


  (-flakes classes)
  (-subclass-flakes classes)
  (-flakes classes "ex:Person")
  (-flakes classes "ex:Doctor")
  (-superclasses classes "ex:Person")
  (-superclasses classes "ex:MedicalProfessional")
  (-superclasses classes "ex:Doctor")
  (-superclasses classes "ex:CollegeStudent")
  (-superclasses classes "ex:MedStudent")
  (-superclasses classes "ex:Resident")

  (-subclasses classes "ex:Person")
  (-subclasses classes "ex:Student")
  (-subclasses classes "ex:MedStudent")
  (-subclasses classes "ex:Resident")

  (-collection classes "ex:Student")
  (-collection classes "ex:Resident")
  (-collection classes "ex:Doctor")

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
                                     [["?child" "_class/subclassOf" "?sub-child"]]]})



  @(fluree.db.api/query db {:select ["?child"]
                            :where  [["?child" "_class/subclassOf+5" ["_class/name" "Person"]]]})



  @(fluree.db.api/query db {:select ["_class/name" {"_class/_subclassOf" ["_class/name" {"_class/_subclassOf" ["*"]}]}]
                            :from   ["_class/name" "Person"]})


  @(fluree.db.api/query db {:select ["_class/name" {"_class/_subclassOf" {"_class/name" nil
                                                                          "_recur"      100}}]
                            :from   ["_class/name" "Person"]})



  @(fluree.db.api/query db {:select ["?x"]
                            :where  [["?x" "_collection/name" "?y"]]
                            :union  [
                                     ["?x" "_predicate/name" "?y"]]})




  (async/<!! (find-sub-classes (async/<!! db) "Person")))




