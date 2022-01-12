(ns fluree.db.json-ld.flakes
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

;; designed to take a JSON-LD document and turn it into a list of flakes

(def predefined-properties
  {"http://www.w3.org/2000/01/rdf-schema#Class"          const/$rdfs:Class
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" const/$rdf:Property
   "http://www.w3.org/2002/07/owl#Class"                 const/$owl:Class
   "http://www.w3.org/2002/07/owl#ObjectProperty"        const/$owl:ObjectProperty
   "http://www.w3.org/2002/07/owl#DatatypeProperty"      const/$owl:DatatypeProperty})

(def class+property-iris (into #{} (keys predefined-properties)))

(defn class-or-property?
  [{:keys [type] :as node}]
  (some class+property-iris type))


(defn json-ld-type-data
  "Returns two-tuple of [class-subject-ids class-flakes]
  where class-flakes will only contain newly generated class
  flakes if they didn't already exist."
  [class-iris t iris next-pid]
  (loop [[class-iri & r] class-iris
         class-sids   []
         class-flakes []]
    (if class-iri
      (if-let [existing (get @iris class-iri)]
        (recur r (conj class-sids existing) class-flakes)
        (let [type-sid (if-let [predefined-pid (get predefined-properties class-iri)]
                         predefined-pid
                         (next-pid))]
          (vswap! iris assoc class-iri type-sid)
          (recur r
                 (conj class-sids type-sid)
                 (into class-flakes
                       [(flake/->Flake type-sid const/$iri class-iri t true nil)
                        (flake/->Flake type-sid const/$rdf:type const/$rdfs:Class t true nil)]))))
      [class-sids class-flakes])))

(defn add-property
  [sid property {:keys [id value] :as v-map} t iris next-pid next-sid]
  (let [existing-pid   (get @iris property)
        pid            (or existing-pid
                           (let [new-id (next-pid)]
                             (vswap! iris assoc property new-id)
                             new-id))
        property-flake (when-not existing-pid
                         (flake/->Flake pid const/$iri property t true nil))
        flakes         (if id
                         (let [[id-sid id-flake] (if-let [existing (get @iris id)]
                                                   [existing nil]
                                                   (let [id-sid (next-sid)]
                                                     (vswap! iris assoc id id-sid)
                                                     (if (str/starts-with? id "_:") ;; blank node
                                                       [id-sid nil]
                                                       [id-sid (flake/->Flake id-sid const/$iri id t true nil)])))]
                           (cond-> [(flake/->Flake sid pid id-sid t true nil)]
                                   id-flake (conj id-flake)))
                         [(flake/->Flake sid pid value t true nil)])]
    (cond-> flakes
            property-flake (conj property-flake))))


(defn json-ld-node->flakes
  [node t iris next-pid next-sid]
  (let [id           (:id node)
        existing-sid (when id (get @iris id))
        sid          (or existing-sid
                         (let [new-sid (if (class-or-property? node)
                                         (next-pid)
                                         (next-sid))]
                           (vswap! iris assoc id new-sid)
                           new-sid))
        id-flake     (if (or (nil? id)
                             existing-sid
                             (str/starts-with? id "_:"))
                       []
                       [(flake/->Flake sid const/$iri id t true nil)])]
    (reduce-kv
      (fn [flakes k v]
        (case k
          (:id :idx) flakes
          :type (let [[type-sids class-flakes] (json-ld-type-data v t iris next-pid)
                      type-flakes (map #(flake/->Flake sid const/$rdf:type % t true nil) type-sids)]
                  (into flakes (concat class-flakes type-flakes)))
          ;;else
          (if (sequential? v)
            (into flakes (mapcat #(add-property sid k % t iris next-pid next-sid) v))
            (into flakes (add-property sid k v t iris next-pid next-sid)))))
      id-flake node)))

(defn json-ld-graph->flakes
  "Raw JSON-LD graph to a set of flakes"
  [json-ld opts]
  (let [t           (or (:t opts) -1)
        block       (or (:block opts) 1)
        expanded    (json-ld/expand json-ld)
        iris        (volatile! {})
        last-pid    (volatile! 1000)
        last-sid    (volatile! (flake/->sid const/$_default 0))
        next-pid    (fn [] (vswap! last-pid inc))
        next-sid    (fn [] (vswap! last-sid inc))
        base-flakes (flake/sorted-set-by
                      flake/cmp-flakes-spot
                      (flake/->Flake const/$rdf:type const/$iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" t true nil)
                      (flake/->Flake const/$rdfs:Class const/$iri "http://www.w3.org/2000/01/rdf-schema#Class" t true nil))]
    (loop [[node & r] (if (sequential? expanded)
                        expanded
                        [expanded])
           flakes base-flakes]
      (if node
        (recur r (into flakes (json-ld-node->flakes node t iris next-pid next-sid)))
        {:block  block
         :t      t
         :flakes flakes}))))

(comment

  (json-ld-graph->flakes
    {"@context" "https://schema.org/",
     "@graph"   [{"@id"             "http://worldcat.org/entity/work/id/2292573321",
                  "@type"           "Book",
                  "author"          {"@id" "http://viaf.org/viaf/17823"},
                  "inLanguage"      "fr",
                  "name"            "Rouge et le noir",
                  "workTranslation" {"@type" "Book", "@id" "http://worldcat.org/entity/work/id/460647"}}
                 {"@id"               "http://worldcat.org/entity/work/id/460647",
                  "@type"             "Book",
                  "about"             "Psychological fiction, French",
                  "author"            {"@id" "http://viaf.org/viaf/17823"},
                  "inLanguage"        "en",
                  "name"              "Red and Black : A New Translation, Backgrounds and Sources, Criticism",
                  "translationOfWork" {"@id" "http://worldcat.org/entity/work/id/2292573321"},
                  "translator"        {"@id" "http://viaf.org/viaf/8453420"}}]}
    {})


  (json-ld-graph->flakes
    {"@context"                  "https://schema.org",
     "@id"                       "https://www.wikidata.org/wiki/Q836821",
     "@type"                     "Movie",
     "name"                      "The Hitchhiker's Guide to the Galaxy",
     "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
     "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
     "isBasedOn"                 {"@id"    "https://www.wikidata.org/wiki/Q3107329",
                                  "@type"  "Book",
                                  "name"   "The Hitchhiker's Guide to the Galaxy",
                                  "isbn"   "0-330-25864-8",
                                  "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                            "@type" "Person"
                                            "name"  "Douglas Adams"}}}
    {})

  )