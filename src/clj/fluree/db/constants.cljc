(ns fluree.db.constants
  (:require [fluree.db.json-ld.iri :as iri]))

#?(:clj (set! *warn-on-reflection* true))


;; Version

(def ^:const data_version 4)

;; iri constants
(def ^:const iri-CommitProof "https://ns.flur.ee/ledger#CommitProof")
(def ^:const iri-Commit "https://ns.flur.ee/ledger#Commit")
(def ^:const iri-commit "https://ns.flur.ee/ledger#commit")
(def ^:const iri-DB "https://ns.flur.ee/ledger#DB")
(def ^:const iri-data "https://ns.flur.ee/ledger#data")
(def ^:const iri-t "https://ns.flur.ee/ledger#t")

(def ^:const iri-address "https://ns.flur.ee/ledger#address")
(def ^:const iri-v "https://ns.flur.ee/ledger#v")
(def ^:const iri-flakes "https://ns.flur.ee/ledger#flakes")
(def ^:const iri-size "https://ns.flur.ee/ledger#size")
(def ^:const iri-assert "https://ns.flur.ee/ledger#assert")
(def ^:const iri-retract "https://ns.flur.ee/ledger#retract")
(def ^:const iri-previous "https://ns.flur.ee/ledger#previous")
(def ^:const iri-alias "https://ns.flur.ee/ledger#alias")
(def ^:const iri-ledger "https://ns.flur.ee/ledger#ledger")
(def ^:const iri-branch "https://ns.flur.ee/ledger#branch")
(def ^:const iri-issuer "https://www.w3.org/2018/credentials#issuer")
(def ^:const iri-cred-subj "https://www.w3.org/2018/credentials#credentialSubject")
(def ^:const iri-index "https://ns.flur.ee/ledger#index")
(def ^:const iri-ns "https://ns.flur.ee/ledger#ns")
(def ^:const iri-time "https://ns.flur.ee/ledger#time")
(def ^:const iri-message "https://ns.flur.ee/ledger#message")
(def ^:const iri-tag "https://ns.flur.ee/ledger#tag")
(def ^:const iri-updates "https://ns.flur.ee/ledger#updates")
(def ^:const iri-default-context "https://ns.flur.ee/ledger#defaultContext")
(def ^:const iri-Context "https://ns.flur.ee/ledger#Context")
(def ^:const iri-allow "https://ns.flur.ee/ledger#allow")
(def ^:const iri-equals "https://ns.flur.ee/ledger#equals")
(def ^:const iri-contains "https://ns.flur.ee/ledger#contains")
(def ^:const iri-$identity "https://ns.flur.ee/ledger#$identity")
(def ^:const iri-target-role "https://ns.flur.ee/ledger#targetRole")
(def ^:const iri-target-class "https://ns.flur.ee/ledger#targetClass")
(def ^:const iri-target-node "https://ns.flur.ee/ledger#targetNode")
(def ^:const iri-target-objects-of "https://ns.flur.ee/ledger#targetObjectsOf")
(def ^:const iri-property "https://ns.flur.ee/ledger#property")
(def ^:const iri-policy "https://ns.flur.ee/ledger#Policy")
(def ^:const iri-opts "https://ns.flur.ee/ledger#opts")
(def ^:const iri-path "https://ns.flur.ee/ledger#path")
(def ^:const iri-action "https://ns.flur.ee/ledger#action")
(def ^:const iri-all-nodes "https://ns.flur.ee/ledger#allNodes")
(def ^:const iri-view "https://ns.flur.ee/ledger#view")
(def ^:const iri-modify "https://ns.flur.ee/ledger#modify")
(def ^:const iri-role "https://ns.flur.ee/ledger#role")
(def ^:const iri-where "https://ns.flur.ee/ledger#where")
(def ^:const iri-values "https://ns.flur.ee/ledger#values")
(def ^:const iri-insert "https://ns.flur.ee/ledger#insert")
(def ^:const iri-delete "https://ns.flur.ee/ledger#delete")

(def ^:const iri-context "@context")
(def ^:const iri-id "@id")
(def ^:const iri-value "@value")
(def ^:const iri-language "@language")
(def ^:const iri-type "@type")
(def ^:const iri-rdf-type "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
(def ^:const iri-class "http://www.w3.org/2000/01/rdf-schema#Class")
(def ^:const iri-lang-string "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")

;; system collection ids
(def ^:const $_tx -1) ; Note unlike other collection ids, this is never used to generate _tx values, as _tx has the full negative range
(def ^:const $_predicate 0)
(def ^:const $_collection 1)
(def ^:const $_shard 2)
(def ^:const $_tag 3)
(def ^:const $_fn 4)
(def ^:const $_user 5)
(def ^:const $_auth 6)
(def ^:const $_role 7)
(def ^:const $_rule 8)
(def ^:const $_setting 9)
(def ^:const $_ctx 10)
(def ^:const $_prefix 11)
(def ^:const $_default 12)

(def ^:const $numSystemCollections 19)                      ;; max number reserved for 'system'
(def ^:const $maxSystemPredicates 999)

;; predicate id constants

(def ^:const $_previous 52)
(def ^:const $_v 58)
(def ^:const $_address 59)

(def ^:const $_commit:dbId 51)                                ;; JSON-LD: turning into data/db id

(def ^:const $_commit:idRef 53)
(def ^:const $_commit:message 54)
(def ^:const $_commit:time 55)
(def ^:const $_commit:signer 57)


(def ^:const $_ledger:alias 170)
(def ^:const $_ledger:branch 171)
(def ^:const $_ledger:context 172)

(def ^:const $_commit:data 160)
(def ^:const $_commitdata:flakes 182)
(def ^:const $_commitdata:size 183)
(def ^:const $_commitdata:t 184)

(def ^:const $_predicate:fullText 27)
(def ^:const $_predicate:equivalentProperty 35)                          ;; any unique alias for predicate


(def ^:const $id (iri/iri->sid "@id"))

(def ^:const $rdf:type (iri/iri->sid "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
(def ^:const $rdf:Property (iri/iri->sid "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"))
(def ^:const $rdf:langString (iri/iri->sid "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"))

(def ^:const $rdfs:subClassOf (iri/iri->sid "http://www.w3.org/2000/01/rdf-schema#subClassOf"))
(def ^:const $rdfs:subPropertyOf (iri/iri->sid "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"))
(def ^:const $rdfs:Class (iri/iri->sid "http://www.w3.org/2000/01/rdf-schema#Class"))

;; shacl
(def ^:const $sh:NodeShape (iri/iri->sid "http://www.w3.org/ns/shacl#NodeShape"))
(def ^:const $sh:PropertyShape (iri/iri->sid "http://www.w3.org/ns/shacl#PropertyShape"))
(def ^:const $sh:targetClass (iri/iri->sid "http://www.w3.org/ns/shacl#targetClass"))
(def ^:const $sh:targetNode (iri/iri->sid "http://www.w3.org/ns/shacl#targetNode"))
(def ^:const $sh:targetObjectsOf (iri/iri->sid "http://www.w3.org/ns/shacl#targetObjectsOf"))
(def ^:const $sh:targetSubjectsOf (iri/iri->sid "http://www.w3.org/ns/shacl#targetSubjectsOf"))
(def ^:const $sh:closed (iri/iri->sid "http://www.w3.org/ns/shacl#closed"))
(def ^:const $sh:ignoredProperties (iri/iri->sid "http://www.w3.org/ns/shacl#ignoredProperties"))
(def ^:const $sh:property (iri/iri->sid "http://www.w3.org/ns/shacl#property"))
(def ^:const $sh:node (iri/iri->sid "http://www.w3.org/ns/shacl#node"))
(def ^:const $sh:qualifiedValueShape (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedValueShape"))
(def ^:const $sh:qualifiedMinCount (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedMinCount"))
(def ^:const $sh:qualifiedMaxCount (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedMaxCount"))
(def ^:const $sh:qualifiedValueShapesDisjoint (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint"))
(def ^:const $sh:path (iri/iri->sid "http://www.w3.org/ns/shacl#path"))
(def ^:const $sh:minCount (iri/iri->sid "http://www.w3.org/ns/shacl#minCount"))
(def ^:const $sh:maxCount (iri/iri->sid "http://www.w3.org/ns/shacl#maxCount"))
(def ^:const $sh:datatype (iri/iri->sid "http://www.w3.org/ns/shacl#datatype"))
;; nodes
(def ^:const $sh:nodeKind (iri/iri->sid "http://www.w3.org/ns/shacl#nodeKind"))
(def ^:const $sh:IRI (iri/iri->sid "http://www.w3.org/ns/shacl#IRI"))
(def ^:const $sh:IRIOrLiteral (iri/iri->sid "http://www.w3.org/ns/shacl#IRIOrLiteral"))
(def ^:const $sh:BlankNodeOrIRI (iri/iri->sid "http://www.w3.org/ns/shacl#BlankNodeOrIRI"))
(def ^:const $sh:BlankNode (iri/iri->sid "http://www.w3.org/ns/shacl#BlankNode"))
(def ^:const $sh:BlankNodeOrLiteral (iri/iri->sid "http://www.w3.org/ns/shacl#BlankNodeOrLiteral"))
(def ^:const $sh:Literal (iri/iri->sid "http://www.w3.org/ns/shacl#Literal"))
;; string validation
(def ^:const $sh:flags (iri/iri->sid "http://www.w3.org/ns/shacl#flags"))
(def ^:const $sh:minLength (iri/iri->sid "http://www.w3.org/ns/shacl#minLength"))
(def ^:const $sh:maxLength (iri/iri->sid "http://www.w3.org/ns/shacl#maxLength"))
(def ^:const $sh:pattern (iri/iri->sid "http://www.w3.org/ns/shacl#pattern"))
(def ^:const $sh:languageIn (iri/iri->sid "http://www.w3.org/ns/shacl#languageIn"))
(def ^:const $sh:uniqueLang (iri/iri->sid "http://www.w3.org/ns/shacl#uniqueLang"))
;; class restrictions
(def ^:const $sh:class (iri/iri->sid "http://www.w3.org/ns/shacl#class"))
(def ^:const $sh:hasValue (iri/iri->sid "http://www.w3.org/ns/shacl#hasValue"))
(def ^:const $sh:in (iri/iri->sid "http://www.w3.org/ns/shacl#in"))
;; number comparisons
(def ^:const $sh:minExclusive (iri/iri->sid "http://www.w3.org/ns/shacl#minExclusive"))
(def ^:const $sh:minInclusive (iri/iri->sid "http://www.w3.org/ns/shacl#minInclusive"))
(def ^:const $sh:maxExclusive (iri/iri->sid "http://www.w3.org/ns/shacl#maxExclusive"))
(def ^:const $sh:maxInclusive (iri/iri->sid "http://www.w3.org/ns/shacl#maxInclusive"))


;;property pair constraints
(def ^:const $sh:equals (iri/iri->sid "http://www.w3.org/ns/shacl#equals"))
(def ^:const $sh:disjoint (iri/iri->sid "http://www.w3.org/ns/shacl#disjoint"))
(def ^:const $sh:lessThan (iri/iri->sid "http://www.w3.org/ns/shacl#lessThan"))
(def ^:const $sh:lessThanOrEquals (iri/iri->sid "http://www.w3.org/ns/shacl#lessThanOrEquals"))

;; logical constraints
(def ^:const $sh:not (iri/iri->sid "http://www.w3.org/ns/shacl#not"))
(def ^:const $sh:and (iri/iri->sid "http://www.w3.org/ns/shacl#and"))
(def ^:const $sh:or (iri/iri->sid "http://www.w3.org/ns/shacl#or"))
(def ^:const $sh:xone (iri/iri->sid "http://www.w3.org/ns/shacl#xone"))

;; path types
(def ^:const $sh:alternativePath (iri/iri->sid "http://www.w3.org/ns/shacl#alternativePath"))
(def ^:const $sh:zeroOrMorePath (iri/iri->sid "http://www.w3.org/ns/shacl#zeroOrMorePath"))
(def ^:const $sh:oneOrMorePath (iri/iri->sid "http://www.w3.org/ns/shacl#oneOrMorePath"))
(def ^:const $sh:zeroOrOnePath (iri/iri->sid "http://www.w3.org/ns/shacl#zeroOrOnePath"))
(def ^:const $sh:inversePath (iri/iri->sid "http://www.w3.org/ns/shacl#inversePath"))

;; fluree-specific
(def ^:const $fluree:context 250)
(def ^:const $fluree:targetClass 255)

;; owl
(def ^:const $owl:Class (iri/iri->sid "http://www.w3.org/2002/07/owl#Class"))
(def ^:const $owl:ObjectProperty "http://www.w3.org/2002/07/owl#ObjectProperty")
(def ^:const $owl:DatatypeProperty (iri/iri->sid "http://www.w3.org/2002/07/owl#DatatypeProperty"))

;; == xsd data types ==
;; major types (a) ref, (b) string, (c) number, (d) boolean
;; xsd common types
(def ^:const $xsd:anyURI-iri "http://www.w3.org/2001/XMLSchema#anyURI")
(def ^:const $xsd:anyURI (iri/iri->sid $xsd:anyURI-iri))
(def ^:const $xsd:string (iri/iri->sid "http://www.w3.org/2001/XMLSchema#string"))
(def ^:const $xsd:boolean (iri/iri->sid "http://www.w3.org/2001/XMLSchema#boolean"))
(def ^:const $xsd:date (iri/iri->sid "http://www.w3.org/2001/XMLSchema#date"))
(def ^:const $xsd:dateTime (iri/iri->sid "http://www.w3.org/2001/XMLSchema#dateTime"))
;; xsd number types
(def ^:const $xsd:decimal (iri/iri->sid "http://www.w3.org/2001/XMLSchema#decimal"))
(def ^:const $xsd:double (iri/iri->sid "http://www.w3.org/2001/XMLSchema#double"))
(def ^:const $xsd:integer (iri/iri->sid "http://www.w3.org/2001/XMLSchema#integer"))
(def ^:const $xsd:long (iri/iri->sid "http://www.w3.org/2001/XMLSchema#long"))
(def ^:const $xsd:int (iri/iri->sid "http://www.w3.org/2001/XMLSchema#int"))
(def ^:const $xsd:short (iri/iri->sid "http://www.w3.org/2001/XMLSchema#short"))
(def ^:const $xsd:float (iri/iri->sid "http://www.w3.org/2001/XMLSchema#float"))
(def ^:const $xsd:unsignedLong (iri/iri->sid "http://www.w3.org/2001/XMLSchema#unsignedLong"))
(def ^:const $xsd:unsignedInt (iri/iri->sid "http://www.w3.org/2001/XMLSchema#unsignedInt"))
(def ^:const $xsd:unsignedShort (iri/iri->sid "http://www.w3.org/2001/XMLSchema#unsignedShort"))
(def ^:const $xsd:positiveInteger (iri/iri->sid "http://www.w3.org/2001/XMLSchema#positiveInteger"))
(def ^:const $xsd:nonPositiveInteger (iri/iri->sid "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"))
(def ^:const $xsd:negativeInteger (iri/iri->sid "http://www.w3.org/2001/XMLSchema#negativeInteger"))
(def ^:const $xsd:nonNegativeInteger (iri/iri->sid "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"))
;; xsd date and specialized strings
(def ^:const $xsd:duration (iri/iri->sid "http://www.w3.org/2001/XMLSchema#duration"))
(def ^:const $xsd:gDay (iri/iri->sid "http://www.w3.org/2001/XMLSchema#gDay"))
(def ^:const $xsd:gMonth (iri/iri->sid "http://www.w3.org/2001/XMLSchema#gMonth"))
(def ^:const $xsd:gMonthDay (iri/iri->sid "http://www.w3.org/2001/XMLSchema#gMonthDay"))
(def ^:const $xsd:gYear (iri/iri->sid "http://www.w3.org/2001/XMLSchema#gYear"))
(def ^:const $xsd:gYearMonth (iri/iri->sid "http://www.w3.org/2001/XMLSchema#gYearMonth"))
(def ^:const $xsd:time (iri/iri->sid "http://www.w3.org/2001/XMLSchema#time"))
(def ^:const $xsd:normalizedString (iri/iri->sid "http://www.w3.org/2001/XMLSchema#normalizedString"))
(def ^:const $xsd:token (iri/iri->sid "http://www.w3.org/2001/XMLSchema#token"))
(def ^:const $xsd:language (iri/iri->sid "http://www.w3.org/2001/XMLSchema#language"))
;; xsd binary types
(def ^:const $xsd:byte (iri/iri->sid "http://www.w3.org/2001/XMLSchema#byte")) ; store as number
(def ^:const $xsd:unsignedByte (iri/iri->sid "http://www.w3.org/2001/XMLSchema#unsignedByte")) ; store as number
(def ^:const $xsd:hexBinary (iri/iri->sid "http://www.w3.org/2001/XMLSchema#hexBinary"))
(def ^:const $xsd:base64Binary (iri/iri->sid "http://www.w3.org/2001/XMLSchema#base64Binary"))
(def ^:const $rdf:json (iri/iri->sid "http://www.w3.org/2001/XMLSchema#json"))
;; NOTE: Add multibyte type?
;; NOTE: Add geo types? string-encoded GeoJSON?


(def ^:const $_tag:id 30)

(def ^:const $fluree:default-context 150)
