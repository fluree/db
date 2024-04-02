(ns fluree.db.constants
  (:require [fluree.db.json-ld.iri :as iri :refer [fluree-iri]]))

#?(:clj (set! *warn-on-reflection* true))

;; Version
(def ^:const data_version 4)

;; iri constants
(def ^:const iri-CommitProof (fluree-iri "CommitProof"))
(def ^:const iri-Commit (fluree-iri "Commit"))
(def ^:const iri-commit (fluree-iri "commit"))
(def ^:const iri-DB (fluree-iri "DB"))
(def ^:const iri-data (fluree-iri "data"))
(def ^:const iri-t (fluree-iri "t"))

(def ^:const iri-address (fluree-iri "address"))
(def ^:const iri-v (fluree-iri "v"))
(def ^:const iri-flakes (fluree-iri "flakes"))
(def ^:const iri-size (fluree-iri "size"))
(def ^:const iri-assert (fluree-iri "assert"))
(def ^:const iri-retract (fluree-iri "retract"))
(def ^:const iri-previous (fluree-iri "previous"))
(def ^:const iri-alias (fluree-iri "alias"))
(def ^:const iri-ledger (fluree-iri "ledger"))
(def ^:const iri-branch (fluree-iri "branch"))
(def ^:const iri-issuer "https://www.w3.org/2018/credentials#issuer")
(def ^:const iri-cred-subj "https://www.w3.org/2018/credentials#credentialSubject")
(def ^:const iri-index (fluree-iri "index"))
(def ^:const iri-ns (fluree-iri "ns"))
(def ^:const iri-time (fluree-iri "time"))
(def ^:const iri-author (fluree-iri "author"))
(def ^:const iri-txn (fluree-iri "txn"))
(def ^:const iri-message (fluree-iri "message"))
(def ^:const iri-tag (fluree-iri "tag"))
(def ^:const iri-updates (fluree-iri "updates"))
(def ^:const iri-allow (fluree-iri "allow"))
(def ^:const iri-equals (fluree-iri "equals"))
(def ^:const iri-contains (fluree-iri "contains"))
(def ^:const iri-$identity (fluree-iri "$identity"))
(def ^:const iri-target-role (fluree-iri "targetRole"))
(def ^:const iri-target-class (fluree-iri "targetClass"))
(def ^:const iri-target-node (fluree-iri "targetNode"))
(def ^:const iri-target-objects-of (fluree-iri "targetObjectsOf"))
(def ^:const iri-property (fluree-iri "property"))
(def ^:const iri-policy (fluree-iri "Policy"))
(def ^:const iri-opts (fluree-iri "opts"))
(def ^:const iri-path (fluree-iri "path"))
(def ^:const iri-action (fluree-iri "action"))
(def ^:const iri-all-nodes (fluree-iri "allNodes"))
(def ^:const iri-view (fluree-iri "view"))
(def ^:const iri-modify (fluree-iri "modify"))
(def ^:const iri-role (fluree-iri "role"))
(def ^:const iri-where (fluree-iri "where"))
(def ^:const iri-values (fluree-iri "values"))
(def ^:const iri-insert (fluree-iri "insert"))
(def ^:const iri-delete (fluree-iri "delete"))

(def ^:const iri-context "@context")
(def ^:const iri-id "@id")
(def ^:const iri-value "@value")
(def ^:const iri-language "@language")
(def ^:const iri-type "@type")
(def ^:const iri-filter "@filter")
(def ^:const iri-json "http://www.w3.org/2001/XMLSchema#json")
(def ^:const iri-anyURI "http://www.w3.org/2001/XMLSchema#anyURI")
(def ^:const iri-rdf-type "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
(def ^:const iri-class "http://www.w3.org/2000/01/rdf-schema#Class")
(def ^:const iri-lang-string "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")

;; predicate id constants

(def ^:const $_previous (iri/iri->sid iri-previous))
(def ^:const $_v (iri/iri->sid iri-v))
(def ^:const $_address (iri/iri->sid iri-address))
(def ^:const $_commit:message (iri/iri->sid iri-message))
(def ^:const $_commit:time (iri/iri->sid iri-time))
(def ^:const $_commit:signer (iri/iri->sid iri-issuer))
(def ^:const $_commit:author (iri/iri->sid iri-author))
(def ^:const $_commit:txn (iri/iri->sid iri-txn))
(def ^:const $_ledger:alias (iri/iri->sid iri-alias))
(def ^:const $_ledger:branch (iri/iri->sid iri-branch))

(def ^:const $_commit:data (iri/iri->sid iri-data))
(def ^:const $_commitdata:flakes (iri/iri->sid iri-flakes))
(def ^:const $_commitdata:size (iri/iri->sid iri-size))
(def ^:const $_commitdata:t (iri/iri->sid iri-t))

(def ^:const $id (iri/iri->sid iri-id))

(def ^:const $rdf:type iri/type-sid)
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
(def ^:const $fluree:targetClass (iri/iri->sid iri-target-class))

;; owl
(def ^:const $owl:Class (iri/iri->sid "http://www.w3.org/2002/07/owl#Class"))
(def ^:const $owl:ObjectProperty (iri/iri->sid "http://www.w3.org/2002/07/owl#ObjectProperty"))
(def ^:const $owl:DatatypeProperty (iri/iri->sid "http://www.w3.org/2002/07/owl#DatatypeProperty"))
(def ^:const $owl:equivalentProperty (iri/iri->sid "http://www.w3.org/2002/07/owl#equivalentProperty"))

;; == xsd data types ==
;; major types (a) ref, (b) string, (c) number, (d) boolean
;; xsd common types
(def ^:const $xsd:anyURI (iri/iri->sid iri-anyURI))
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
(def ^:const $rdf:json (iri/iri->sid iri-json))
;; NOTE: Add multibyte type?
;; NOTE: Add geo types? string-encoded GeoJSON?

(def ^:const $f:role (iri/iri->sid iri-role))
