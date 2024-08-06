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
(def ^:const iri-fluree-t (fluree-iri "t"))
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
(def ^:const iri-namespaces (fluree-iri "namespaces"))
(def ^:const iri-issuer "https://www.w3.org/2018/credentials#issuer")
(def ^:const iri-cred-subj "https://www.w3.org/2018/credentials#credentialSubject")
(def ^:const iri-index (fluree-iri "index"))
(def ^:const iri-ns (fluree-iri "ns"))
(def ^:const iri-time (fluree-iri "time"))
(def ^:const iri-author (fluree-iri "author"))
(def ^:const iri-txn (fluree-iri "txn"))
(def ^:const iri-annotation (fluree-iri "annotation"))
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
(def ^:const iri-rule (fluree-iri "rule"))
(def ^:const iri-query (fluree-iri "query"))
(def ^:const iri-onClass (fluree-iri "onClass"))
(def ^:const iri-onProperty (fluree-iri "onProperty"))
(def ^:const iri-exMessage (fluree-iri "exMessage"))
(def ^:const iri-policyClass (fluree-iri "policyClass"))
(def ^:const iri-context "@context")
(def ^:const iri-base "@base")
(def ^:const iri-vocab "@vocab")
(def ^:const iri-id "@id")
(def ^:const iri-value "@value")
(def ^:const iri-language "@language")
(def ^:const iri-type "@type")
(def ^:const iri-filter "@filter")
(def ^:const iri-t "@t")
(def ^:const iri-json "http://www.w3.org/2001/XMLSchema#json")
(def ^:const iri-anyURI "http://www.w3.org/2001/XMLSchema#anyURI")
(def ^:const iri-rdf-type "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
(def ^:const iri-class "http://www.w3.org/2000/01/rdf-schema#Class")
(def ^:const iri-lang-string "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")
(def ^:const iri-string "http://www.w3.org/2001/XMLSchema#string")

;; rdfs
(def ^:const iri-rdfs:Class "http://www.w3.org/2000/01/rdf-schema#Class")
(def ^:const iri-rdfs:subClassOf "http://www.w3.org/2000/01/rdf-schema#subClassOf")
(def ^:const iri-rdfs:subPropertyOf "http://www.w3.org/2000/01/rdf-schema#subPropertyOf")
(def ^:const iri-rdfs:domain "http://www.w3.org/2000/01/rdf-schema#domain")
(def ^:const iri-rdfs:range "http://www.w3.org/2000/01/rdf-schema#range")

;; OWL
(def ^:const iri-owl:Thing "http://www.w3.org/2002/07/owl#Thing")
(def ^:const iri-owl:Class "http://www.w3.org/2002/07/owl#Class")
(def ^:const iri-owl:ObjectProperty "http://www.w3.org/2002/07/owl#ObjectProperty")
(def ^:const iri-owl:FunctionalProperty "http://www.w3.org/2002/07/owl#FunctionalProperty")
(def ^:const iri-owl:InverseFunctionalProperty "http://www.w3.org/2002/07/owl#InverseFunctionalProperty")
(def ^:const iri-owl:SymetricProperty "http://www.w3.org/2002/07/owl#SymetricProperty")
(def ^:const iri-owl:TransitiveProperty "http://www.w3.org/2002/07/owl#TransitiveProperty")

(def ^:const iri-owl:sameAs "http://www.w3.org/2002/07/owl#sameAs")

;; property expressions
(def ^:const iri-owl:propertyChainAxiom "http://www.w3.org/2002/07/owl#propertyChainAxiom")
(def ^:const iri-owl:inverseOf "http://www.w3.org/2002/07/owl#inverseOf")
(def ^:const iri-owl:hasKey "http://www.w3.org/2002/07/owl#hasKey")

;; class expressions
(def ^:const iri-owl:equivalentClass "http://www.w3.org/2002/07/owl#equivalentClass")
(def ^:const iri-owl:intersectionOf "http://www.w3.org/2002/07/owl#intersectionOf")
(def ^:const iri-owl:unionOf "http://www.w3.org/2002/07/owl#unionOf")
(def ^:const iri-owl:Restriction "http://www.w3.org/2002/07/owl#Restriction")
(def ^:const iri-owl:onProperty "http://www.w3.org/2002/07/owl#onProperty")
(def ^:const iri-owl:onClass "http://www.w3.org/2002/07/owl#onClass")
(def ^:const iri-owl:oneOf "http://www.w3.org/2002/07/owl#oneOf")
(def ^:const iri-owl:hasValue "http://www.w3.org/2002/07/owl#hasValue")
(def ^:const iri-owl:someValuesFrom "http://www.w3.org/2002/07/owl#someValuesFrom")
(def ^:const iri-owl:allValuesFrom "http://www.w3.org/2002/07/owl#allValuesFrom")
(def ^:const iri-owl:maxCardinality "http://www.w3.org/2002/07/owl#maxCardinality")
(def ^:const iri-owl:maxQualifiedCardinality "http://www.w3.org/2002/07/owl#maxQualifiedCardinality")
(def ^:const iri-owl:qualifiedCardinality "http://www.w3.org/2002/07/owl#qualifiedCardinality")

;; SHACL validation report iris
(def ^:const iri_ValidationReport "http://www.w3.org/ns/shacl#ValidationReport")
(def ^:const iri_conforms "http://www.w3.org/ns/shacl#conforms")
(def ^:const iri_shapesGraphWellFormed "http://www.w3.org/ns/shacl#shapesGraphWellFormed")
(def ^:const iri_result "http://www.w3.org/ns/shacl#result")
(def ^:const iri_ValidationResult "http://www.w3.org/ns/shacl#ValidationResult")
(def ^:const iri_focusNode "http://www.w3.org/ns/shacl#focusNode")
(def ^:const iri_resultPath "http://www.w3.org/ns/shacl#resultPath")
(def ^:const iri_value "http://www.w3.org/ns/shacl#value")
(def ^:const iri_sourceShape "http://www.w3.org/ns/shacl#sourceShape")
(def ^:const iri_constraintComponent "http://www.w3.org/ns/shacl#constraintComponent")
(def ^:const iri_details "http://www.w3.org/ns/shacl#details")
(def ^:const iri_resultMessage "http://www.w3.org/ns/shacl#resultMessage")
(def ^:const iri_resultSeverity "http://www.w3.org/ns/shacl#resultSeverity")
(def ^:const iri_Violation "http://www.w3.org/ns/shacl#Violation")
(def ^:const iri_expectation (fluree-iri "expectation"))

;; predicate id constants

(def ^:const $_previous (iri/iri->sid iri-previous))
(def ^:const $_v (iri/iri->sid iri-v))
(def ^:const $_address (iri/iri->sid iri-address))
(def ^:const $_commit:message (iri/iri->sid iri-message))
(def ^:const $_commit:time (iri/iri->sid iri-time))
(def ^:const $_commit:signer (iri/iri->sid iri-issuer))
(def ^:const $_commit:author (iri/iri->sid iri-author))
(def ^:const $_commit:txn (iri/iri->sid iri-txn))
(def ^:const $_commit:annotation (iri/iri->sid iri-annotation))
(def ^:const $_ledger:alias (iri/iri->sid iri-alias))
(def ^:const $_ledger:branch (iri/iri->sid iri-branch))

(def ^:const $_commit:data (iri/iri->sid iri-data))
(def ^:const $_commitdata:flakes (iri/iri->sid iri-flakes))
(def ^:const $_commitdata:size (iri/iri->sid iri-size))
(def ^:const $_commitdata:t (iri/iri->sid iri-fluree-t))

(def ^:const $id (iri/iri->sid iri-id))

(def ^:const $rdf:type iri/type-sid)
(def ^:const $rdf:Property (iri/iri->sid "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"))
(def ^:const $rdf:langString (iri/iri->sid "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"))

(def ^:const $rdfs:subClassOf (iri/iri->sid iri-rdfs:subClassOf))
(def ^:const $rdfs:subPropertyOf (iri/iri->sid iri-rdfs:subPropertyOf))
(def ^:const $rdfs:Class (iri/iri->sid iri-rdfs:Class))


;; shacl
(def ^:const sh_NodeShape (iri/iri->sid "http://www.w3.org/ns/shacl#NodeShape"))
(def ^:const sh_deactivated (iri/iri->sid "http://www.w3.org/ns/shacl#deactivated"))
(def ^:const sh_message (iri/iri->sid "http://www.w3.org/ns/shacl#message"))
(def ^:const sh_severity (iri/iri->sid "http://www.w3.org/ns/shacl#severity"))

(def ^:const sh_PropertyShape (iri/iri->sid "http://www.w3.org/ns/shacl#PropertyShape"))
(def ^:const sh_path (iri/iri->sid "http://www.w3.org/ns/shacl#path"))

(def ^:const sh_alternativePath (iri/iri->sid "http://www.w3.org/ns/shacl#alternativePath"))
(def ^:const sh_zeroOrMorePath (iri/iri->sid "http://www.w3.org/ns/shacl#zeroOrMorePath"))
(def ^:const sh_oneOrMorePath (iri/iri->sid "http://www.w3.org/ns/shacl#oneOrMorePath"))
(def ^:const sh_zeroOrOnePath (iri/iri->sid "http://www.w3.org/ns/shacl#zeroOrOnePath"))
(def ^:const sh_inversePath (iri/iri->sid "http://www.w3.org/ns/shacl#inversePath"))

;; targets
(def ^:const sh_targetClass (iri/iri->sid "http://www.w3.org/ns/shacl#targetClass"))
(def ^:const sh_targetNode (iri/iri->sid "http://www.w3.org/ns/shacl#targetNode"))
(def ^:const sh_targetObjectsOf (iri/iri->sid "http://www.w3.org/ns/shacl#targetObjectsOf"))
(def ^:const sh_targetSubjectsOf (iri/iri->sid "http://www.w3.org/ns/shacl#targetSubjectsOf"))

;; constraints:
;; value type
(def ^:const sh_class (iri/iri->sid "http://www.w3.org/ns/shacl#class"))
(def ^:const sh_datatype (iri/iri->sid "http://www.w3.org/ns/shacl#datatype"))
(def ^:const sh_nodeKind (iri/iri->sid "http://www.w3.org/ns/shacl#nodeKind"))

(def ^:const sh_IRI (iri/iri->sid "http://www.w3.org/ns/shacl#IRI"))
(def ^:const sh_IRIOrLiteral (iri/iri->sid "http://www.w3.org/ns/shacl#IRIOrLiteral"))
(def ^:const sh_BlankNodeOrIRI (iri/iri->sid "http://www.w3.org/ns/shacl#BlankNodeOrIRI"))
(def ^:const sh_BlankNode (iri/iri->sid "http://www.w3.org/ns/shacl#BlankNode"))
(def ^:const sh_BlankNodeOrLiteral (iri/iri->sid "http://www.w3.org/ns/shacl#BlankNodeOrLiteral"))
(def ^:const sh_Literal (iri/iri->sid "http://www.w3.org/ns/shacl#Literal"))

;; cardinality
(def ^:const sh_minCount (iri/iri->sid "http://www.w3.org/ns/shacl#minCount"))
(def ^:const sh_maxCount (iri/iri->sid "http://www.w3.org/ns/shacl#maxCount"))

;; value range
(def ^:const sh_minExclusive (iri/iri->sid "http://www.w3.org/ns/shacl#minExclusive"))
(def ^:const sh_minInclusive (iri/iri->sid "http://www.w3.org/ns/shacl#minInclusive"))
(def ^:const sh_maxExclusive (iri/iri->sid "http://www.w3.org/ns/shacl#maxExclusive"))
(def ^:const sh_maxInclusive (iri/iri->sid "http://www.w3.org/ns/shacl#maxInclusive"))

;; string-based
(def ^:const sh_minLength (iri/iri->sid "http://www.w3.org/ns/shacl#minLength"))
(def ^:const sh_maxLength (iri/iri->sid "http://www.w3.org/ns/shacl#maxLength"))
(def ^:const sh_pattern (iri/iri->sid "http://www.w3.org/ns/shacl#pattern"))
(def ^:const sh_flags (iri/iri->sid "http://www.w3.org/ns/shacl#flags"))
(def ^:const sh_languageIn (iri/iri->sid "http://www.w3.org/ns/shacl#languageIn"))
(def ^:const sh_uniqueLang (iri/iri->sid "http://www.w3.org/ns/shacl#uniqueLang"))

;; property pair
(def ^:const sh_equals (iri/iri->sid "http://www.w3.org/ns/shacl#equals"))
(def ^:const sh_disjoint (iri/iri->sid "http://www.w3.org/ns/shacl#disjoint"))
(def ^:const sh_lessThan (iri/iri->sid "http://www.w3.org/ns/shacl#lessThan"))
(def ^:const sh_lessThanOrEquals (iri/iri->sid "http://www.w3.org/ns/shacl#lessThanOrEquals"))

;; logical constraints
(def ^:const sh_not (iri/iri->sid "http://www.w3.org/ns/shacl#not"))
(def ^:const sh_and (iri/iri->sid "http://www.w3.org/ns/shacl#and"))
(def ^:const sh_or (iri/iri->sid "http://www.w3.org/ns/shacl#or"))
(def ^:const sh_xone (iri/iri->sid "http://www.w3.org/ns/shacl#xone"))

;; shape-based
(def ^:const sh_property (iri/iri->sid "http://www.w3.org/ns/shacl#property"))
(def ^:const sh_node (iri/iri->sid "http://www.w3.org/ns/shacl#node"))
(def ^:const sh_qualifiedValueShape (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedValueShape"))
(def ^:const sh_qualifiedMinCount (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedMinCount"))
(def ^:const sh_qualifiedMaxCount (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedMaxCount"))
(def ^:const sh_qualifiedValueShapesDisjoint (iri/iri->sid "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint"))

;; other
(def ^:const sh_closed (iri/iri->sid "http://www.w3.org/ns/shacl#closed"))
(def ^:const sh_ignoredProperties (iri/iri->sid "http://www.w3.org/ns/shacl#ignoredProperties"))
(def ^:const sh_in (iri/iri->sid "http://www.w3.org/ns/shacl#in"))
(def ^:const sh_hasValue (iri/iri->sid "http://www.w3.org/ns/shacl#hasValue"))


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
