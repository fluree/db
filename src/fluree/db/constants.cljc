(ns fluree.db.constants)

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

(def ^:const iri-id "@id")
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
;; TODO - jumping predicate ids - rethink ordering a bit
(def ^:const $rdf:type 200)
(def ^:const $rdfs:subClassOf 201)
(def ^:const $rdfs:subPropertyOf 202)
(def ^:const $rdfs:Class 203)
(def ^:const $rdf:Property 204)
(def ^:const $rdf:langString 205)

;; shacl
(def ^:const $sh:NodeShape 210)
(def ^:const $sh:PropertyShape 211)
(def ^:const $sh:targetClass 212)
(def ^:const $sh:targetNode 213)
(def ^:const $sh:targetObjectsOf 214)
(def ^:const $sh:targetSubjectsOf 215)
(def ^:const $sh:closed 216)
(def ^:const $sh:ignoredProperties 217)
(def ^:const $sh:property 218)
(def ^:const $sh:node 261)
(def ^:const $sh:qualifiedValueShape 262)
(def ^:const $sh:qualifiedMinCount 263)
(def ^:const $sh:qualifiedMaxCount 264)
(def ^:const $sh:qualifiedValueShapesDisjoint 265)
(def ^:const $sh:path 219)
(def ^:const $sh:minCount 220)
(def ^:const $sh:maxCount 221)
(def ^:const $sh:datatype 222)
;; nodes
(def ^:const $sh:nodeKind 223)
(def ^:const $sh:IRI 224)
(def ^:const $sh:IRIOrLiteral 225)
(def ^:const $sh:BlankNodeOrIRI 226)
(def ^:const $sh:BlankNode 227)
(def ^:const $sh:BlankNodeOrLiteral 228)
(def ^:const $sh:Literal 229)
;; string validation
(def ^:const $sh:flags 249)
(def ^:const $sh:minLength 230)
(def ^:const $sh:maxLength 231)
(def ^:const $sh:pattern 232)
(def ^:const $sh:languageIn 233)
(def ^:const $sh:uniqueLang 234)
;; class restrictions
(def ^:const $sh:class 235)
(def ^:const $sh:hasValue 236)
(def ^:const $sh:in 237)
;; number comparisons
(def ^:const $sh:minExclusive 238)
(def ^:const $sh:minInclusive 239)
(def ^:const $sh:maxExclusive 240)
(def ^:const $sh:maxInclusive 241)


;;property pair constraints
(def ^:const $sh:equals 242)
(def ^:const $sh:disjoint 243)
(def ^:const $sh:lessThan 244)
(def ^:const $sh:lessThanOrEquals 248)

;; logical constraints
(def ^:const $sh:not 251)
(def ^:const $sh:and 252)
(def ^:const $sh:or 253)
(def ^:const $sh:xone 254)

;; path types
(def ^:const $sh:alternativePath 256)
(def ^:const $sh:zeroOrMorePath 257)
(def ^:const $sh:oneOrMorePath 258)
(def ^:const $sh:zeroOrOnePath 259)
(def ^:const $sh:inversePath 260)

;; fluree-specific
(def ^:const $fluree:context 250)
(def ^:const $fluree:targetClass 255)

;; owl
(def ^:const $owl:Class 245)
(def ^:const $owl:ObjectProperty 246)
(def ^:const $owl:DatatypeProperty 247)

;; == xsd data types ==
;; major types (a) ref, (b) string, (c) number, (d) boolean
;; xsd common types
(def ^:const $xsd:anyURI 0)
(def ^:const $xsd:string 1)
(def ^:const $xsd:boolean 2)
(def ^:const $xsd:date 3)
(def ^:const $xsd:dateTime 4)
;; xsd number types
(def ^:const $xsd:decimal 5)
(def ^:const $xsd:double 6)
(def ^:const $xsd:integer 7)
(def ^:const $xsd:long 8)
(def ^:const $xsd:int 10)
(def ^:const $xsd:short 11)
(def ^:const $xsd:float 12)
(def ^:const $xsd:unsignedLong 13)
(def ^:const $xsd:unsignedInt 14)
(def ^:const $xsd:unsignedShort 15)
(def ^:const $xsd:positiveInteger 16)
(def ^:const $xsd:nonPositiveInteger 17)
(def ^:const $xsd:negativeInteger 18)
(def ^:const $xsd:nonNegativeInteger 19)
;; xsd date and specialized strings
(def ^:const $xsd:duration 20)
(def ^:const $xsd:gDay 21)
(def ^:const $xsd:gMonth 22)
(def ^:const $xsd:gMonthDay 23)
(def ^:const $xsd:gYear 24)
(def ^:const $xsd:gYearMonth 25)
(def ^:const $xsd:time 26)
(def ^:const $xsd:normalizedString 27)
(def ^:const $xsd:token 28)
(def ^:const $xsd:language 29)
;; xsd binary types
(def ^:const $xsd:byte 30)                                  ;; store as number
(def ^:const $xsd:unsignedByte 31)                          ;; store as number
(def ^:const $xsd:hexBinary 32)
(def ^:const $xsd:base64Binary 33)
;; NOTE: Add multibyte type?
;; NOTE: Add JSON type?
;; NOTE: Add geo types? string-encoded GeoJSON?


(def ^:const $_tag:id 30)

(def ^:const $fluree:default-context 150)
