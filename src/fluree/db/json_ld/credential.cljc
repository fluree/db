(ns fluree.db.json-ld.credential
  (:require [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [alphabase.core :as alphabase]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))

#_(defn generate-block
    "Generates a JSON-LD block file in the Fluree format."
    [{:keys [db-after flakes tx-state]}]
    (let [{:keys [iris]} tx-state
          iri-map (volatile! (-> (reduce #(assoc %1 (val %2) (key %2)) {} @iris)
                                 (assoc const/$rdf:type "@type")))
          ctx     (volatile! {})]
      (loop [[s-flakes & r] (partition-by #(.-s ^Flake %) flakes)
             assert  []
             retract []]
        (if s-flakes
          (let [sid            (.-s ^Flake (first s-flakes))
                s-iri          (jld-commit/get-s-iri sid db-after iri-map nil)
                non-iri-flakes (remove #(= const/$iri (.-p ^Flake %)) s-flakes)
                [s-assert s-retract] (jld-commit/subject-block non-iri-flakes db-after iri-map ctx)
                assert*        (if s-assert
                                 (conj assert (assoc s-assert "@id" s-iri))
                                 assert)
                retract*       (if s-retract
                                 (conj retract (assoc s-retract "@id" s-iri))
                                 retract)]
            (recur r assert* retract*))
          {:ctx     (dissoc @ctx "@type")                   ;; @type will be marked as @type: @id, which is implied
           :assert  assert
           :retract retract}))))

#_(defn wrap-block
    [tx-result]
    (let [{:keys [assert retract ctx]} (generate-block tx-result)]
      (cond-> {"@context" ["https://flur.ee/ns/block"
                           ctx]
               "@type"    ["Block"]}
              (seq assert) (assoc "assert" assert)
              (seq retract) (assoc "retract" retract))))

(def jws-header {"alg"  "ES256K-R"                          ;; https://github.com/decentralized-identity/EcdsaSecp256k1RecoverySignature2020
                 "b64"  false
                 "crit" ["b64"]})


(def jws-header-json (json-ld/normalize-data jws-header {:algorithm :basic
                                                         :format    :application/json}))

;; TODO - below encoding should be :base64URL once supported
(def jws-header-b64 (alphabase/base-to-base jws-header-json :string :base64)) ;; "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19"


(defn signing-input
  "JWS signing input is b64URL of header, + '.' + b64URL of json to be signed.
  The resulting input is hashed with SHA-256, and that result is what is signed."
  [json]
  ;; TODO - below should be :base64URL once supported
  (let [input (str jws-header-b64 "." (alphabase/base-to-base json :string :base64))]
    (crypto/sha2-256 input :bytes)))


(defn credential-json
  "Takes final credential response object (as returned by sign-credential)
  and formats JSON document ready for publishing."
  [credential-object]
  (let [{:keys [credential normalized]} credential-object
        proof            (get credential "proof")
        proof-normalized (json-ld/normalize-data proof)]
    (str (subs normalized 0 (dec (count normalized)))       ;; remove trailing '}', then add back
         ",\"proof\":" proof-normalized "}")))


(defn sign
  [credential private-key]
  ;; TODO - this is using a custom proof @type, and it should use EcdsaSecp256k1RecoverySignature2020 which requires RDF normalization
  (let [payload-json  (json-ld/normalize-data credential)
        signing-input (signing-input payload-json)
        ;; TODO - we need :base64URL encoding for signature, could update crypto/sign-message to allow configurable return encoding. Currently only returns hex
        ;; TODO - need to verify payload part of string to be signed - I think it is b64URL header + hex, but it would make more sense (to me) to have it just a byte array
        ;; TODO (continued) - which avoids any encoding dependency. JWS and/or EcdsaSecp256k1RecoverySignature2020 specs may make this process more clear.
        sig-b64       (-> signing-input
                          (crypto/sign-message private-key)
                          ;; TODO - below should be :base64URL
                          (alphabase/base-to-base :hex :base64))
        did           (get credential "issuer")
        proof         {"type"               "https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020"
                       "verificationMethod" did
                       "created"            (util/current-time-iso)
                       "proofPurpose"       "assertionMethod"
                       "jws"                (str jws-header-b64 ".." sig-b64)}]
    {:credential (assoc credential "proof" proof)
     :normalized payload-json}))


(defn generate
  [credentialSubject opts]
  (let [{:keys [did private]} opts
        did* (or (:id did)
                 (str "did:fluree:" (crypto/account-id-from-private private)))]
    (sign
      {"@context"          ["https://www.w3.org/2018/credentials/v1"
                            "https://flur.ee/ns/block"]
       "id"                "blah"
       "type"              ["VerifiableCredential"]
       "issuer"            did*
       "issuanceDate"      (util/current-time-iso)
       "credentialSubject" credentialSubject}
      private)))


(defn verify
  [credential-json]
  (let [cred            (json/parse credential-json false)
        cred-normalized (-> cred
                            (dissoc "proof")
                            json-ld/normalize-data)
        signing-input   (signing-input cred-normalized)
        proof           (get cred "proof")
        proof-did       (get proof "verificationMethod")
        jws             (get proof "jws")                   ;; "eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19..<signature data>"
        [header signature] (str/split jws #"\.\.")
        sig-hex         (alphabase/base-to-base signature :base64 :hex)
        key-id          (crypto/account-id-from-message signing-input sig-hex)
        derived-did     (str "did:fluree:" key-id)]
    (when-not (= jws-header-b64 header)
      ;; TODO - below should use :base64URL once supported
      (throw (ex-info (str "Unsupported jws header in credential: " (alphabase/base-to-base header :base64 :string))
                      {:status 403 :error :json-ld/invalid-credential})))
    (when-not (= derived-did proof-did)
      (throw (ex-info (str "Derived did from signature does not match did in 'proof' of credential. Derived: "
                           derived-did ", proof verificationMethod: " proof-did)
                      {:status 403 :error :json-ld/invalid-credential})))
    cred))



(comment

  (def kp (crypto/generate-key-pair))

  (:private kp)

  (def cred (sign payload (:private kp)))
  cred
  (json/parse cred false)
  (verify cred)


  (def payload {"@context"          ["https://www.w3.org/2018/credentials/v1" "https://flur.ee/ns/block"],
                "id"                "blah",
                "type"              ["VerifiableCredential" "Commit"],
                "issuer"            "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
                "issuanceDate"      "SOMEDATE",
                "credentialSubject" {"@context" ["https://flur.ee/ns/block"
                                                 {"id"               "@id",
                                                  "type"             "@type",
                                                  "rdfs"             "http://www.w3.org/2000/01/rdf-schema#",
                                                  "schema"           "http://schema.org/",
                                                  "wiki"             "https://www.wikidata.org/wiki/",
                                                  "schema:isBasedOn" {"@type" "@id"},
                                                  "schema:author"    {"@type" "@id"}}],
                                     "type"     ["Commit"],
                                     "branch"   "main",
                                     "t"        1,
                                     "message"  "Initial commit",
                                     "assert"   [{"type" "rdfs:Class", "id" "schema:Movie"}
                                                 {"type" "rdfs:Class", "id" "schema:Book"}
                                                 {"type" "rdfs:Class", "id" "schema:Person"}
                                                 {"schema:isBasedOn"                 "wiki:Q3107329",
                                                  "schema:titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                                                  "schema:disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                                                  "schema:name"                      "The Hitchhiker's Guide to the Galaxy",
                                                  "type"                             "schema:Movie",
                                                  "id"                               "wiki:Q836821"}
                                                 {"schema:author" "wiki:Q42",
                                                  "schema:isbn"   "0-330-25864-8",
                                                  "schema:name"   "The Hitchhiker's Guide to the Galaxy",
                                                  "type"          "schema:Book",
                                                  "id"            "wiki:Q3107329"}
                                                 {"schema:name" "Douglas Adams", "type" "schema:Person", "id" "wiki:Q42"}]},})

  )