(ns fluree.db.json-ld.credential
  (:require [fluree.db.util.json :as json]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.crypto :as crypto]
            [alphabase.core :as alphabase]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(def jws-header
  "The JWS header for a secp256k1 signing key."
  ;; https://github.com/decentralized-identity/EcdsaSecp256k1RecoverySignature2020
  {"alg"  "ES256K-R"
   "b64"  false
   "crit" ["b64"]})


(def jws-header-json (json-ld/normalize-data jws-header {:algorithm :basic
                                                         :format    :application/json}))


(def jws-header-b64 (alphabase/base-to-base jws-header-json :string :base64url))


#_(defn credential-json
    "Takes final credential response object (as returned by sign-credential)
    and formats JSON document ready for publishing."
    [credential-object]
    (let [{:keys [credential normalized]} credential-object
          proof            (get credential "proof")
          ;; TODO: implement URDNA2015
          proof-normalized (json-ld/normalize-data proof)]
      (str (subs normalized 0 (dec (count normalized))) ;; remove trailing '}', then add back
           ",\"proof\":" proof-normalized "}")))


(defn signing-input
  "JOSE JWS signing input is b64URL of header, + '.' + b64URL of json to be signed.
  The resulting input is hashed with SHA-256, and that result is what is signed."
  [payload]
  (str jws-header-b64 "." (alphabase/base-to-base payload :string :base64url)))


(defn serialize-jws
  "Serialize a JWS according to the JOSE specification, compact form."
  [signing-input signature]
  (str signing-input "." signature))


(defn deserialize-jws
  "Deserialize a compact JWS into its component parts"
  [jws]
  (let [[header payload sig] (str/split jws #"\.")]
    {:header    (alphabase/base-to-base header :base64url :string)
     :payload   (alphabase/base-to-base payload :base64url :string)
     :signature (alphabase/base-to-base sig :base64url :hex)}))


(defn sign
  "Given a payload and a signing key, returns a JOSE JSON Web Signature."
  [payload signing-key]
  (let [signing-input (signing-input payload)
        signature     (crypto/sign-message signing-input signing-key)
        b64-signature (alphabase/base-to-base signature :hex :base64url)]
    (serialize-jws signing-input b64-signature)))


(defn create-proof
  "Given a canonicalized credential-subject, create a proof for it."
  ;; TODO: this is using a custom proof @type, and it should use EcdsaSecp256k1RecoverySignature2020 which requires RDF normalization
  [credential-subject issuer signing-key]
  {"type"               "https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020"
   "created"            (util/current-time-iso)
   "verificationMethod" issuer
   "proofPurpose"       "assertionMethod"
   "jws"                (sign credential-subject signing-key)})


(defn cred-id
  "Generates credential id from hash within credential subject."
  [credential-subject hash-key]
  (let [hash-iri (get credential-subject hash-key)
        [_ hash] (re-find #"^urn:sha256:(.+)$" hash-iri)]
    (str "fluree:sha256:" hash)))


(defn generate
  "Generate a VerifiableCredential given a subject and some issuer opts."
  [credential-subject {:keys [did private hash-key] :as opts}]
  (let [did* (or (:id did)
                 (str "did:fluree:" (crypto/account-id-from-private private)))
        id   (cred-id credential-subject hash-key)]
    {"@context"          "https://www.w3.org/2018/credentials/v1"
     "id"                id
     "type"              ["VerifiableCredential"]
     "issuer"            did*
     "issuanceDate"      (util/current-time-iso)
     "credentialSubject" credential-subject
     "proof"             (create-proof (json-ld/normalize-data credential-subject) did* private)}))


(defn verify
  "Takes a credential"
  [credential]
  (try*
    (let [proof-did     (get-in credential ["proof" "verificationMethod"])
          jws           (get-in credential ["proof" "jws"])
          {:keys [header payload signature]} (deserialize-jws jws)
          signing-input (signing-input payload)
          key-id        (crypto/account-id-from-message signing-input signature)
          derived-did   (str "did:fluree:" key-id)]
      (cond-> {:credential credential}
              (not= jws-header-json header)
              (update :errors (fnil conj [])
                      {:error :credential/unknown-signing-algorithm
                       :message (str "Unsupported jws header in credential: " header)})

              (not= derived-did proof-did)
              (update :errors (fnil conj [])
                      {:error :credential/invalid-signature
                       :message (str "Derived did from signature does not match did in 'proof' of credential. Derived: "
                                     derived-did ", proof verificationMethod: " proof-did)})))
    (catch* e
            {:credential credential
             :errors [{:error :credential/unverifiable
                       :message e}]})))

(comment
  (def in "{\"@context\":[\"https://www.w3.org/2018/credentials/v1\",\"https://flur.ee/ns/block\"],\"credentialSubject\":{\"@context\":[\"https://flur.ee/ns/block\"],\"@type\":[\"https://flur.ee/ns/block/Commit\"],\"https://flur.ee/ns/block/branchName\":\"main\",\"https://flur.ee/ns/block/time\":\"2022-03-16T06:06:49.426747Z\",\"https://flur.ee/ns/block/tx-hash\":\"urn:sha256:3d97c6550bfb7e8e33e9d884ec753016857f680a2790a9f85cdfdc08d516458d\",\"https://flur.ee/ns/block/txs\":[{\"@context\":{},\"https://flur.ee/ns/tx/assert\":[{\"@id\":\"eb1332cb-3395-4ecc-a526-4129f0cbeaea\",\"book/author\":\"Neal Stephenson\",\"book/title\":\"Anathem\"}],\"https://flur.ee/ns/tx/t\":1},{\"@context\":{},\"https://flur.ee/ns/tx/assert\":[{\"@id\":\"fee1ea15-7caf-4b40-a069-2110cded0600\",\"book/author\":\"Neal Stephenson\",\"book/title\":\"Cryptonomicon\"}],\"https://flur.ee/ns/tx/t\":2},{\"@context\":{},\"https://flur.ee/ns/tx/assert\":[{\"@id\":\"b3e2cf9d-dbe5-4e93-bae5-def04fbdb69f\",\"book/author\":\"brandon sanderson\",\"book/title\":\"mistborn\"}],\"https://flur.ee/ns/tx/t\":3},{\"@context\":{},\"https://flur.ee/ns/tx/assert\":[{\"@id\":\"b3e2cf9d-dbe5-4e93-bae5-def04fbdb69f\",\"book/author\":\"Brandon Sanderson\",\"book/title\":\"Mistborn\"}],\"https://flur.ee/ns/tx/retract\":[{\"@id\":\"b3e2cf9d-dbe5-4e93-bae5-def04fbdb69f\",\"book/author\":\"brandon sanderson\",\"book/title\":\"mistborn\"}],\"https://flur.ee/ns/tx/t\":4},{\"@context\":{},\"https://flur.ee/ns/tx/assert\":[{\"@id\":\"49ad3fc7-64ae-4f68-85d6-5cb7f5a58fd9\",\"book/author\":\"China Mieville\",\"book/title\":\"The City & The City\"}],\"https://flur.ee/ns/tx/t\":5}]},\"id\":\"https://flur.ee/ns/credential\",\"issuanceDate\":\"2022-03-16T06:06:49.429686Z\",\"issuer\":\"did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6\",\"proof\":{\"created\":\"2022-03-16T06:06:49.430023Z\",\"jws\":\"eyJhbGciOiJFUzI1NkstUiIsImI2NCI6ZmFsc2UsImNyaXQiOlsiYjY0Il19.eyJAY29udGV4dCI6WyJodHRwczovL2ZsdXIuZWUvbnMvYmxvY2siXSwiQHR5cGUiOlsiaHR0cHM6Ly9mbHVyLmVlL25zL2Jsb2NrL0NvbW1pdCJdLCJodHRwczovL2ZsdXIuZWUvbnMvYmxvY2svYnJhbmNoTmFtZSI6Im1haW4iLCJodHRwczovL2ZsdXIuZWUvbnMvYmxvY2svdGltZSI6IjIwMjItMDMtMTZUMDY6MDY6NDkuNDI2NzQ3WiIsImh0dHBzOi8vZmx1ci5lZS9ucy9ibG9jay90eC1oYXNoIjoidXJuOnNoYTI1NjozZDk3YzY1NTBiZmI3ZThlMzNlOWQ4ODRlYzc1MzAxNjg1N2Y2ODBhMjc5MGE5Zjg1Y2RmZGMwOGQ1MTY0NThkIiwiaHR0cHM6Ly9mbHVyLmVlL25zL2Jsb2NrL3R4cyI6W3siQGNvbnRleHQiOnt9LCJodHRwczovL2ZsdXIuZWUvbnMvdHgvYXNzZXJ0IjpbeyJAaWQiOiJlYjEzMzJjYi0zMzk1LTRlY2MtYTUyNi00MTI5ZjBjYmVhZWEiLCJib29rL2F1dGhvciI6Ik5lYWwgU3RlcGhlbnNvbiIsImJvb2svdGl0bGUiOiJBbmF0aGVtIn1dLCJodHRwczovL2ZsdXIuZWUvbnMvdHgvdCI6MX0seyJAY29udGV4dCI6e30sImh0dHBzOi8vZmx1ci5lZS9ucy90eC9hc3NlcnQiOlt7IkBpZCI6ImZlZTFlYTE1LTdjYWYtNGI0MC1hMDY5LTIxMTBjZGVkMDYwMCIsImJvb2svYXV0aG9yIjoiTmVhbCBTdGVwaGVuc29uIiwiYm9vay90aXRsZSI6IkNyeXB0b25vbWljb24ifV0sImh0dHBzOi8vZmx1ci5lZS9ucy90eC90IjoyfSx7IkBjb250ZXh0Ijp7fSwiaHR0cHM6Ly9mbHVyLmVlL25zL3R4L2Fzc2VydCI6W3siQGlkIjoiYjNlMmNmOWQtZGJlNS00ZTkzLWJhZTUtZGVmMDRmYmRiNjlmIiwiYm9vay9hdXRob3IiOiJicmFuZG9uIHNhbmRlcnNvbiIsImJvb2svdGl0bGUiOiJtaXN0Ym9ybiJ9XSwiaHR0cHM6Ly9mbHVyLmVlL25zL3R4L3QiOjN9LHsiQGNvbnRleHQiOnt9LCJodHRwczovL2ZsdXIuZWUvbnMvdHgvYXNzZXJ0IjpbeyJAaWQiOiJiM2UyY2Y5ZC1kYmU1LTRlOTMtYmFlNS1kZWYwNGZiZGI2OWYiLCJib29rL2F1dGhvciI6IkJyYW5kb24gU2FuZGVyc29uIiwiYm9vay90aXRsZSI6Ik1pc3Rib3JuIn1dLCJodHRwczovL2ZsdXIuZWUvbnMvdHgvcmV0cmFjdCI6W3siQGlkIjoiYjNlMmNmOWQtZGJlNS00ZTkzLWJhZTUtZGVmMDRmYmRiNjlmIiwiYm9vay9hdXRob3IiOiJicmFuZG9uIHNhbmRlcnNvbiIsImJvb2svdGl0bGUiOiJtaXN0Ym9ybiJ9XSwiaHR0cHM6Ly9mbHVyLmVlL25zL3R4L3QiOjR9LHsiQGNvbnRleHQiOnt9LCJodHRwczovL2ZsdXIuZWUvbnMvdHgvYXNzZXJ0IjpbeyJAaWQiOiI0OWFkM2ZjNy02NGFlLTRmNjgtODVkNi01Y2I3ZjVhNThmZDkiLCJib29rL2F1dGhvciI6IkNoaW5hIE1pZXZpbGxlIiwiYm9vay90aXRsZSI6IlRoZSBDaXR5ICYgVGhlIENpdHkifV0sImh0dHBzOi8vZmx1ci5lZS9ucy90eC90Ijo1fV19.HDBEAiA4cRt_SxUN6QrD_5TVsP0usSO-xQO5V0oGoaN_djW0PAIgWTo3nNcgqds93a4Cn5i83PvN52BQ-MdksY-aC9ikAjc=\",\"proofPurpose\":\"assertionMethod\",\"type\":\"https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020\",\"verificationMethod\":\"did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6\"},\"type\":[\"VerifiableCredential\"]}")

  (def did {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
            :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
            :public "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca"})

  (verify (json/parse in false))





  (def kp (crypto/generate-key-pair))
  (def payload {"@context"          ["https://www.w3.org/2018/credentials/v1" "https://flur.ee/ns/block"],
                "id"                "blah",
                "type"              ["VerifiableCredential" "Commit"],
                "issuer"            "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
                "issuanceDate"      "SOMEDATE",
                "credentialSubject" {"@context" ["https://flur.ee/ns/block"
                                                 {"id" "@id" ,
                                                  "type" "@type" ,
                                                  "rdfs" "http://www.w3.org/2000/01/rdf-schema#" ,
                                                  "schema" "http://schema.org/" ,
                                                  "wiki" "https://www.wikidata.org/wiki/" ,
                                                  "schema:isBasedOn" {"@type" "@id"} ,
                                                  "schema:author" {"@type" "@id"}}] ,
                                     "type" ["Commit"] ,
                                     "branch" "main" ,
                                     "t" 1,
                                     "message" "Initial commit" ,
                                     "assert" [{"type" "rdfs:Class" , "id" "schema:Movie"}
                                               {"type" "rdfs:Class" , "id" "schema:Book"}
                                               {"type" "rdfs:Class" , "id" "schema:Person"}
                                               {"schema:isBasedOn" "wiki:Q3107329" ,
                                                "schema:titleEIDR" "10.5240/B752-5B47-DBBE-E5D4-5A3F-N" ,
                                                "schema:disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings" ,
                                                "schema:name" "The Hitchhiker's Guide to the Galaxy" ,
                                                "type" "schema:Movie" ,
                                                "id" "wiki:Q836821"}
                                               {"schema:author" "wiki:Q42" ,
                                                "schema:isbn" "0-330-25864-8" ,
                                                "schema:name" "The Hitchhiker's Guide to the Galaxy" ,
                                                "type" "schema:Book" ,
                                                "id" "wiki:Q3107329"}
                                               {"schema:name" "Douglas Adams" , "type" "schema:Person" , "id" "wiki:Q42"}]}})

  (def cred (sign payload (:private kp)))

  (verify-jws (:credential cred))

  (def jws (-> cred :credential (get "proof") (get "jws")))

  (= (json-ld/normalize-data payload)
     (:payload (jose-deserialize-compact jws)))

  (verify-jws (:credential cred))
  ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6" "did:fluree:Tf25LUDWuX57vWi2ZddXxmkQLWv5TD5UR5h"]




  (-> (:credential cred)
      (json/stringify )
      (json/parse false)
      (get "proof")
      (dissoc "jws"))
  {"type" "https://flur.ee/ns/v1#EcdsaSecp256k1RecoverySignature2020", "created" "2022-01-26T17:41:18.374232Z", "verificationMethod" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6", "proofPurpose" "assertionMethod"}


  (:normalized cred)



  (verify cred)
  ,)
