(ns fluree.db.json-ld.did
  (:require [fluree.db.api :as fdb]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.crypto :as crypto]
            [alphabase.core :as alphabase]))




(defn document
  "Returns a current, valid did document based on provided address."
  [did-id]
  (go-try
    (let [ledger-id (ledger-from-did did-id)
          did-id    (id-in-ledger did-id)
          db        (load-db)
          did-data  (fdb/query-async db {})]
      ;; format did
      did-data

      ))

  )

(defn validate-credential
  [credential]
  (let [did (did-from-credential credential)
        did-document (document did)
        verification-type :TODO
        public-key :TODO]
    :VERIFY-SIG-BOOLEAN

    ))


(comment

  (def keypair (crypto/generate-key-pair))
  keypair
  (def did-id (str "did:fluree:" (crypto/account-id-from-public (:public keypair))))
  did-id



  (crypto/generate-key-pair)
  (crypto/account-id-from-public "0225354d7658f5339fab821f9ce32df6748853bb31c1cbb13ecdc2fadd95e00ec0")

  (alphabase/byte-array-to-base (byte-array [0x0F 0x02]) :base58)

  (second (byte-array [0x0F 0x02]))

  (nth (alphabase/base-to-byte-array "TfBUa9RhNMTfR7U8y6JVVfwHC4zQ48k8GSb" :base58) 2)


  (def sample-dids
    [
     {"@context" ["https://www.w3.org/ns/did/v1" {"@base" "did:example:123"}],
      "id" "did:example:123",
      "publicKey" [{"id" "#key-0",
                    "type" "Ed25519VerificationKey2020",
                    "controller" "did:example:123",
                    "publicKeyMultibase" "z6Mkf5rGMoatrSj1f4CyvuHBeXJELe9RPdzo2PKGNCKVtZxP"}],
      "authentication" ["#key-0"],
      "assertionMethod" ["#key-0"],
      "capabilityDelegation" ["#key-0"],
      "capabilityInvocation" ["#key-0"]}


     {"@context"             ["https://w3id.org/did/v0.11" "https://w3id.org/veres-one/v1"],
      "id"                   "did:fluree:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
      "publicKey"            [{"id"              "#keys-1",
                               "type"            "Ed25519VerificationKey2018",
                               "controller"      "z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                               "publicKeyBase58" "CkFJJBKPGRzGwMNjeDkJQ8uS8p5BGXxE4ikyK32nNu8B"}]
      "authentication"       [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MkrCWLtRZpbyUk3rDSKni9FETRxPM2gRCakjfu9JzoJ7uZ",
                               "type"            "Ed25519VerificationKey2018",
                               "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                               "publicKeyBase58" "CkFJJBKPGRzGwMNjeDkJQ8uS8p5BGXxE4ikyK32nNu8B"}],
      "capabilityInvocation" [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                               "type"            "Ed25519VerificationKey2018",
                               "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                               "publicKeyBase58" "DkByBFZQnkQhCt6VMRwwM9ed6opzTMiQoTRsC2nEJ1hb"}],
      "capabilityDelegation" [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MkiNjEhhtm4pggXpjBSypei78tSSnP44jpNauV4jtmyiSR",
                               "type"            "Ed25519VerificationKey2018",
                               "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                               "publicKeyBase58" "4vUC7TeKjHCDRKtUmQros1atcsWXeBVTgZzZETvm4Vf3"}],
      "assertionMethod"      [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6Mkozb1pvNKPKUcdukgPTwDsB5mBUh2JZYrf4k3z6vwKoSt",
                               "type"            "Ed25519VerificationKey2018",
                               "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                               "publicKeyBase58" "AYKyEg7t3mz9XQuyhtyP25XmMuRAtgJVy3q89pxvQafW"}]}]

    )


  )

(comment

  ;; referencing a did
  "did:fluree:ipfs:<cid>#<optional-iri>"

  {"@context"             ["https://w3id.org/did/v0.11" "https://w3id.org/veres-one/v1"],
   "id"                   "did:fluree:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
   "publicKey"            [{"id"              "#keys-1",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "CkFJJBKPGRzGwMNjeDkJQ8uS8p5BGXxE4ikyK32nNu8B"}]
   "authentication"       [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MkrCWLtRZpbyUk3rDSKni9FETRxPM2gRCakjfu9JzoJ7uZ",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "CkFJJBKPGRzGwMNjeDkJQ8uS8p5BGXxE4ikyK32nNu8B"}],
   "capabilityInvocation" [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "DkByBFZQnkQhCt6VMRwwM9ed6opzTMiQoTRsC2nEJ1hb"}],
   "capabilityDelegation" [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MkiNjEhhtm4pggXpjBSypei78tSSnP44jpNauV4jtmyiSR",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "4vUC7TeKjHCDRKtUmQros1atcsWXeBVTgZzZETvm4Vf3"}],
   "assertionMethod"      [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6Mkozb1pvNKPKUcdukgPTwDsB5mBUh2JZYrf4k3z6vwKoSt",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "AYKyEg7t3mz9XQuyhtyP25XmMuRAtgJVy3q89pxvQafW"}]}


  {"@context"             ["https://w3id.org/did/v0.11" "https://w3id.org/veres-one/v1"],
   "id"                   "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
   "authentication"       [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MkrCWLtRZpbyUk3rDSKni9FETRxPM2gRCakjfu9JzoJ7uZ",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "CkFJJBKPGRzGwMNjeDkJQ8uS8p5BGXxE4ikyK32nNu8B"}],
   "capabilityInvocation" [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "DkByBFZQnkQhCt6VMRwwM9ed6opzTMiQoTRsC2nEJ1hb"}],
   "capabilityDelegation" [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6MkiNjEhhtm4pggXpjBSypei78tSSnP44jpNauV4jtmyiSR",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "4vUC7TeKjHCDRKtUmQros1atcsWXeBVTgZzZETvm4Vf3"}],
   "assertionMethod"      [{"id"              "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy#z6Mkozb1pvNKPKUcdukgPTwDsB5mBUh2JZYrf4k3z6vwKoSt",
                            "type"            "Ed25519VerificationKey2018",
                            "controller"      "did:v1:test:nym:z6MksCT1mVor8HuAKNwC2zunCFCcvP6qsExmVULo2JkFDEUy",
                            "publicKeyBase58" "AYKyEg7t3mz9XQuyhtyP25XmMuRAtgJVy3q89pxvQafW"}]}

  {"@context" ["https://www.w3.org/ns/did/v1" {"@base" "did:example:123"}],
   "id" "did:example:123",
   "publicKey" [{"id" "#key-0",
                 "type" "Ed25519VerificationKey2020",
                 "controller" "did:example:123",
                 "publicKeyMultibase" "z6Mkf5rGMoatrSj1f4CyvuHBeXJELe9RPdzo2PKGNCKVtZxP"}],
   "authentication" ["#key-0"],
   "assertionMethod" ["#key-0"],
   "capabilityDelegation" ["#key-0"],
   "capabilityInvocation" ["#key-0"]}

  )