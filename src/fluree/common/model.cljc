(ns fluree.common.model
  (:require [malli.core :as m]
            [malli.error :as me]))

(defn valid?
  [model x]
  (m/validate model x))

(defn explain
  [model x]
  (m/explain model x))

(defn report
  [explanation]
  (me/humanize explanation))

(def Did
  [:map
   [:private :string]
   [:public :string]
   [:id :string]])

(def TrustPolicy
  [:orn
   [:open [:enum :all]]
   [:whitelist [:sequential :string]]])

(def DistrustPolicy
  [:orn
   [:closed [:enum :all]]
   [:blacklist [:sequential :string]]])

(defn valid-trust-policy?
  "Is the given trust/distrust policy logically coherent?

  trust    - :all | [<pubkey> ...]
  distrust - :all | [<pubkey> ...]
  "
  [trust distrust]
  (or ;; if both
    (and trust distrust
         ;; only certain combinations
         (or
           ;; open + blacklist
           (and (= trust :all) (sequential? distrust))
           ;; closed + whitelist
           (and (= distrust :all) (sequential? trust))
           ;; whitelist + blacklist
           (and (sequential? trust)
                (sequential? distrust))))
    ;; one or the other is required
    (or trust distrust)))
