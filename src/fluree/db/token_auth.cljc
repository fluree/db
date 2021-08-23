(ns fluree.db.token-auth
  (:require [fluree.crypto.hmac :refer [hmac-sha256]]
            [alphabase.core :as alphabase]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [clojure.string :as str]
            [fluree.db.util.core :as util]))

#?(:clj #?(:clj (set! *warn-on-reflection* true)))

(defn- base64->base64url
  [b64]
  (-> b64
      (str/replace #"=+$" "")
      (str/replace #"\+" "-")
      (str/replace #"/" "_")))

;;;;;;;;;;;;;;;;;;
;;; JWT operations

(def ^:private jwt-header {:alg "HS256"
                           :typ "JWT"})

(def ^:private jwt-header-enc (-> jwt-header
                                  json/stringify-UTF8
                                  alphabase/bytes->base64
                                  base64->base64url))


(defn- generate-jwt-sig
  "Generates jwt signature as base64URL using secret given a string token."
  [secret token]
  (-> token
      alphabase/string->bytes
      (hmac-sha256 secret)
      alphabase/bytes->base64
      base64->base64url))


(defn generate-jwt
  "Generates a HS256 JWT token containing data as a map and secured with secret."
  [secret payload]
  (let [data-enc (-> payload
                     json/stringify-UTF8
                     alphabase/bytes->base64
                     base64->base64url)
        token    (str jwt-header-enc "." data-enc)
        hs256    (generate-jwt-sig secret token)]
    (str token "." hs256)))


(defn verify-jwt
  "Returns the JWT payload as map if valid, else an exception."
  [secret jwt]
  (let [[header payload sig] (str/split jwt #"\.")
        header*  (try*
                   (-> header
                       alphabase/base64->bytes
                       json/parse)
                   (catch* e
                           (throw (ex-info "Invalid JWT header."
                                           {:status 401
                                            :error  :db/invalid-token
                                            :meta   {:jwt    jwt
                                                     :reason (.getMessage e)}}))))
        token    (str header "." payload)
        sig*     (generate-jwt-sig secret token)
        _        (when (not= header* jwt-header)
                   (throw (ex-info "Invalid JWT header. Only HS256 algorithm supported."
                                   {:status 401
                                    :error  :db/invalid-token
                                    :meta   {:jwt    jwt
                                             :header header*}})))
        _        (when (not= sig* sig)
                   (throw (ex-info "Invalid JWT signature."
                                   {:status 401
                                    :error  :db/invalid-token
                                    :meta   {:jwt jwt}})))
        payload* (try*
                   (-> payload
                       alphabase/base64->bytes
                       json/parse)
                   (catch* e
                           (throw (ex-info "Invalid JWT payload."
                                           {:status 401
                                            :error  :db/invalid-token
                                            :meta   {:jwt    jwt
                                                     :reason (.getMessage e)}}))))]
    (when (and (:exp payload*)
               (< (:exp payload*) (util/current-time-millis)))
      (throw (ex-info "JWT has expired."
                      {:status 401
                       :error  :db/expired-token
                       :meta   {:jwt jwt}})))
    payload*))
