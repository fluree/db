(ns fluree.db.query.http-signatures
  (:require [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.json :as json])
  #?(:clj (:import (java.time ZoneOffset ZonedDateTime)
                   (java.time.format DateTimeFormatter)
                   (java.net URL))))

#?(:clj (set! *warn-on-reflection* true))

;; signatures of http requests. see:
;; https://tools.ietf.org/id/draft-cavage-http-signatures-08.html

(defn generate-signing-string
  "keys must include:
  method - post, get, etc. - used for (request-target) which is required
  path - path of the request
  "
  [val-map ks]
  (when-not (and (some #{"(request-target)"} ks)
                 (some #{"date" "x-fluree-date" "mydate"} ks)) ;; 'mydate' is an option here for legacy fluree-cryptography library use, deprecate
    (throw (ex-info (str "A valid http-signature must sign at least (request-target), date (or x-fluree-date), "
                         "and digest if the request has a body. You requested a signature based on: " (pr-str ks) ".")
                    {:status 400
                     :error  :db/invalid-auth})))
  (let [get-or-throw (fn [m k]
                       (or (get m k)
                           (throw (ex-info (str "Signing string component " k " is not present.")
                                           {:status 401 :error :db/invalid-auth}))))]
    (->> (reduce
           (fn [acc k]
             (if (= "(request-target)" k)
               (conj acc (str "(request-target): "
                              (str/lower-case (name (get-or-throw val-map "method")))
                              " " (str/lower-case (get-or-throw val-map "path"))))
               (conj acc (str k ": " (get-or-throw val-map k)))))
           [] ks)
         (str/join "\n"))))


(defn verify-signature-header*
  [req sig-header-str]
  (let [{:keys [request-method uri headers]} req
        sig-map     (try*
                      (->> (str/split sig-header-str #"\",")
                           (map #(-> %
                                     (str/replace #"\"" "")
                                     (str/split #"=")))
                           (into {}))
                      (catch* _
                        (throw (ex-info (str "Invalid Signature header. Ensure you have "
                                             "key1=value1,key2=value2 format and all string "
                                             "values are inside double-quotes.")
                                        {:status 400
                                         :error  :db/invalid-auth}))))
        sig-parts   (str/split (get sig-map "headers" "") #" ")
        sign-string (generate-signing-string (assoc headers "method" request-method
                                                            "path" uri)
                                             sig-parts)
        signature   (get sig-map "signature")
        authority   (crypto/account-id-from-message sign-string signature)
        key-id      (get sig-map "keyId")
        auth        (if (= "na" key-id) nil key-id)]
    (log/debug "Verifying signature. Sign string:"
               sign-string "Signature:" signature "Account Id:" authority)
    {:auth      (or auth authority)
     :authority authority
     :type      :http-signature
     :signed    sign-string
     :signature signature}))


(defn verify-signature-header
  "Verifies signed http request. If signature header does not exist,
  returns nil. If it exists and is valid, returns the authid associated
  with it. If it exists and is invalid, throws exception."
  [req]
  (when-let [sig-header-str (get (:headers req) "signature")]
    (log/trace "Verifying http signature header:" sig-header-str)
    (verify-signature-header* req sig-header-str)))


(defn verify-digest
  "If a message digest is present, verifies it."
  [req]
  (let [digest (get-in req [:headers "digest"])]
    (if (and digest (not= digest "null"))
      (let [[_ hash-type hash] (re-find #"^([^=]+)=(.+)$" digest)
            _           (when-not (#{"SHA-256"} hash-type)
                          (throw (ex-info (str "Digest type of " hash-type " is not supported.")
                                          {:status 401 :error :db/invalid-auth})))
            body        (:body req)
            body        (if (or (nil? body) (string? body)) body (json/stringify body))
            _           (log/debug "verify-digest request body:" body)
            calc-digest (case hash-type
                          "SHA-256" (crypto/sha2-256 body :base64))
            _           (log/debug "request digest:" hash "computed digest:" calc-digest)
            valid?      (= hash calc-digest)]
        (if-not valid?
          (throw (ex-info (str "Invalid digest.")
                          {:status 401 :error :db/invalid-auth}))
          req))
      req)))


(defn sign-request
  "Signs http request by creating required headers and using supplied private key.
  req-type should be :get, :post, etc."
  ([req-method url request private-key]
   (sign-request req-method url request private-key nil))
  ([req-method url request private-key auth]
   (let [{:keys [headers body]} request
         path (let [match (re-find #"^(https?\:)//(([^:/?#]*)(?:\:([0-9]+))?)([/]{0,1}[^?#]*)$" url)]
                (get match 5))
         date             (or (get headers "date")
                              #?(:clj  (-> (DateTimeFormatter/RFC_1123_DATE_TIME) (.format (ZonedDateTime/now (ZoneOffset/UTC))))
                                 :cljs (.toUTCString (js/Date.))))
         digest           (when body (str "SHA-256=" (crypto/sha2-256 body :base64)))
         sign-headers     (if body
                            ["(request-target)" "x-fluree-date" "digest"]
                            ["(request-target)" "x-fluree-date"])
         sign-vals        (if body
                            {"x-fluree-date" date "digest" digest "method" req-method "path" path}
                            {"x-fluree-date" date "method" req-method "path" path})
         signing-string   (generate-signing-string sign-vals sign-headers)
         sign-headers-str (str/join " " sign-headers)
         sig              (crypto/sign-message signing-string private-key)
         auth             (or auth "na")
         ;; we put "Date" in the sig header because browsers will not allow a Date header
         sig-header       (str "keyId=\"" auth "\",headers=\"" sign-headers-str "\",algorithm=\"ecdsa-sha256\",signature=\"" sig "\",date=\"" date "\"")
         headers*         (util/without-nils
                            (merge headers {"Digest"        digest
                                            "X-Fluree-Date" date
                                            "Signature"     sig-header}))]
     (assoc request :headers headers*))))


(defn verify-request*
  "Returns auth record from separated request parts."
  ([req method action db-name]
   (verify-request* req method
                    (str "/fdb/" (util/keyword->str db-name)
                         "/" (util/keyword->str action))))
  ([req method uri]
   (-> req
       verify-digest
       (assoc :request-method method
              :uri uri)
       verify-signature-header)))


(defn verify-request
  "Returns map of auth and authority from request."
  [request]
  (-> request
      verify-digest
      verify-signature-header))



(comment

  (def sig-str (generate-signing-string {"date"   "Tue, 11 Mar 2019 19:20:12 GMT"
                                         "method" :post
                                         "path"   "/fdb/test/one/query"
                                         "digest" "SHA-256=7H1ZpcmGuWumeVlRzVWnfrYLfDyamUg5Y8Km49rb/c8="}
                                        ["(request-target)" "date" "digest"]))

  (def myrequest {:headers {"content-type" "application/json"}
                  :body    (fluree.db.util.json/stringify {:select ["*"] :from "_collection"})})

  (def signed-request
    (sign-request :post "http://localhost:8090/fdb/test/permissions/query" myrequest "78f2ee93ef8008a270ffad949799462474f44c1ee8b29f07ec4fe1ae965b92c"))


  signed-request

  (verify-request* signed-request :query "test/permissions" "localhost"))
