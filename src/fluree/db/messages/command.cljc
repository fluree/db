(ns fluree.db.messages.command
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]))

(set! *warn-on-reflection* true)

(def max-size 10000000)

(def always?
  (constantly true))

(def never?
  (constantly false))

(defn small?
  [cmd]
  (-> cmd
      count
      (<= max-size)))

(defn no-colon?
  [s]
  (not (str/includes? s ":")))

(defn network?
  [s]
  (some? (re-matches #"^[a-z0-9-]+$" s)))

(defn ledger-id?
  [s]
  (some? (re-matches #"^[a-z0-9-]+$" s)))

(defn ledger-string?
  [s]
  (some? (re-matches #"^[a-z0-9-]+/[a-z0-9-]+$" s)))

(defn with-namespace?
  [kw]
  (-> kw namespace boolean))

(s/def ::id string?)
(s/def ::cmd (s/and string? small?))
(s/def ::sig string?)
(s/def ::signed (s/nilable string?))

(s/def ::signed-cmd
  (s/keys :req-un [::id ::cmd ::sig]
          :opt-un [::signed]))

(s/def ::type keyword?)
(s/def ::action keyword?)

(s/def ::tx (s/or :map  map?
                  :coll (s/coll-of map?)))
(s/def ::qry map?)
(s/def ::deps (s/coll-of string?))
(s/def ::network (s/and string? network?))
(s/def ::ledger-id (s/and string? ledger-id?))
(s/def ::ledger (s/or :pair    (s/tuple ::network ::ledger-id)
                      :string  (s/and string? ledger-string?)
                      :keyword (s/and keyword? with-namespace?)))
(s/def ::snapshot always?)
(s/def ::owners (s/coll-of string?))
(s/def ::private-key string?)
(s/def ::expire pos-int?)
(s/def ::nonce int?)

(defmulti cmd-data-spec :type)

(defmethod cmd-data-spec :tx
  [_]
  (s/keys :req-un [::type ::tx ::ledger]
          :opt-un [::deps ::expire ::nonce]))

(defmethod cmd-data-spec :signed-qry
  [_]
  (s/keys :req-un [::type ::ledger ::action ::qry]
          :opt-un [::expire ::nonce]))

(defmethod cmd-data-spec :new-ledger
  [_]
  (s/keys :req-un [::type ::ledger]
          :opt-un [::auth ::owners ::snapshot ::expire ::nonce]))

(defmethod cmd-data-spec :delete-ledger
  [_]
  (s/keys :req-un [::type ::ledger]))

(defmethod cmd-data-spec :default-key
  [_]
  (s/keys :req-un [::type ::private-key]
          :opt-un [::network ::ledger-id ::expire ::nonce]))

(defmethod cmd-data-spec :default
  [_]
  never?)

(s/def ::cmd-data
  (s/multi-spec cmd-data-spec :type))

(defn throw-invalid
  [message]
  (throw (ex-info message
                  {:status 400
                   :error  :db/invalid-command})))

(defn parse-signed-command
  [msg]
  (let [signed-cmd (s/conform ::signed-cmd msg)]
    (when (s/invalid? signed-cmd)
      (throw-invalid (s/explain-str ::signed-cmd msg)))
    signed-cmd))

(defn parse-json
  [cmd]
  (try
    (-> cmd
        json/parse
        (update :type keyword)
        (update :action keyword))
    (catch Exception _
      (throw-invalid "Invalid command serialization, could not decode JSON."))))

(defn validate
  [cmd-data]
  (let [checked-data (s/conform ::cmd-data (util/without-nils cmd-data))]
    (if (s/invalid? checked-data)
      (throw-invalid (s/explain-str ::cmd-data cmd-data))
      (s/unform ::cmd-data checked-data))))

(defn parse-cmd-data
  [cmd]
  (-> cmd
      parse-json
      validate))

(defn parse-auth-id
  [{:keys [cmd sig signed] :as _parsed-command}]
  (try
    (-> signed
        (or cmd)
        (crypto/account-id-from-message sig))
    (catch Exception _
      (throw-invalid "Invalid signature on command."))))

(defn parse-id
  [cmd-str]
  (crypto/sha3-256 cmd-str))

(defn parse
  [msg]
  (let [{:keys [cmd sig signed] :as signed-cmd}
        (parse-signed-command msg)

        id       (parse-id cmd)
        auth-id  (parse-auth-id signed-cmd)
        cmd-data (parse-cmd-data cmd)]
    {:id         id
     :auth-id    auth-id
     :signed-cmd signed-cmd
     :cmd-data   cmd-data}))

(defn with-auth
  [cmd-data private-key opts]
  (if-let [{:keys [auth] :as verified-auth} (:verified-auth opts)]
    (do (log/debug "Using verified auth:" auth)
        (assoc cmd-data :auth auth))
    (let [key-auth-id (crypto/account-id-from-private private-key)]
      (if-let [auth (:auth opts)]
        (assoc cmd-data
               :auth auth
               :authority (when-not (= auth key-auth-id)
                            key-auth-id))
        (assoc cmd-data :auth key-auth-id)))))

(defn ->tx-command
  [txn ledger private-key timestamp opts]
  (let [{:keys [expire nonce deps]
         :or   {nonce  timestamp
                expire (+ timestamp 300000)}}
        opts

        cmd-data {:type      :tx
                  :ledger    ledger
                  :tx        txn
                  :nonce     nonce
                  :expire    expire
                  :deps      deps}]
    (-> cmd-data
        (with-auth private-key opts)
        util/without-nils
        validate)))

(defn ->default-key-command
  [conn network ledger-id private-key timestamp opts]
  (let [{:keys [expire nonce signing-key]
         :or   {nonce  timestamp
                expire (+ timestamp 30000)}}
        opts

        cmd-data   {:type        :default-key
                    :network     network
                    :ledger-id   ledger-id
                    :private-key private-key
                    :nonce       nonce
                    :expire      expire}]
    (-> cmd-data
        util/without-nils
        validate)))

(defn ->delete-ledger-command
  [ledger timestamp opts]
  (let [{:keys [nonce expire]
         :or   {nonce   timestamp
                expire  (+ timestamp 30000)}}
        opts

        cmd-data {:type   :delete-ledger
                  :ledger ledger
                  :nonce  nonce
                  :expire expire}]
    (-> cmd-data
        util/without-nils
        validate)))

(defn json-serialize
  [cmd-data]
  (try (json/stringify cmd-data)
       (catch Exception _
         (throw (ex-info (str "Transaction contains data that cannot be serialized into JSON.")
                         {:status 400 :error :db/invalid-tx})))))

(defn command->envelope
  [command]
  {:cmd (json-serialize command)})

(defn with-verified-auth
  [envelope {:keys [signature signed]}]
  (assoc envelope
         :sig    signature
         :signed signed))

(defn with-signature
  [{:keys [cmd] :as envelope} private-key]
  (let [sig (crypto/sign-message cmd private-key)]
    (assoc envelope :sig sig)))

(defn with-id
  [{:keys [cmd] :as envelope}]
  (let [id (crypto/sha3-256 cmd)]
    (assoc envelope :id id)))

(defn sign
  ([command private-key]
   (sign command private-key {}))
  ([command private-key opts]
   (let [envelope (-> command command->envelope with-id)]
     (if-let [verified-auth (:verified-auth opts)]
       (with-verified-auth envelope verified-auth)
       (with-signature envelope private-key)))))

(defn build-and-sign-tx
  [txn ledger timestamp private-key opts]
  (-> txn
      (->tx-command ledger private-key timestamp opts)
      (sign private-key opts)
      with-id))
