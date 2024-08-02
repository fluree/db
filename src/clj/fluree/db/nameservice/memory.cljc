(ns fluree.db.nameservice.memory
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.db.platform :as platform]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn memory-address
  "Turn a path into a fluree memory address."
  [path]
  (str "fluree:memory://" path))

(defn- address-path
  "Returns the path portion of a Fluree memory address."
  [address]
  (if-let [[_ path] (re-find #"^fluree:memory://(.+)$" address)]
    path
    (throw (ex-info (str "Incorrectly formatted Fluree memory db address: " address)
                    {:status 500 :error :db/invalid-db}))))

(defn- read-address
  [data-atom address]
  (let [addr-path (address-path address)]
    #?(:clj  (get @data-atom addr-path)
       :cljs (or (get @data-atom addr-path)
                 (and platform/BROWSER (.getItem js/localStorage addr-path))))))

(defn push!
  [data-atom {commit-address   "address"
              nameservice-iris "ns"
              :as              commit-data}]
  (go
    (let [my-ns-iri (->> (map #(get % "id") nameservice-iris)
                         (some #(when (re-matches #"^fluree:memory:.+" %) %)))
          commit-path (address-path commit-address)
          head-path   (address-path my-ns-iri)]
      (swap! data-atom
             (fn [state]
               (let [commit (get state commit-path)]
                 (when-not commit
                   (throw (ex-info (str "Unable to locate commit in memory, cannot push!: " commit-address)
                                   {:status 500 :error :db/invalid-db})))
                 (log/debug "pushing:" my-ns-iri "referencing commit:" commit-address)
                 (let [commit (assoc commit "address" commit-address)]
                   (assoc state head-path commit)))))
      #?(:cljs (and platform/BROWSER (.setItem js/localStorage address-path commit-path)))
      commit-data)))

(defn lookup
  [data-atom ledger-alias]
  (go #?(:clj
         (when-let [head-commit (read-address data-atom ledger-alias)]
           (-> head-commit (get "address")))

         :cljs
         (if platform/BROWSER
           (when-let [head-commit (read-address data-atom ledger-alias)]
             (memory-address head-commit))
           (throw (ex-info (str "Cannot lookup ledger address with memory connection: "
                                ledger-alias)
                           {:status 500 :error :db/invalid-ledger}))))))

(defn ledger-list
  [state-atom opts]
  (go (-> @state-atom keys)))

(defn address
  [ledger-alias {:keys [branch] :as _opts}]
  (go (memory-address (str ledger-alias "/" (name branch) "/head"))))

(defrecord MemoryNameService
  [state-atom sync?]
  ns-proto/Publisher
  (-push [_ commit-data] (push! state-atom commit-data))

  ns-proto/iNameService
  (-lookup [_ ledger-alias] (lookup state-atom ledger-alias))
  (-lookup [_ ledger-alias opts] (lookup state-atom ledger-alias)) ;; TODO - doesn't support branches yet
  (-sync? [_] sync?)
  (-ledgers [_ opts] (ledger-list state-atom opts))
  (-address [_ ledger-alias opts]
    (address ledger-alias opts))
  (-alias [_ ledger-address]
    (-> (address-path ledger-address)
        (str/split #"/")
        (->> (drop 2)
             (str/join "/"))))
  (-close [nameservice] (reset! state-atom {})))


(defn initialize
  [state-atom]
  (map->MemoryNameService {:state-atom state-atom
                           :sync?      true}))
