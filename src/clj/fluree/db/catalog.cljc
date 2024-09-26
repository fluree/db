(ns fluree.db.catalog
  (:require [clojure.core.async :as async]
            [clojure.pprint :as pprint]
            [fluree.db.storage :as storage]
            [fluree.db.remote-system :as remote-system])
  #?(:clj (:import (java.io Writer))))

(defrecord Catalog [])

#?(:clj
   (defmethod print-method Catalog [^Catalog clg, ^Writer w]
     (.write w (str "#fluree/Catalog "))
     (binding [*out* w]
       (pr (->> clg keys vec))))
   :cljs
     (extend-type Catalog
       IPrintWithWriter
       (-pr-writer [clg w _opts]
         (-write w "#fluree/Catalog ")
         (-write w (pr (->> clg keys vec))))))

(defmethod pprint/simple-dispatch Catalog [^Catalog clg]
  (pr clg))

(defn section-entry
  [section]
  (let [loc (storage/location section)]
    [loc section]))

(defn with-remote-system
  [remote-section remote-system]
  (reduce (fn [sec address-identifier]
            (assoc sec address-identifier remote-system))
          remote-section (remote-system/address-identifiers remote-system)))

(defn remote-systems->section
  [remote-systems]
  (reduce with-remote-system {} remote-systems))

(defn catalog
  ([local-stores remote-systems]
   (let [default-location (-> local-stores first storage/location)]
     (catalog local-stores remote-systems default-location)))
  ([local-stores remote-systems default-location]
   (let [remote-section (remote-systems->section remote-systems)]
     (-> (->Catalog)
         (into (map section-entry) local-stores)
         (assoc ::default default-location, ::remote remote-section)))))

(defn get-local-store
  [clg location]
  (let [location* (if (= location ::default)
                    (get clg ::default)
                    location)]
    (get clg location*)))

(defn get-remote-system
  [clg location]
  (when-let [identifier (storage/get-identifier location)]
    (-> clg ::remote (get identifier))))

(defn locate-address
  [clg address]
  (let [[location _local-path] (storage/split-address address)]
    (or (get-local-store clg location)
        (get-remote-system clg location))))

(defn async-location-error
  [address]
  (let [ex (ex-info (str "Unrecognized storage location:" address)
                    {:status 500, :error :db/unexpected-error})]
    (doto (async/chan)
      (async/put! ex))))

(defn read-location-json
  [clg address]
  (if-let [store (locate-address clg address)]
    (storage/read-json store address)
    (async-location-error address)))

(defn content-write-location-json
  ([clg path data]
   (content-write-location-json clg ::default path data))
  ([clg location path data]
   (if-let [store (get-local-store clg location)]
     (storage/content-write-json store path data)
     (async-location-error location))))
