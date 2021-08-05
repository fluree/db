(ns fluree.db.full-text.block-registry
  (:refer-clojure :exclude [read])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import java.io.File))

(defprotocol BlockRegistry
  (read [r])
  (register [r status])
  (reset [r]))

(defrecord DiskRegistry [^File file]
  BlockRegistry
  (read
    [_]
    (when (.exists file)
      (-> file slurp edn/read-string)))
  (register
    [_ status]
    (->> status prn-str (spit file)))
  (reset
    [_]
    (when (.exists file)
      (io/delete-file file))))

(defn disk-registry
  [base-path]
  (let [path (str/join "/" [base-path "block_registry.edn"])]
    (-> path io/as-file ->DiskRegistry)))

(defrecord MemoryRegistry [state]
  BlockRegistry
  (read
    [_]
    @state)
  (register
    [_ status]
    (reset! state status))
  (reset
    [_]
    (reset! state nil)))

(defn memory-registry
  []
  (->MemoryRegistry (atom nil)))
