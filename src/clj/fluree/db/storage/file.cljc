(ns fluree.db.storage.file
  (:require [fluree.db.storage :refer [Storage]]
            [clojure.string :as str]
            #?@(:cljs [["fs" :as fs]
                       ["path" :as path]])
            #?(:clj [clojure.java.io :as io])
            [fluree.db.util.log :as log :include-macros true])
  #?(:clj
     (:import (java.io ByteArrayOutputStream FileNotFoundException))))

#?(:clj (set! *warn-on-reflection* true))

(defn build-path
  [& components]
  (str/join "/" components))

(defn read-file
  [path]
  #?(:clj
     (try
       (with-open [xin  (io/input-stream path)
                   xout (ByteArrayOutputStream.)]
         (io/copy xin xout)
         (.toByteArray xout))

       (catch FileNotFoundException _
         nil))
     :cljs
     (try
       (fs/readFileSync path)
       (catch :default e
         (when (not= "ENOENT" (.-code e))
           (throw (ex-info "Error reading file."
                           {"errno"   ^String (.-errno e)
                            "syscall" ^String (.-syscall e)
                            "code"    (.-code e)
                            "path"    (.-path e)})))))))

(defn file-exists?
  [path]
  #?(:clj (.exists (io/file path))
     :cljs (fs/existsSync path)))

(defn write-file
  "Write bytes to disk at the given file path."
  [path ^bytes val]
  #?(:clj
     (try
       (with-open [out (io/output-stream (io/file path))]
         (.write out val))
       (catch FileNotFoundException _
         (try
           (io/make-parents (io/file path))
           (with-open [out (io/output-stream (io/file path))]
             (.write out val))
           (catch Exception e
             (log/error (str "Unable to create storage directory: " path
                             " with error: " (.getMessage e) "."))
             (log/error (str "Fatal Error, shutting down!"))
             (System/exit 1))))
       (catch Exception e (throw e)))
     :cljs
     (try
       (fs/writeFileSync path val)
       (catch :default e
         (if (= (.-code e) "ENOENT")
           (try
             (fs/mkdirSync (path/dirname path) #js{:recursive true})
             (try
               (fs/writeFileSync path val)
               (catch :default e
                 (log/error (str "Unable to write file to path " path
                                 " with error: " ^String (.-message e) "."))
                 (log/error (str "Fatal Error, shutting down! "
                                 {"errno"   ^String (.-errno e)
                                  "syscall" ^String (.-syscall e)
                                  "code"    (.-code e)
                                  "path"    (.-path e)}))
                 (js/process.exit 1)))
             (catch :default e
               (log/error (str "Unable to create storage directory: " path
                               " with error: " ^String (.-message e) "."))
               (log/error (str "Fatal Error, shutting down!"))
               (js/process.exit 1)))
           (throw (ex-info "Error writing file."
                           {"errno"   ^String (.-errno e)
                            "syscall" ^String (.-syscall e)
                            "code"    (.-code e)
                            "path"    (.-path e)})))))))

(defrecord FileStore [storage-dir]
  Storage
  (-read [_ local-path]
    (let [path (build-path storage-dir local-path)]
      (read-file path)))
  (-exists? [_ local-path]
    (let [path (build-path storage-dir local-path)]
      (file-exists? path)))
  (-list [_ path])
  (-write [_ local-path v]
    (let [path (build-path storage-dir local-path)]
      (write-file path v)))
  (-delete [_ path]))

(def current-dir
  (str #?(:clj  (.getAbsolutePath (io/file ""))
          :cljs (path/resolve ".")) "/"))

(defn absolute-path?
  [path]
  #?(:clj (.isAbsolute (io/file path))
     :cljs (path/isAbsolute path)))

(defn absolute-path
  [path]
  (if (absolute-path? path)
    path
    (build-path current-dir path)))

(defn path->canonical-path
  [path]
  (let [absolute (absolute-path path)]
    #?(:clj  (-> absolute io/file .getCanonicalPath)
       :cljs (path/resolve absolute))))

(defn file-store
  [storage-dir]
  (-> storage-dir path->canonical-path ->FileStore))
