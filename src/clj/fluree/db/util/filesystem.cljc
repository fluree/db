(ns fluree.db.util.filesystem
  (:refer-clojure :exclude [exists?])
  (:require [fluree.db.util.log :as log]
            #?(:clj [clojure.java.io :as io])
            #?@(:cljs [["fs" :as fs]
                       ["path" :as path]])
            [clojure.core.async :as async])
  #?(:clj (:import (java.io ByteArrayOutputStream FileNotFoundException File))))

#?(:clj (set! *warn-on-reflection* true))

(defn write-file
  "Write bytes to disk at the given file path."
  [path ^bytes val]
  #?(:clj
     (async/thread
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
         (catch Exception e (throw e))))
     :cljs
     (async/go
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
                              "path"    (.-path e)}))))))))

(defn read-file
  "Read a string from disk at `path`. Returns nil if file does not exist."
  [path]
  #?(:clj
     (async/thread
       (try
         (with-open [xin  (io/input-stream path)
                     xout (ByteArrayOutputStream.)]
           (io/copy xin xout)
           (String. (.toByteArray xout)))

         (catch FileNotFoundException _
           nil)))
     :cljs
     (async/go
       (try
         (fs/readFileSync path "utf8")
         (catch :default e
           (when (not= "ENOENT" (.-code e))
             (throw (ex-info "Error reading file."
                             {"errno"   ^String (.-errno e)
                              "syscall" ^String (.-syscall e)
                              "code"    (.-code e)
                              "path"    (.-path e)}))))))))

(defn delete-file
  "Delete the file at `path`."
  [path]
  #?(:clj
     (async/thread
       (try
         (io/delete-file (io/file path))
         :deleted
         (catch Exception e
           (log/trace (str "Failed to delete file: " path))
           e)))
     :cljs
     (async/go
       (try
         (fs/unlinkSync path)
         :deleted
         (catch :default e
           (log/trace (str "Failed to delete file: " path))
           e)))))

(defn list-files
  [path]
  #?(:clj
     (async/thread
       (try
         (map #(.getName ^File %)
              (.listFiles (io/file path)))
         (catch Exception e
           (log/error e (str "Failed to list files at path: " path))
           (throw e))))
     :cljs
     (async/go
       (try
         (fs/readdirSync path)
         (catch :default e
           (log/error e (str "Failed to list files at path: " path))
           (throw e))))))

(defn exists?
  [path]
  #?(:clj  (async/thread (->> path io/file .exists))
     :cljs (async/go (fs/existsSync path))))


(defn local-path
  "Gives absolute full local path if input path is not already absolute."
  [path]
  (let [abs-path? #?(:clj (.isAbsolute (io/file path))
                     :cljs (path/isAbsolute path))
        abs-root  (if abs-path?
                    ""
                    (str #?(:clj  (.getAbsolutePath (io/file ""))
                            :cljs (path/resolve ".")) "/"))
        path      (str abs-root path "/")]
    #?(:clj  (-> path io/file .getCanonicalPath)
       :cljs (path/resolve path))))
