(ns fluree.db.util.filesystem
  (:refer-clojure :exclude [exists?])
  (:require [fluree.db.util.log :as log]
            #?(:clj [clojure.java.io :as io])
            #?@(:cljs [["fs" :as fs]
                       ["path" :as path]]))
  #?(:clj (:import (java.io ByteArrayOutputStream FileNotFoundException))))

#?(:clj (set! *warn-on-reflection* true))

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

(defn read-file
  "Read a string from disk at `path`. Returns nil if file does not exist."
  [path]
  #?(:clj
     (try
       (with-open [xin  (io/input-stream path)
                   xout (ByteArrayOutputStream.)]
         (io/copy xin xout)
         (String. (.toByteArray xout)))

       (catch FileNotFoundException _
         nil))
     :cljs
     (try
       (fs/readFileSync path "utf8")
       (catch :default e
         (when (not= "ENOENT" (.-code e))
           (throw (ex-info "Error reading file."
                           {"errno"   ^String (.-errno e)
                            "syscall" ^String (.-syscall e)
                            "code"    (.-code e)
                            "path"    (.-path e)})))))))

(defn exists?
  [path]
  #?(:clj  (->> path io/file .exists)
     :cljs (fs/existsSync full-path)))


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