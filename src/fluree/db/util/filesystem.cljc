(ns fluree.db.util.filesystem
  (:refer-clojure :exclude [exists?])
  (:require #?@(:clj [[clojure.java.io :as io]
                      [clojure.core.cache :as cache]])
            #?@(:cljs [[cljs.cache :as cache]
                       ["fs" :as fs]
                       ["fs-ext" :refer [flockSync]]
                       ["path" :as path]])
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.crypto.aes :as aes]
            [fluree.db.util.log :as log])
  #?(:clj (:import (java.io ByteArrayOutputStream FileNotFoundException File)
                   (java.nio ByteBuffer)
                   (java.nio.channels FileChannel)
                   (java.nio.file Files OpenOption Paths StandardOpenOption)
                   (java.nio.file.attribute FileAttribute PosixFilePermissions))))

#?(:clj (set! *warn-on-reflection* true))

(def lock-cache-size
  4096)

(def local-lock-cache
  (-> {}
      (cache/lru-cache-factory :threshold lock-cache-size)
      atom))

(defn ensure-local-lock
  [m path]
  (if (cache/has? m path)
    (cache/hit m path)
    (cache/miss m path #?(:clj  (Object.)
                          :cljs (js-obj)))))

(defn get-local-lock
  [path]
  (-> local-lock-cache
      (swap! ensure-local-lock path)
      (cache/lookup path)))

#?(:clj
   (def empty-path-array
     (into-array String [])))

#?(:clj
   (def writable-open-options
     (into-array OpenOption [StandardOpenOption/CREATE StandardOpenOption/READ
                             StandardOpenOption/SYNC StandardOpenOption/WRITE])))

#?(:clj
   (def parent-attributes
     (into-array FileAttribute
                 (-> "rwxr-x---"
                     PosixFilePermissions/fromString
                     PosixFilePermissions/asFileAttribute
                     vector))))

#?(:clj
   (defn open-file-channel ^FileChannel
     [path-str]
     (let [path   (Paths/get path-str empty-path-array)
           parent (.getParent path)]
       (when (some? parent)
         (Files/createDirectories parent parent-attributes))
       (FileChannel/open path writable-open-options))))

#?(:clj
   (defn read-file-channel
     [^FileChannel file-ch]
     (let [size (.size file-ch)
           buf  (ByteBuffer/allocate (int size))]
       (doto file-ch
         (.position 0)
         (.read buf))
       (.flip buf)
       (let [bs (byte-array (.remaining buf))]
         (.get buf bs)
         bs))))

#?(:clj
   (defn write-file-channel
     [^FileChannel file-ch bs]
     (let [buf (ByteBuffer/wrap bs)]
       (doto file-ch
         (.truncate 0)
         (.position 0)
         (.write buf)))))

#?(:cljs
   (defn with-locked-fd
     "Replace the contents of the file descriptor `fd` by applying function `f`
     with an exclusive lock"
     [fd f]
     (flockSync fd "ex")
     (try
       (let [stats  (fs/fstatSync fd)
             size   (.-size stats)
             buffer (js/Buffer.alloc size)
             _      (fs/readSync fd buffer 0 size 0)
             result (f buffer)]
         (fs/ftruncateSync fd 0)
         (fs/writeSync fd result 0)
         result)
       (finally
         (flockSync fd "un")
         (fs/closeSync fd)))))

(defn with-file-lock
  [path f]
  #?(:clj
     (async/thread
       (locking (get-local-lock path)
         (with-open [file-ch (open-file-channel path)]
           (let [os-lock (.lock file-ch)]
             (try
               (let [result (-> file-ch read-file-channel f)]
                 (write-file-channel file-ch result)
                 result)
               (catch Exception e
                 e)
               (finally
                 (.release os-lock)))))))
     :cljs
     (async/go
       (locking (get-local-lock path)
         (try
           (let [fd (fs/openSync path "r+")]
             (with-locked-fd fd f))
           (catch :default e
             (if (= "ENOENT" (.-code e))
               (try
                 (fs/mkdirSync (path/dirname path) #js{:recursive true})
                 (let [fd (fs/openSync path "w+")]
                   (with-locked-fd fd f))
                 (catch :default create-e
                   create-e))
               e)))))))

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
               (log/error! ::write-file-error e {:msg "Unable to create storage directory"
                                                 :path path})
               (log/error e "Unable to create storage directory:" path ".")
               (log/error "Fatal Error, shutting down!")
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
                   (log/error! ::write-file-error e {:msg "Unable to write file to path"
                                                     :path path})
                   (log/error e "Unable to write file to path" path ".")
                   (log/error (str "Fatal Error, shutting down! "
                                   {"errno"   ^String (.-errno e)
                                    "syscall" ^String (.-syscall e)
                                    "code"    (.-code e)
                                    "path"    (.-path e)}))
                   (js/process.exit 1)))
               (catch :default e
                 (log/error! ::write-file-error e {:msg "Unable to create storage directory"
                                                   :path path})
                 (log/error e "Unable to create storage directory:" path ".")
                 (log/error "Fatal Error, shutting down!")
                 (js/process.exit 1)))
             (throw (ex-info "Error writing file."
                             {"errno"   ^String (.-errno e)
                              "syscall" ^String (.-syscall e)
                              "code"    (.-code e)
                              "path"    (.-path e)}))))))))

(defn read-file
  "Read bytes from disk at `path`. Returns nil if file does not exist.
   If encryption-key is provided, expects file to be encrypted and will decrypt it,
   returning the decrypted bytes."
  ([path] (read-file path nil))
  ([path encryption-key]
   #?(:clj
      (async/thread
        (try
          (with-open [xin  (io/input-stream path)
                      xout (ByteArrayOutputStream.)]
            (io/copy xin xout)
            (let [raw-bytes (.toByteArray xout)]
              (if encryption-key
                (try
                  (aes/decrypt raw-bytes encryption-key
                               {:output-format :none})
                  (catch Exception e
                    (ex-info (str "Failed to decrypt file: " path)
                             {:status 500
                              :error :db/storage-error
                              :path path}
                             e)))
                raw-bytes)))
          (catch FileNotFoundException _
            nil)
          (catch Exception e
            e)))
      :cljs
      (async/go
        (try
          (if encryption-key
            ;; For encrypted files, read as buffer and decrypt to bytes
            (let [buffer (fs/readFileSync path)]
              (try
                (aes/decrypt buffer encryption-key
                             {:output-format :none})
                (catch :default e
                  (ex-info (str "Failed to decrypt file: " path)
                           {:status 500
                            :error :db/storage-error
                            :path path}
                           e))))
            ;; For non-encrypted files, read as buffer (bytes)
            (fs/readFileSync path))
          (catch :default e
            (if (= "ENOENT" (.-code e))
              nil
              (ex-info "Error reading file."
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
           (log/error! ::list-files-error e {:msg "Failed to list files at path"
                                             :path path})
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

(defn encode-path-segment
  "URL-encode a string to be safe for use in file paths and S3 keys.
   Preserves alphanumeric characters, hyphens, underscores, and periods.
   Other characters are percent-encoded as %XX where XX is the hex code."
  [s]
  (str/replace s
               #"[^a-zA-Z0-9\-_.]"
               #(let [code (int (first %))
                      hex  #?(:clj (format "%02X" code)
                              :cljs (let [h (.toString code 16)]
                                      (if (= 1 (count h))
                                        (str "0" (.toUpperCase h))
                                        (.toUpperCase h))))]
                  (str "%" hex))))
