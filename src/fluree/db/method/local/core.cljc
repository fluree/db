(ns fluree.db.method.local.core
  (:require [fluree.db.util.log :as log]
            #?(:clj [clojure.java.io :as io]))
  #?(:clj (:import (java.io FileNotFoundException))))

#?(:clj
   (defn local-write-fn-java
     ([]
      (let [home-fluree (str (System/getProperty "user.home") "/.fluree/")]
        (io/make-parents (io/file home-fluree))
        (local-write-fn-java home-fluree)))
     ([base-directory]
      (fn [filename data]
        (try
          (spit (io/file base-directory filename) data)
          (catch FileNotFoundException _
            (try
              (io/make-parents (io/file base-directory filename))
              (spit (io/file base-directory filename) data)

              (catch Exception e
                (log/error (str "Unable to create storage directory: " base-directory
                                " with error: " (.getMessage e) ". Permission issue?"))
                (log/error (str "Fatal Error, shutting down!"))
                (System/exit 1)))))))))

(defn local-write-fn
  []
  #?(:clj  (local-write-fn-java)
     :cljs (throw (ex-info "Local write not yet implemented" {}))))

(comment

  (local-write-fn)
  ((local-write-fn) "test/blah2.txt" "Hello")


  )