(ns fluree.db.util.graalvm
  "Utilities for detecting and handling GraalVM native-image runtime."
  (:require [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn build?
  "Returns true if building for GraalVM.
   Checks for environment variable or system property set during build.
   This is primarily used at compile-time for conditional compilation."
  []
  #?(:clj (or (System/getenv "FLUREE_GRAALVM_BUILD")
              (System/getProperty "fluree.graalvm.build"))
     :cljs false))

(defn runtime?
  "Detects if running in GraalVM native-image at runtime.
   Uses ImageInfo API when available, returns false otherwise (regular JVM).
   This uses reflection to avoid ClassNotFoundException on regular JVM."
  []
  #?(:clj
     (try
       ;; Try to use GraalVM's ImageInfo API to detect native-image runtime
       (let [image-info-class (Class/forName "org.graalvm.nativeimage.ImageInfo")
             method           (.getMethod image-info-class "inImageRuntimeCode" (into-array Class []))]
         (boolean (.invoke method nil (object-array 0))))
       (catch ClassNotFoundException _
         ;; ImageInfo class not available - running on regular JVM
         false)
       (catch Exception e
         ;; Any other error, assume regular JVM
         (log/warn e "Error detecting GraalVM runtime, assuming regular JVM")
         false))
     :cljs false))

#?(:clj
   (defmacro if-graalvm
     "Compile-time conditional for GraalVM-specific code.
      Uses graalvm-branch when building for GraalVM, else-branch otherwise."
     [graalvm-branch else-branch]
     (if (build?)
       graalvm-branch
       else-branch)))
