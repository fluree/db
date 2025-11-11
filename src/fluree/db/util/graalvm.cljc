(ns fluree.db.util.graalvm
  "Utilities for detecting and handling GraalVM native-image runtime."
  #?(:clj (:require [fluree.db.util.log :as log])))

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
   Uses system property check first, with reflection fallback.
   Returns false for regular JVM."
  []
  #?(:clj
     (let [prop (System/getProperty "org.graalvm.nativeimage.imagecode")]
       (if (some? prop)
         (let [is-runtime (= "runtime" (.toLowerCase prop))]
           (when is-runtime
             (log/info "Detected GraalVM native-image runtime via system property"))
           is-runtime)
         (try
           ;; Fallback: try to use GraalVM's ImageInfo constants
           (let [cls          (Class/forName "org.graalvm.nativeimage.ImageInfo")
                 key-field    (.getField cls "PROPERTY_IMAGE_CODE_KEY")
                 val-field    (.getField cls "PROPERTY_IMAGE_CODE_VALUE_RUNTIME")
                 key          (.get key-field nil)
                 val-runtime  (.get val-field nil)
                 prop2        (System/getProperty (str key))
                 is-runtime   (= (str val-runtime) prop2)]
             (when is-runtime
               (log/info "Detected GraalVM native-image runtime via ImageInfo constants"))
             is-runtime)
           (catch ClassNotFoundException _
             ;; ImageInfo class not available - running on regular JVM
             false)
           (catch NoSuchFieldException _
             ;; Fields not available in this GraalVM version - assume regular JVM
             false)
           (catch Throwable e
             ;; Any unexpected error, log and assume regular JVM
             (log/warn e "Unexpected error detecting GraalVM runtime, assuming regular JVM")
             false))))
     :cljs false))

#?(:clj
   (defmacro if-graalvm
     "Compile-time conditional for GraalVM-specific code.
      Uses graalvm-branch when building for GraalVM, else-branch otherwise."
     [graalvm-branch else-branch]
     (if (build?)
       graalvm-branch
       else-branch)))
