(ns fluree.db.util.graalvm
  "GraalVM compatibility utilities to replace eval-based macros")

;; For GraalVM compatibility, we need to avoid using eval at runtime.
;; This namespace provides alternatives to eval-based utilities.

(defn graalvm-build?
  "Returns true if building for GraalVM. 
   Checks for environment variable or system property set during build."
  []
  #?(:clj (or (System/getenv "FLUREE_GRAALVM_BUILD")
              (System/getProperty "fluree.graalvm.build"))
     :cljs false))

(defmacro if-graalvm
  "Compile-time conditional for GraalVM-specific code.
   Uses graalvm-branch when building for GraalVM, else-branch otherwise."
  [graalvm-branch else-branch]
  (if (graalvm-build?)
    graalvm-branch
    else-branch))