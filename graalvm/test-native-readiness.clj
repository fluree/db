(ns test-native-readiness
  "Script to test if code is ready for GraalVM native compilation"
  (:require [clojure.string :as str]))

(defn check-for-eval []
  (println "\nChecking for eval usage...")
  (let [files (file-seq (clojure.java.io/file "src"))
        clj-files (filter #(re-matches #".*\.clj[cs]?$" (.getName %)) files)
        eval-uses (atom [])]
    (doseq [file clj-files]
      (let [content (slurp file)
            lines (str/split-lines content)]
        (doseq [[idx line] (map-indexed vector lines)]
          (when (and (str/includes? line "eval")
                     (not (str/includes? line "eval.cljc"))
                     (not (str/includes? line ";"))
                     (not (str/includes? line "eval-dispatch")))
            (swap! eval-uses conj {:file (.getPath file)
                                  :line (inc idx)
                                  :content (str/trim line)})))))
    (if (empty? @eval-uses)
      (println "✓ No eval usage found!")
      (do
        (println "✗ Found eval usage:")
        (doseq [{:keys [file line content]} @eval-uses]
          (println (str "  " file ":" line " - " content)))))))

(defn check-for-reflection []
  (println "\nChecking for reflection...")
  (let [files (file-seq (clojure.java.io/file "src"))
        clj-files (filter #(re-matches #".*\.clj[cs]?$" (.getName %)) files)
        reflection-uses (atom [])]
    (doseq [file clj-files]
      (let [content (slurp file)
            lines (str/split-lines content)]
        (doseq [[idx line] (map-indexed vector lines)]
          (when (or (str/includes? line "Class/forName")
                    (str/includes? line ".newInstance")
                    (str/includes? line ".getDeclaredMethod")
                    (str/includes? line ".getDeclaredField"))
            (swap! reflection-uses conj {:file (.getPath file)
                                        :line (inc idx)
                                        :content (str/trim line)})))))
    (if (empty? @reflection-uses)
      (println "✓ No problematic reflection found!")
      (do
        (println "✗ Found reflection that may need configuration:")
        (doseq [{:keys [file line content]} @reflection-uses]
          (println (str "  " file ":" line " - " content)))))))

(defn check-dynamic-requires []
  (println "\nChecking for dynamic requires...")
  (let [files (file-seq (clojure.java.io/file "src"))
        clj-files (filter #(re-matches #".*\.clj[cs]?$" (.getName %)) files)
        dynamic-requires (atom [])]
    (doseq [file clj-files]
      (let [content (slurp file)
            lines (str/split-lines content)]
        (doseq [[idx line] (map-indexed vector lines)]
          (when (and (str/includes? line "require")
                     (or (str/includes? line "resolve")
                         (str/includes? line "symbol")))
            (swap! dynamic-requires conj {:file (.getPath file)
                                         :line (inc idx)
                                         :content (str/trim line)})))))
    (if (empty? @dynamic-requires)
      (println "✓ No dynamic requires found!")
      (do
        (println "✗ Found potential dynamic requires:")
        (doseq [{:keys [file line content]} @dynamic-requires]
          (println (str "  " file ":" line " - " content)))))))

(defn -main []
  (println "=== GraalVM Native Image Readiness Check ===")
  (check-for-eval)
  (check-for-reflection)
  (check-dynamic-requires)
  (println "\n=== Summary ===")
  (println "Run this before attempting native image compilation.")
  (println "Address any issues found above.")
  (System/exit 0))

(-main)