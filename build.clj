(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.fluree/db)
(def version "3.0.0-alpha2")

(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file "target/fluree-db.jar")

(def source-uri "https://github.com/fluree/db")

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (b/write-pom {:class-dir class-dir
                :lib       lib
                :version   version
                :basis     basis
                :src-dirs  ["src/clj"]
                :scm       {:url                 source-uri
                            :connection          "scm:git:https://github.com/fluree/db.git"
                            :developerConnection "scm:git:git@github.com:fluree/db.git"}
                :pom-data  [[:licenses
                             [:license
                              [:name "EPL-2.0"]
                              [:url "https://www.eclipse.org/legal/epl-2.0"]]]]})
  (b/copy-dir {:src-dirs    ["src/clj" "resources"]
               :target-dir  class-dir})
  (b/jar {:class-dir class-dir
          :jar-file  jar-file}))

(defn install [_]
  (b/install {:basis     basis
              :lib       lib
              :version   version
              :jar-file  jar-file
              :class-dir class-dir}))

(defn docs [{:keys [output-path]}]
  ;; This is (for now) the best way to run things like codox that expect the
  ;; full project classpath to be available.
  (let [opts (cond-> {:version version
                      :source-uri (str source-uri
                                       "/blob/v{version}/{filepath}#L{line}")}
               output-path (assoc :output-path output-path))]
    (b/process {:command-args ["clojure" "-X:docs" (pr-str opts)]})))

(defn deploy [_]
  (dd/deploy {:installer :remote
              :artifact  jar-file
              :pom-file  (b/pom-path {:lib lib, :class-dir class-dir})}))

(defn sync-package-json [{:keys [target node?]}]
  (let [node-arg (when node? "--node")
        cmd-args (remove nil? ["bb" "run" "sync-package-json" version
                               (str target) node-arg])]
    (println "cmd args:" (pr-str cmd-args))
    (b/process {:command-args cmd-args})))

(defn print-version [_]
  (println (pr-str {:version version})))
