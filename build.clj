(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'com.fluree/db)
(def version "2.0.0-beta2")

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
                :src-dirs  ["src"]
                :scm       {:url                 source-uri
                            :connection          "scm:git:https://github.com/fluree/db.git"
                            :developerConnection "scm:git:git@github.com:fluree/db.git"}})
  (b/copy-dir {:src-dirs    ["src" "resources"]
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
  ;; Seems like there should be a better way to do this, but I couldn't figure
  ;; one out. If you try to run codox directly from here it can't find any of
  ;; the project namespaces. Seems like codox would need a way to pass a basis
  ;; into it. But this at least lets us inject the version set in here.
  (let [opts (cond-> {:version version}
                     output-path (assoc :output-path output-path))]
    (b/process {:command-args ["clojure" "-X:docs" (pr-str opts)]})))
