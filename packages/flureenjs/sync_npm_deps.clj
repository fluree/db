(ns sync-npm-deps
  (:require [cheshire.core :as json]))

(let [project  (json/parse-string (slurp "../../package.json"))
      template (json/parse-string (slurp "package.json.template"))
      updated  (assoc template "dependencies" (get project "dependencies"))]
  (spit "package.json" (json/generate-string updated {:pretty true})))
