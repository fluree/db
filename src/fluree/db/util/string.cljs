(ns fluree.db.util.string
  (:require [goog.string :as gstring]
            [goog.string.format]))


;; ClojureScript used to have a cljs.core/format that wrapped
;; goog.string.format but they took it out b/c it was too different from the
;; Java stuff that clojure.core/format wraps. But our usage is so basic that
;; this should suffice 99+% of the time. - WSM 2021-08-30
(defn format [format-str & vals]
  (apply gstring/format format-str vals))
