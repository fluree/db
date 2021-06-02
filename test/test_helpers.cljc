(ns test-helpers
  (:require
    #?@(:clj  [[clojure.test :refer :all]
               [clojure.core.async :refer [chan <!! >!! go alts! timeout]]]
        :cljs [[cljs.test :refer-macros [deftest is testing async]]
               [cljs.core.async :refer [chan take! go alts! timeout]]])))

;https://stackoverflow.com/questions/30766215/how-do-i-unit-test-clojure-core-async-go-macros

(defn test-async
  "Asynchronous test awaiting ch to produce a value or close."
  [ch]
  #?(:clj
     (<!! ch)
     :cljs
     (async done
            (take! ch (fn [_] (done))))))

(defn test-within
  "Asserts that ch does not close or produce a value within ms. Returns a
  channel from which the value can be taken."
  [ms ch]
  (go (let [t (timeout ms)
            [v ch] (alts! [ch t])]
        (is (not= ch t)
            (str "Test should have finished within " ms "ms."))
        v)))

