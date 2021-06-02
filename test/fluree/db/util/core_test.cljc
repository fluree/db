(ns fluree.db.util.core-test
  (:require
    #?@(:clj  [[clojure.test :refer :all]]
        :cljs [[cljs.test :refer-macros [deftest is testing]]
               [goog.date]])
    [fluree.db.util.core :as c])
  #?(:clj  (:import
             (clojure.lang ExceptionInfo)
             (java.time LocalDateTime LocalDate Instant)
             (java.time.format DateTimeParseException)
             (java.util Date))
     :cljs (:import
             goog.date.Date
             goog.date.DateTime)))

;; Date-related values
(def now-instant #?(:clj  (Instant/now)
                    :cljs (js/Date.now))) ;(.toUTCString (js/Date.)) (.valueOf (js/Date.)
(def now-ms      #?(:clj  (.toEpochMilli now-instant)
                    :cljs (.valueOf now-instant)))
(def now-date    #?(:clj  (Date/from now-instant)
                    :cljs now-instant))

(def invalid-dt #?(:clj  (LocalDateTime/now)
                   :cljs {:key-1 "value-1"}))
(def invalid-dt-str #?(:clj  (.toString (LocalDateTime/now))
                       :cljs "2021-05-26X19:53:33.091114Z"))
(def valid-dt-str "2021-05-26T19:53:33.091114Z")


(deftest db-util-core-test
  (testing "index-of"
    (let [x ["clojure" 536 "Fluree" {:key-1 "value-1"}]]
      (is (= 2 (c/index-of x "Fluree")))
      (is (nil? (c/index-of x "clojurescript")))))

  (testing "random-uuid"
    (is (some? (c/random-uuid))))

  (testing "date->millis"
    (is (= now-ms (c/date->millis now-ms)))
    #?(:clj (is (= now-ms (-> now-instant str c/date->millis))))
    (is (= now-ms (c/date->millis now-instant)))
    (is (= now-ms (c/date->millis now-date)))

    ;; 2021-05-26T11:54:32.149533 <- invalid date format?
    (is (thrown-with-msg?
          #?(:clj ExceptionInfo :cljs js/Error)
          #"Invalid date"
          (c/date->millis invalid-dt)))
    (is (thrown-with-msg?
          #?(:clj  DateTimeParseException
             :cljs js/Error)
          #?(:clj  #"could not be parsed"
             :cljs #".getTime is not a function")
          (c/date->millis invalid-dt-str))))

  (testing "email?"
    (is (some? (c/email? "joe@flur.ee")))
    (is (nil? (c/email? "joeschmoe"))))

  (testing "pred-ident?"
    (is (c/pred-ident? ["_auth/id" "TGIF"]))
    (is (not (c/pred-ident? [12344567 "TGIF"])))
    (is (not (c/pred-ident? 123456787)))
    (is (not (c/pred-ident? [12344567 "TGIF" 1234 false]))))

  (testing "temp-ident?"
    (is (c/temp-ident? "any-string"))
    (is (not (c/temp-ident? 12345)))
    (is (not (c/temp-ident? now-instant))))

  (testing "subj-ident?"
    (is (c/subj-ident? 12345))
    (is (c/subj-ident? ["_auth/id" "TGIF"]))
    (is (not (c/subj-ident? "any-string")))
    (is (not (c/subj-ident? [12344567 "TGIF" 1234 false]))))

  (testing "str->int"
    (is (= 2147483647 (c/str->int "2147483647")))
    #?(
       :clj (is (thrown? NumberFormatException (c/str->int "ABC")))
       :cljs (is (js/isNaN (c/str->int "ABC")))))

  (testing "str->keyword"
    (is (= :sesame
           (c/str->keyword :sesame)
           (c/str->keyword ":sesame")
           (c/str->keyword "sesame")))
    (is (thrown-with-msg?
          #?(:clj ExceptionInfo :cljs js/Error)
          #"Cannot convert type"
          (c/str->keyword now-ms))))

  (testing "keyword->str"
    (is (= "sesame"
           (c/keyword->str :sesame)
           (c/keyword->str "sesame")))
    (is (thrown-with-msg?
          #?(:clj ExceptionInfo :cljs js/Error)
          #"Cannot convert type"
          (c/keyword->str now-ms))))

  (testing "str->epoch-ms"
    (is (= 1622058813091 (c/str->epoch-ms valid-dt-str)))
    #?(:clj  (is (thrown-with-msg?
                   ExceptionInfo
                   #"Invalid time string"
                   (c/str->epoch-ms invalid-dt-str)))
       :cljs (is (js/isNaN (c/str->epoch-ms invalid-dt-str)))))

  (testing "trunc"
    (is (= "Clojure" (c/trunc "Clojure" 10)))
    (is (= " ..." (c/trunc "Clojure" 0)))
    (is (= "Clojure ..." (c/trunc "ClojureScript" 7))))

  (testing "without-nils"
    (is (nil? (-> {:auth nil :person "scott"} c/without-nils :auth)))
    (is (some? (-> {:auth nil :person "scott"} c/without-nils :person))))

  (testing "inclusive-range"
    (let [res (c/inclusive-range 10)]
      (is (= 11 (count res)))
      (is (= 0  (first res)))
      (is (= 10 (last res))))
    (let [res (c/inclusive-range 5 10)]
      (is (= 6 (count res)))
      (is (= 5 (first res)))
      (is (= 10 (last res))))
    (let [res (c/inclusive-range 5 10 2)]
      (is (= 4 (count res)))
      (is (= 5 (first res)))
      (is (= 11 (last res)))))

  (testing "exception?"
    (is (not (c/exception? {:key-1 "key" :value-1 "value"})))
    (let [ex (ex-info "To access the server, either open-api must be true or a valid auth must be available."
                      {:status 401
                       :error  :db/invalid-request})]
      (is (c/exception? ex))))

  (testing "url-encode-decode"
    (let [url "http://demoledger.fluree.com:8080/fdb/health"]
      (is (= url
             (-> url c/url-encode c/url-decode)))))

  (testing "map-invert"
    (testing "transformation"
      (let [m  {:key-1 "key1" :value-1 "value1"}
            m' (c/map-invert m)]
        (is (= (keys m) (vals m')))
        (is (= (vals m) (keys m')))))
    (testing "value with nil"
      (let [m  {:key-1 "key1" :value-1 nil}
            m' (c/map-invert m)]
        (is (= (keys m) (vals m')))
        (is (= (vals m) (keys m'))))))

  (testing "zero-pad"
    (is (= "123456789" (c/zero-pad "123456789" 2)))
    (is (= "00001" (c/zero-pad 1 5))))

  )
