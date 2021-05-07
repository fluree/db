(ns fluree.db.dbfunctions.js
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [fluree.db.util.log :as log]
            [fluree.db.util.async :refer [<? go-try merge-into? channel?]]
            #?(:clj  [instaparse.core :as insta :refer [defparser]]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])
            [clojure.edn :as edn]))

(def ^:const flureejs (insta/parser (io/resource "flureejs.bnf")))

(def ^:const operators {:lt         `<
                        :gt         `>
                        :gte        `>=
                        :lte        `<=
                        :equals     `=
                        :not-equals `not=})

(def ^:const method-map {"toUpperCase" `str/upper-case})


(defmulti statement-parse (fn [statement-type & _] statement-type))

(defmethod statement-parse :literal
  [_ [literal-type literal]]
  (case literal-type
    :string (edn/read-string literal)
    :string-single-quot (-> (subs literal 1 (dec (count literal)))
                            (str/replace "\\'" "'"))
    :integer #?(:clj (Long/parseLong literal) :cljs (js/parseInt literal))
    :decimal #?(:clj (Double/parseDouble literal) :cljs (js/parseFloat literal))
    :boolean (if (= "true" literal) true false)
    :null nil))

(defmethod statement-parse :symbol
  ;; [:symbol "str2"]
  [_ symbol-name]
  (symbol symbol-name))

(defmethod statement-parse :method
  ;; [:method [:symbol "toUpperCase"] [:arguments]]
  [_ method-name args]
  (let [method    (or (get method-map (second method-name))
                      (throw (ex-info (str "Unknown method: " method-name)
                                      {:status 400 :error :db/invalid-method})))
        arguments (mapv #(apply statement-parse %) (rest args))]
    [method arguments]))

(defmethod statement-parse :arguments
  ;; [:arguments [:argument [:symbol "str2"]] [:argument [:symbol "str3"]] [:argument [:symbol "str4"]]]
  [_ & arguments]
  (mapv #(apply statement-parse %) arguments))

(defmethod statement-parse :argument
  ;; [:argument [:symbol "str2"]]
  [_ argument]
  (apply statement-parse argument))

(defmethod statement-parse :object-get
  ;; [:object-get [:symbol "$ctx"] [:symbol "o"]]
  ;; [:object-get [:literal [:string "\"there\""]] [:method [:symbol "toUpperCase"] [:arguments]]]
  [_ & get-ops]
  (let [[base-op & rest-ops] get-ops
        base-form (apply statement-parse base-op)]
    (reduce (fn [form get-op]
              (let [op-type (first get-op)]
                (case op-type
                  :literal (throw (ex-info (str "Literal value cannot be in a nested dot-notation: " (second get-op) ".")
                                           {:status 400 :error :db/invalid-function})) ;; [:literal [:string "\"there\""]])
                  :symbol `(clojure.core/get ~form ~(second get-op)) ;; [:symbol "o"]
                  :method (let [[method args] (apply statement-parse get-op)] ;; [:method [:symbol "toUpperCase"] [:arguments]]
                            `(~method ~form ~@args)))))
            base-form rest-ops)))

(defmethod statement-parse :var-define
  ;[:var-define [:symbol "objectValue"] [:object-get [:symbol "$ctx"] [:symbol "o"]]]
  [_ symbol set-to]
  (when-not (= :symbol (first symbol))
    (throw (ex-info (str "A variable can only be defined for a symbol, provided: " symbol ".")
                    {:status 400 :error :db/invalid-function})))
  (let [symbol* (apply statement-parse symbol)
        set-to* (apply statement-parse set-to)]
    [symbol* set-to*]))

(defmethod statement-parse :return
  ;; [:return [:symbol "hi"]]
  [_ return-arg]
  (apply statement-parse return-arg))

(defmethod statement-parse :arrow-fn
  ;[:arrow-fn
  ;  [:arguments [:argument [:symbol "myarg"]]]
  ;  [:statement
  ;   [:var-set
  ;    [:symbol "myvar"]
  ;    [:method-call [:literal [:string-single-quot "'there'"]] [:method "toUpperCase"] [:arguments]]]
  ;   [:return [:symbol "myvar"]]]]
  [_ & args]
  (let [[async? args block] (if (= "async" (first args))
                              (cons true (rest args))       ;; first arg is 'async', ignore with 'rest'
                              (cons false args))            ;; no 'async', so add 'true' in front of args
        args*        (apply statement-parse args)
        statements*  (apply statement-parse block)
        statements** (if async?
                       ;; TODO - try/catch as below, and likely async/go will only work in Java, not JavaScript runtime
                       ;`(clojure.core.async/go (try ~statements* (catch Exception e e))) ;; don't want throwable inside async, return raw exception
                       `(go-try ~statements*)
                       statements*)]
    `(fn ~args* ~statements**)))


(defmethod statement-parse :expression
  ;[:expression [:symbol "objectValue"] [:operator [:lt]] [:literal [:integer "0"]]]
  [_ lhs operator rhs]
  (let [lhs*          (apply statement-parse lhs)
        operator-type (first (second operator))
        operator-fn   (or (get operators operator-type)
                          (throw (ex-info (str "Invalid operator used: " operator-type ".")
                                          {:status 400 :error :db/invalid-function})))
        rhs*          (apply statement-parse rhs)]
    (list operator-fn lhs* rhs*)))

(defmethod statement-parse :if-else
  ; [:if-else
  ;  [:if
  ;   [:expression [:symbol "objectValue"] [:operator [:lt]] [:literal [:integer "0"]]]
  ;   [:block [:throw [:literal [:string-single-quot "'Object value is not negative!'"]]]]]
  ;  [:else [:block [:return [:literal [:boolean "true"]]]]]]
  [_ & if-elses]
  ;; TODO - should ensure only one :else block else throw -- parser can return multiple
  (let [cond-expr (reduce (fn [acc if-else]
                            (let [if-else-type (first if-else)
                                  ;; and :else statement doesn't have a condition, but :if and :else-if do
                                  condition    (when-not (= :else if-else-type)
                                                 (apply statement-parse (second if-else)))
                                  block        (if (= :else if-else-type)
                                                 (apply statement-parse (second if-else))
                                                 (apply statement-parse (nth if-else 2)))]
                              (if (= :else if-else-type)
                                (into acc [:else block])
                                (into acc [condition block]))))
                          [] if-elses)]
    `(cond ~@cond-expr)))

(defmethod statement-parse :throw
  ;[:throw [:literal [:string-single-quot "'Object value is not negative!'"]]]]
  [_ ex-message]
  ;; TODO should probably check ex-message is a literal, else invalid fn
  (let [message (str (apply statement-parse ex-message))]
    `(throw (ex-info ~message {:status 400 :error :db/smart-fn}))))

(defmethod statement-parse :block
  ;[:block
  ; [:var-define [:symbol "objectValue"] [:object-get [:symbol "$ctx"] [:symbol "o"]]]
  ; [:if-else
  ;  [:if
  ;   [:expression [:symbol "objectValue"] [:operator [:lt]] [:literal [:integer "0"]]]
  ;   [:block [:throw [:literal [:string-single-quot "'Object value is not negative!'"]]]]]
  ;  [:else [:block [:return [:literal [:boolean "true"]]]]]]]
  [_ & statements]
  (let [{var-defines true others false} (group-by #(= :var-define (first %)) statements)
        ;; hoist all var-defines into let form
        let-items  (mapcat #(apply statement-parse %) var-defines)
        statements (map #(apply statement-parse %) others)]
    `(let [~@let-items] ~@statements)))

(defmethod statement-parse :exports
  ; [:exports
  ;  [:symbol "nonNegative"]
  ;  [:arrow-fn
  ;   [:arguments [:argument [:symbol "$ctx"]]]
  ;   [:block
  ;    [:var-define [:symbol "objectValue"] [:object-get [:symbol "$ctx"] [:symbol "o"]]] ...
  [_ symbol-tuple function]
  (let [function-name (second symbol-tuple)]
    {:name function-name
     :fn   (apply statement-parse function)}))


(defn parse
  [smart-fn-str]
  (let [parsed (insta/parse flureejs smart-fn-str)]
    (log/info parsed)
    (apply statement-parse parsed)))


(comment

  (let [ctx {"o" 5}
        fn  (->> (slurp (io/resource "smartFunctions/nonNegative-sf.js"))
                 parse :fn eval)]
    (clojure.core.async/<!! (fn ctx)))


  (parse "var hi = 'hi string'")

  (parse "var myvar = 'there'.toUpperCase(); \n return myvar;")

  (insta/parse flureejs "exports.myFn = (myarg) => { var myvar = 'there'.toUpperCase(); \n return myvar; };")

  (insta/parse flureejs "exports.myFn = (myarg) => { blah.ctx.res(false); };")

  flureejs

  (insta/parse flureejs "var hi = 'there'.toUpperCase(); return hi")

  (->> (slurp (io/resource "smartFunctions/nonNegative-sf.js"))
       (insta/parse flureejs))

  (->> (slurp (io/resource "smartFunctions/sample-sf.js"))
       (insta/parse flureejs))

  ((-> '()
       (conj '(let [hi "blah"] hi))
       (conj [])
       (conj 'clojure.core/fn)
       (eval)))

  )

;; var str = "Hello World!";
;; var res = str.toUpperCase();

; [{
;    "_id": "_fn",
;    "name": "nonNegative?",
;    "doc": "Checks that a value is non-negative",
;    "code": "(<= 0 (?o))"
;}]
