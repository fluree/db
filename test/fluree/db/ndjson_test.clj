(ns fluree.db.ndjson-test
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.ndjson :as ndjson])
  (:import (java.io StringReader)))

(defn str->reader
  "Creates a StringReader from a string for testing."
  [s]
  (StringReader. s))

;;; ---------------------------------------------------------------------------
;;; Line reading tests
;;; ---------------------------------------------------------------------------

(deftest reader->line-ch-test
  (testing "Reading lines from a reader"
    (let [input "line1\nline2\nline3"
          line-ch (ndjson/reader->line-ch (str->reader input))]
      (is (= {:line-num 1 :content "line1"} (<!! line-ch)))
      (is (= {:line-num 2 :content "line2"} (<!! line-ch)))
      (is (= {:line-num 3 :content "line3"} (<!! line-ch)))
      (is (nil? (<!! line-ch)) "Channel should close after last line")))

  (testing "Skips blank lines"
    (let [input "line1\n\n  \nline2"
          line-ch (ndjson/reader->line-ch (str->reader input))]
      (is (= {:line-num 1 :content "line1"} (<!! line-ch)))
      (is (= {:line-num 4 :content "line2"} (<!! line-ch)))
      (is (nil? (<!! line-ch)))))

  (testing "Trims whitespace"
    (let [input "  line1  \n\tline2\t"
          line-ch (ndjson/reader->line-ch (str->reader input))]
      (is (= {:line-num 1 :content "line1"} (<!! line-ch)))
      (is (= {:line-num 2 :content "line2"} (<!! line-ch)))))

  (testing "Handles empty input"
    (let [input ""
          line-ch (ndjson/reader->line-ch (str->reader input))]
      (is (nil? (<!! line-ch)) "Channel should close on empty input"))))

;;; ---------------------------------------------------------------------------
;;; JSON parsing tests
;;; ---------------------------------------------------------------------------

(deftest parse-line-test
  (testing "Parses valid JSON"
    (let [result (ndjson/parse-line "{\"@id\": \"ex:1\", \"name\": \"Alice\"}" 1)]
      (is (= {"@id" "ex:1" "name" "Alice"} result))))

  (testing "Throws on invalid JSON"
    (is (thrown-with-msg? Exception #"Invalid JSON"
                          (ndjson/parse-line "not json" 1))))

  (testing "Preserves @context keys"
    (let [result (ndjson/parse-line "{\"@context\": {\"ex\": \"http://example.org/\"}}" 1)]
      (is (= {"@context" {"ex" "http://example.org/"}} result)))))

(deftest context-only-line-test
  (testing "Identifies context-only lines"
    (is (true? (ndjson/context-only-line? {"@context" {"ex" "http://example.org/"}})))
    (is (false? (ndjson/context-only-line? {"@context" {"ex" "http://example.org/"}
                                            "@id" "ex:1"})))
    (is (false? (ndjson/context-only-line? {"@id" "ex:1" "name" "Alice"})))))

;;; ---------------------------------------------------------------------------
;;; Context management tests
;;; ---------------------------------------------------------------------------

(deftest merge-contexts-test
  (testing "Returns shared context when doc has none"
    (is (= {"ex" "http://example.org/"}
           (ndjson/merge-contexts {"ex" "http://example.org/"} nil))))

  (testing "Returns doc context when shared is nil"
    (is (= {"ex" "http://example.org/"}
           (ndjson/merge-contexts nil {"ex" "http://example.org/"}))))

  (testing "Merges map contexts"
    (let [result (ndjson/merge-contexts {"ex" "http://example.org/"}
                                         {"schema" "http://schema.org/"})]
      (is (= [{"ex" "http://example.org/"} {"schema" "http://schema.org/"}] result))))

  (testing "Handles sequential doc context"
    (let [result (ndjson/merge-contexts {"ex" "http://example.org/"}
                                         [{"schema" "http://schema.org/"}])]
      (is (= [{"ex" "http://example.org/"} {"schema" "http://schema.org/"}] result)))))

(deftest context-for-document-test
  (testing "Uses opts context when no other context"
    (is (= {"ex" "http://example.org/"}
           (ndjson/context-for-document {"ex" "http://example.org/"} nil {}))))

  (testing "Merges all contexts"
    (let [result (ndjson/context-for-document
                  {"base" "http://base.org/"}
                  {"shared" "http://shared.org/"}
                  {"@context" {"doc" "http://doc.org/"}})]
      (is (vector? result))
      ;; opts merged with shared gives [opts shared], then merged with doc gives [[opts shared] doc]
      (is (= 2 (count result))))))

(deftest prepare-document-test
  (testing "Adds context to document without context"
    (let [result (ndjson/prepare-document {"ex" "http://example.org/"} nil
                                          {"@id" "ex:1" "name" "Alice"})]
      (is (contains? result "@context"))
      (is (= {"ex" "http://example.org/"} (get result "@context")))
      (is (= "ex:1" (get result "@id")))))

  (testing "Preserves document without any context when none provided"
    (let [result (ndjson/prepare-document nil nil {"@id" "ex:1"})]
      (is (= {"@id" "ex:1"} result)))))

;;; ---------------------------------------------------------------------------
;;; Document channel tests
;;; ---------------------------------------------------------------------------

(deftest line-ch->doc-ch-test
  (testing "Parses NDJSON lines into documents"
    (let [input "{\"@id\": \"ex:1\", \"name\": \"Alice\"}\n{\"@id\": \"ex:2\", \"name\": \"Bob\"}"
          line-ch (ndjson/reader->line-ch (str->reader input))
          doc-ch (ndjson/line-ch->doc-ch line-ch)]
      (let [doc1 (<!! doc-ch)]
        (is (= 1 (:line-num doc1)))
        (is (= "ex:1" (get-in doc1 [:doc "@id"]))))
      (let [doc2 (<!! doc-ch)]
        (is (= 2 (:line-num doc2)))
        (is (= "ex:2" (get-in doc2 [:doc "@id"]))))
      (is (nil? (<!! doc-ch)))))

  (testing "Extracts shared context from first line"
    (let [input "{\"@context\": {\"ex\": \"http://example.org/\"}}\n{\"@id\": \"ex:1\", \"name\": \"Alice\"}"
          line-ch (ndjson/reader->line-ch (str->reader input))
          doc-ch (ndjson/line-ch->doc-ch line-ch)]
      (let [doc1 (<!! doc-ch)]
        (is (= 2 (:line-num doc1)))
        (is (= {"ex" "http://example.org/"} (get-in doc1 [:doc "@context"])))
        (is (= "ex:1" (get-in doc1 [:doc "@id"]))))
      (is (nil? (<!! doc-ch)))))

  (testing "Applies opts context to all documents"
    (let [input "{\"@id\": \"ex:1\"}\n{\"@id\": \"ex:2\"}"
          line-ch (ndjson/reader->line-ch (str->reader input))
          doc-ch (ndjson/line-ch->doc-ch line-ch {:context {"ex" "http://example.org/"}})]
      (let [doc1 (<!! doc-ch)]
        (is (= {"ex" "http://example.org/"} (get-in doc1 [:doc "@context"]))))
      (let [doc2 (<!! doc-ch)]
        (is (= {"ex" "http://example.org/"} (get-in doc2 [:doc "@context"])))))))

;;; ---------------------------------------------------------------------------
;;; Batch accumulator tests
;;; ---------------------------------------------------------------------------

(deftest create-batch-accumulator-test
  (testing "Accumulates documents"
    (let [acc (ndjson/create-batch-accumulator {:max-batch-lines 3})]
      (is (= {:flush? false :count 1} ((:add-doc acc) {"@id" "ex:1"} 1)))
      (is (= {:flush? false :count 2} ((:add-doc acc) {"@id" "ex:2"} 2)))
      (is (= {:flush? true :count 3} ((:add-doc acc) {"@id" "ex:3"} 3)))))

  (testing "Flush returns accumulated docs"
    (let [acc (ndjson/create-batch-accumulator {})]
      ((:add-doc acc) {"@id" "ex:1"} 1)
      ((:add-doc acc) {"@id" "ex:2"} 2)
      (let [batch ((:flush acc))]
        (is (= 2 (:count batch)))
        (is (= [{"@id" "ex:1"} {"@id" "ex:2"}] (:docs batch)))
        (is (= [1 2] (:line-nums batch))))))

  (testing "Flush resets accumulator"
    (let [acc (ndjson/create-batch-accumulator {})]
      ((:add-doc acc) {"@id" "ex:1"} 1)
      ((:flush acc))
      (is (= {:flush? false :count 1} ((:add-doc acc) {"@id" "ex:2"} 2)))
      (let [batch ((:flush acc))]
        (is (= 1 (:count batch)))
        (is (= [{"@id" "ex:2"}] (:docs batch)))))))

;;; ---------------------------------------------------------------------------
;;; Integration: reader->doc-ch
;;; ---------------------------------------------------------------------------

(deftest reader->doc-ch-test
  (testing "Creates document channel directly from reader"
    (let [input "{\"@id\": \"ex:1\", \"name\": \"Alice\"}\n{\"@id\": \"ex:2\", \"name\": \"Bob\"}"
          doc-ch (ndjson/reader->doc-ch (str->reader input))]
      (is (= "ex:1" (get-in (<!! doc-ch) [:doc "@id"])))
      (is (= "ex:2" (get-in (<!! doc-ch) [:doc "@id"])))
      (is (nil? (<!! doc-ch)))))

  (testing "Passes context option through"
    (let [input "{\"@id\": \"ex:1\"}"
          doc-ch (ndjson/reader->doc-ch (str->reader input)
                                        {:context {"ex" "http://example.org/"}})
          doc (<!! doc-ch)]
      (is (= {"ex" "http://example.org/"} (get-in doc [:doc "@context"]))))))
