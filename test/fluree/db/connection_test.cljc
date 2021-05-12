(ns fluree.db.connection-test
  (:require #?@(:clj  [[clojure.test :refer :all]]
                :cljs [[cljs.test :refer-macros [deftest is testing]]])
            [fluree.db.connection :refer [server-regex]]))

(deftest parse-uris-test
  (testing "parse"
    (testing "server-regex"
      (testing "check for errors"
        (testing "missing protocol"
          (let [uri "//localhost:8090"]
            (is (nil? (re-matches server-regex uri))
                "no matches when protocol is missing")))
        (testing "missing host"
          (let [uri "https://:8090"]
            (is (nil? (re-matches server-regex uri))
                "no matches when hostname is missing")))
        (testing "path provided"
          (let [uri "https://localhost:8090/fdb/test/chat/query"]
            (is (nil? (re-matches server-regex uri)))
            "parsed protocol, hostname, port and path correctly"))
        (testing "protocol, hostname, port and path provided"
          (let [uri "https://my-fluree-server.ee:8090/fdb/test/chat/query"]
            (is (nil? (re-matches server-regex uri))
                "uri contains path")))
        (testing "protocol, hostname, and path provided"
          (let [uri "https://my-fluree-server.ee/fdb/test/chat/query"]
            (is (nil? (re-matches server-regex uri))
                "uri contains path")))
        (testing "protocol, hostname, port, path and search provided"
          (let [uri "https://my-fluree-server.ee:8090/?example=notsupported"]
            (is (nil? (re-matches server-regex uri))
                "uri contains search criteria")))
        (testing "protocol, hostname, path and search provided"
          (let [uri "https://my-fluree-server.ee/?example=notsupported"]
            (is (nil? (re-matches server-regex uri))
                "uri contains search criteria")))
        (testing "protocol, hostname, path, port and hash provided"
          (let [uri "https://docs.flur.ee:120/#hash"]
            (is (nil? (re-matches server-regex uri))
                "uri contains hash")))
        (testing "protocol, hostname, path and hash provided"
          (let [uri "https://docs.flur.ee/#hash"]
            (is (nil? (re-matches server-regex uri))
                "uri contains hash"))))
      (testing "variations with localhost"
        (testing "protocol, hostname and port provided"
          (let [uri "http://localhost:8090"
                [href protocol hostname port pathname search hash] (re-matches server-regex uri)]
            (is (and (= uri href)
                     (= protocol "http:")
                     (= hostname "localhost")
                     (= port "8090")
                     (nil? pathname)
                     (nil? search)
                     (nil? hash))
                "parsed protocol, hostname and port correctly")))
        (testing "protocol, hostname provided"
          (let [uri "http://localhost"
                [href protocol hostname port pathname search hash] (re-matches server-regex uri)]
            (is (and (= uri href)
                     (= protocol "http:")
                     (= hostname "localhost")
                     (nil? port)
                     (nil? pathname)
                     (nil? search)
                     (nil? hash))
                "parsed protocol and hostname correctly"))))
      (testing "various uris"
        (testing "extra long hostname"
          (let [uri "https://my-fluree-server-with-a-very-long-name-ABCSDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.ee:8090"
                [href protocol hostname port pathname search hash] (re-matches server-regex uri)]
            (is (and (= uri href)
                     (= protocol "https:")
                     (= hostname "my-fluree-server-with-a-very-long-name-ABCSDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.ee")
                     (= port "8090")
                     (nil? pathname)
                     (nil? search)
                     (nil? hash))
                "parsed long uri correctly")))
        (testing "protocol, hostname and port provided"
          (let [uri "http://my-fluree-server.ee:8090"
                [href protocol hostname port pathname search hash] (re-matches server-regex uri)]
            (is (and (= uri href)
                     (= protocol "http:")
                     (= hostname "my-fluree-server.ee")
                     (= port "8090")
                     (nil? pathname)
                     (nil? search)
                     (nil? hash))
                "parsed protocol, hostname and port correctly")))
        (testing "protocol, hostname provided"
          (let [uri "http://my-fluree-server.ee"
                [href protocol hostname port pathname search hash] (re-matches server-regex uri)]
            (is (and (= uri href)
                     (= protocol "http:")
                     (= hostname "my-fluree-server.ee")
                     (nil? port)
                     (nil? pathname)
                     (nil? search)
                     (nil? hash))
                "parsed protocol and hostname correctly")))))))
