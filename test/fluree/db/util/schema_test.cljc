(ns fluree.db.util.schema-test
  (:require
    #?(:clj  [clojure.test :refer :all]
        :cljs [cljs.test :refer-macros [deftest is testing]])
    [fluree.db.constants :as const]
    [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
    [fluree.db.util.schema :as s])
  #?(:clj
     (:import (fluree.db.flake Flake))))


(deftest db-util-schema-test
  (testing "is-tx-meta-flake?"
    (is (->  [-1,100,"89ca43ee603608f0556364383d6d39b34654387c17687655e376a3cb2aee26e1",-1,true,nil]
             flake/parts->Flake
             s/is-tx-meta-flake?))
    (is (->  [1,100,"89ca43ee603608f0556364383d6d39b34654387c17687655e376a3cb2aee26e1",-1,true,nil]
             flake/parts->Flake
             s/is-tx-meta-flake?
             (= false))))

  (testing "is-schema-flake?"
    (is (-> [s/schema-sid-start,10,"_user/username",-1,true,nil]
            flake/parts->Flake
            s/is-schema-flake?))
    (is (-> [s/schema-sid-end,10,"_user/username",-1,true,nil]
            flake/parts->Flake
            s/is-schema-flake?))
    (is (-> [(dec s/schema-sid-start),10,"_user/username",-1,true,nil]
            flake/parts->Flake
            s/is-schema-flake?
            (= false)))
    (is (-> [(inc s/schema-sid-end),10,"_user/username",-1,true,nil]
            flake/parts->Flake
            s/is-schema-flake?
            (= false))))

  (testing "is-setting-flake?"
    (is (-> [s/setting-sid-start,80,"root",-1,true,nil]
            flake/parts->Flake
            s/is-setting-flake?))
    (is (-> [s/setting-sid-start,80,"root",-1,true,nil]
            flake/parts->Flake
            s/is-setting-flake?))
    (is (-> [(dec s/setting-sid-start),80,"root",-1,true,nil]
            flake/parts->Flake
            s/is-setting-flake?
            (= false)))
    (is (-> [(inc s/setting-sid-end),80,"root",-1,true,nil]
            flake/parts->Flake
            s/is-setting-flake?
            (= false))))

  (testing "is-language-flake?"
    (is (-> [158329674399744,const/$_setting:language,52776558133313,-1,true,nil]
            flake/parts->Flake
            s/is-language-flake?))
    (is (-> [158329674399744,(inc const/$_setting:language),52776558133313,-1,true,nil]
            flake/parts->Flake
            s/is-language-flake?
            (= false))))

  (testing "is-genesis-flake?"
    (is (-> [s/tag-sid-start,30,"_predicate/type:string",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/tag-sid-end,30,"_predicate/type:string",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/setting-sid-start,80,"root",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/auth-sid-start,65,123145302310912,-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/auth-sid-end,65,123145302310912,-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/role-sid-start,70,"root",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/role-sid-end,70,"root",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/rule-sid-start,83,"*",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/rule-sid-end,83,"*",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/fn-sid-start,92,"true",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/fn-sid-end,92,"true",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/collection-sid-start,40,"_predicate",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/collection-sid-end,40,"_predicate",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?
            (= false)))
    (is (-> [69,11,"Fuel this auth record has.",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?))
    (is (-> [s/predicate-sid-end,11,"Fuel this auth record has.",-1,true,nil]
            flake/parts->Flake
            s/is-genesis-flake?
            (= false))))

  (testing "add-to-post-preds?"
    (is (-> (map flake/parts->Flake [[1008,15,true,-3,true,nil]])
            (s/add-to-post-preds? 2000)
            some?))
    (is (-> (map flake/parts->Flake [[1008,15,false,-3,true,nil]])
            (s/add-to-post-preds? 2000)
            empty?)))

  (testing "remove-from-post-preds"
    (is (-> (map flake/parts->Flake [[1008,15,true,-3,true,nil]])
            s/remove-from-post-preds
            empty?))
    (is (-> (map flake/parts->Flake [[1008,15,false,-3,true,nil]])
            s/remove-from-post-preds
            some?)))

  (testing "schema-change?"
    (is (->> [[1008,15,false,-3,true,nil]
              [s/auth-sid-end,65,123145302310912,-1,true,nil]]
             (map flake/parts->Flake)
             s/schema-change?))
    (is (->> [[s/role-sid-start,70,"root",-1,true,nil]
              [s/auth-sid-end,65,123145302310912,-1,true,nil]]
             (map flake/parts->Flake)
             s/schema-change?
             nil?)))

  (testing "setting-change?"
    (is (->> [[s/setting-sid-start,80,"root",-1,true,nil]
              [1008,15,false,-3,true,nil]
              [s/auth-sid-end,65,123145302310912,-1,true,nil]]
             (map flake/parts->Flake)
             s/setting-change?))
    (is (->> [[1008,15,false,-3,true,nil]
              [s/auth-sid-end,65,123145302310912,-1,true,nil]]
             (map flake/parts->Flake)
             s/setting-change?
             nil?)))

  (testing "get-language-change"
    (is (->> [[s/setting-sid-start,const/$_setting:language,52776558133313,-1,true,nil]]
             (map flake/parts->Flake)
             s/get-language-change
             some?))
    (is (->> [[(dec s/setting-sid-start),const/$_setting:language,52776558133313,-1,true,nil]]
             (map flake/parts->Flake)
             s/get-language-change
             nil?))
    (is (->> [[s/setting-sid-start,(inc const/$_setting:language),52776558133313,-1,true,nil]]
             (map flake/parts->Flake)
             s/get-language-change
             nil?)))

  (testing "is-pred-flake?"
    (is (-> [s/rule-sid-end,83,"*",-1,true,nil]
            flake/parts->Flake
            s/is-pred-flake?
            (= false)))
    (is (-> [flake/MIN-PREDICATE-ID,83,"*",-1,true,nil]
            flake/parts->Flake
            s/is-pred-flake?))
    (is (-> [flake/MAX-PREDICATE-ID,83,"*",-1,true,nil]
            flake/parts->Flake
            s/is-pred-flake?)))

  (testing "pred-change?"
    (is (->> [[s/rule-sid-end,83,"*",-1,true,nil]
              [flake/MIN-PREDICATE-ID,83,"*",-1,true,nil]]
             (map flake/parts->Flake)
             s/pred-change?))
    (is (->> [[s/rule-sid-end,83,"*",-1,true,nil]
              [(dec flake/MIN-PREDICATE-ID),83,"*",-1,true,nil]]
             (map flake/parts->Flake)
             s/pred-change?
             nil?))))