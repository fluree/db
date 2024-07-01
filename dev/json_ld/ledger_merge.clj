(ns ledger-merge
  (:require [fluree.db :as fluree]
            [fluree.db.did :as did]))

(def sample-order
  {"@context"    "https://schema.org",
   "@id"         "http://data.squareup.com/orders/order123"
   "@type"       "Order",
   "seller"      {"@id"   "acme"
                  "@type" "Organization",
                  "name"  "ACME Supplies"},
   "customer"    {"@id"   "http://data.squareup.com/customer/xyz123"
                  "@type" "Person",
                  "name"  "Jane Doe"},
   "orderedItem" [{"@id"             "item123"
                   "@type"           "OrderItem",
                   "orderItemNumber" "abc123",
                   "orderQuantity"   1,
                   "orderedItem"     {"@id"   "prod/widget"
                                      "@type" "Product",
                                      "name"  "Widget"},
                   "orderItemStatus" "http://schema.org/OrderDelivered",
                   "orderDelivery"   {"@id"                 "delivery/xyz"
                                      "@type"               "ParcelDelivery",
                                      "expectedArrivalFrom" "2015-03-10"}}
                  {"@id"             "item456"
                   "@type"           "OrderItem",
                   "orderItemNumber" "def456",
                   "orderQuantity"   3,
                   "orderedItem"     {"@id"   "prod/widget-accessories"
                                      "@type" "Product",
                                      "name"  "Widget accessories"},
                   "orderItemStatus" "http://schema.org/OrderInTransit",
                   "orderDelivery"   {"@id"                  "parcel/xzy"
                                      "@type"                "ParcelDelivery",
                                      "expectedArrivalFrom"  "2015-03-15",
                                      "expectedArrivalUntil" "2015-03-18"}}]})

(-> sample-order
    (json/stringify)
    (println))

(def sample-customer
  {"@context" "https://schema.org",
   "@graph"   [{"@id"   "http://data.squareup.com/customer/xyz123"
                "@type" "Person",
                "name"  "Jane Doe"}
               {"@id"      "http://data.squareup.com/orders/order123"
                "customer" "http://data.squareup.com/customer/xyz123"}]})
(comment

  (def ipfs-conn
    @(fluree/connect-ipfs
       {:server   nil                                       ;; use default
        :defaults {:ipns    {:key "Fluree1"}                ;; publish to ipns by default using the provided key/profile
                   :context {:id       "@id"
                             :type     "@type"
                             :schema   "http://schema.org/"
                             :sq-cust  "http://data.squareup.com/customer/"
                             :sq-order "http://data.squareup.com/orders/"
                             :rdf      "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                             :rdfs     "http://www.w3.org/2000/01/rdf-schema#"
                             :wiki     "https://www.wikidata.org/wiki/"
                             :skos     "http://www.w3.org/2008/05/skos#"
                             :f        "https://ns.flur.ee/ledger#"}
                   :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))

  (def order-ledger
    @(fluree/create ipfs-conn "square-ext/orders"
                    {:controller "did:fluree:ipns/did.flur.ee/main"}))

  (def cust-ledger
    @(fluree/create ipfs-conn "square-int/customers"
                    {:controller "did:fluree:ipns/did.flur.ee/main"}))

  (def order-db @(fluree/stage order-ledger sample-order))

  (def cust-db @(fluree/stage cust-ledger sample-customer))

  @(fluree/query order-db {:select [:* {:schema/orderedItem [:*]}]
                           :from   :sq-order/order123})

  @(fluree/query cust-db {:select [:*]
                          :from   :sq-cust/xyz123})

  @(fluree/commit! order-db {:message "First order commit"
                             :push?   true})
  @(fluree/commit! cust-db {:message "First customer commit"
                            :push?   true})

  (fluree/status order-ledger)
  (fluree/status cust-ledger)

  (def joint-ledger
    @(fluree/create ipfs-conn "square-int/joint"
                    {:include    ["fluree:ipns://data.fluree.com/square-ext/orders"
                                  "fluree:ipns://data.fluree.com/square-int/customers"]
                     :controller "did:fluree:ipns/did.flur.ee/main"}))

  @(fluree/query (fluree/db joint-ledger)
                 {:select [:* {:schema/customer [:*]}]
                  :from   :sq-order/order123})

  (-> (fluree/db joint-ledger)
      :novelty
      :spot)

  )


