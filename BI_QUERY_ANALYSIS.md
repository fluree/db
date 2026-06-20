# BSBM Business Intelligence — Fluree query-plan analysis

Internal analysis to investigate the Fluree query-planner behavior on the BSBM Business-Intelligence queries. **Not a published benchmark document.**

## Context

- **Engine:** Fluree v4.0.6 (release tag `a600e0d3`).
- **Dataset:** BSBM 100M (`-fc` forward-chained), ledger `bsbm100`, **100,000,978 flakes**.
- **Box:** AWS m7a.4xlarge (16c / 64 GB), Ubuntu 24.04.
- **Plans** captured with `fluree query bsbm100 -f q.rq --explain --sparql --format json`.
- **`this-run` times** are a single execution of the *instantiated* query below (one parameter set); the **benchmark** column is the BSBM driver's average over the seeded run (seed 808080) and is the authoritative figure.

### Cross-engine benchmark — BI @ 100M, single-client, seed 808080

AQET seconds (result rows). Same query parameters across all three engines.

| Query | Fluree | Virtuoso 7 | QLever |
|---|--:|--:|--:|
| Q1 | 7.06 (10 rows) | 0.66 (10) | 0.23 (10) |
| Q2 | 0.12 (10 rows) | 0.07 (10) | 2.60 (10) |
| Q3 | 8.51 (10 rows) | 1.34 (10) | 0.25 (10) |
| Q4 | 35.49 (10 rows) | 120.0 (0, timeout) | 72.08 (5, timeout) |
| Q5 | 15.38 (29 rows) | 0.00 (0 rows) | 0.45 (29) |
| Q6 | 0.88 (37 rows) | 0.04 (37) | 0.14 (37) |
| Q7 | 2.45 (83 rows) | 0.09 (83) | 0.16 (84) |
| Q8 | 19.63 (10 rows) | 13.51 (10) | 5.61 (10) |

Fluree is markedly slower than **both** Virtuoso and QLever on **Q1, Q3, Q5** (and Q7/Q8 to a lesser degree), all returning identical correct results. Fluree is fastest on **Q4** (and the only engine to complete it).

### Reading the physical plan

Each plan node shows the operator and, where relevant: `hash-join-chosen` (true/false), `hash-join-reason` (why a hash join was or wasn't used), `probe-count` / `driving-est` (rows fed into the join), and `est-rows`. `NestedLoopJoinOperator` with `hash-join-chosen=false` is the pattern to look at for the slow queries.

Instantiation parameters used below: `ProductType`=`instances/ProductType1974` (leaf type, ~38 products), `Country1`=`#DE`, `Country2`/`Country`=`#US`, `ConsecutiveMonth`=2008-01-01 / -02-01 / -03-01, `Producer`=`dataFromProducer2095/Producer2095`, `Product`=`dataFromProducer1/Product1`.

> **Parameter representativeness:** Q1/Q3/Q5 reproduce the benchmark slowness well with
> these params (this-run time ≈ benchmark). **Q7 and Q8 do NOT** — Q7 returned 0 rows
> (vs ~83) and Q8 ran ~5× faster than the benchmark because `ProductType1974` is a small
> leaf type. Treat Q7/Q8 plans as illustrative only and re-validate with representative
> params before drawing planner conclusions (see the ⚠️ notes on each).

---

## Q1

*Products ranked by review count, where the producer is in one country and reviewers are in another.*

- **Benchmark @100M (seed 808080):** Fluree **7.06 (10 rows)** · Virtuoso 0.66 (10) · QLever 0.23 (10)
- **This-run (instantiated, single exec):** 11.08 s, 10 rows

**Physical plan (operator tree):**

- **LimitOperator**
  - **ProjectOperator**
    - **SortOperator**
      - **SubqueryOperator**  [join-mode=True]
        - **EmptyOperator**  [est-rows=1]
        - **SubqueryBody**
          - **ProjectOperator**
            - **GroupAggregateOperator**
              - **NestedLoopJoinOperator**  [right=?v2 <14:country> <19:DE>]
                - **NestedLoopJoinOperator**  [right=?v1 <14:producer> ?v2, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=284826, driving-est=1175036]
                  - **NestedLoopJoinOperator**  [right=?v0 <3:type> <14:ProductType>]
                    - **NestedLoopJoinOperator**  [right=?v1 <3:type> ?v0, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=10458238, driving-est=587518]
                      - **NestedLoopJoinOperator**  [right=?v3 <14:reviewFor> ?v1, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=2848260, driving-est=293759]
                        - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=184.2, probe-count=2848260, driving-est=15461]
                          - **NestedLoopJoinOperator**  [est-rows=10, right=?v4 <14:country> <19:US>]
                            - **EmptyOperator**  [est-rows=1]
                          - **DatasetOperator**

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
prefix rev: <http://purl.org/stuff/rev#>

Select ?productType ?reviewCount
{
 { Select ?productType (count(?review) As ?reviewCount)
  {
   ?productType a bsbm:ProductType .
   ?product a ?productType .
   ?product bsbm:producer ?producer .
   ?producer bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .
   ?review bsbm:reviewFor ?product .
   ?review rev:reviewer ?reviewer .
   ?reviewer bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
  }
  Group By ?productType
 }
}
Order By desc(?reviewCount) ?productType
Limit 10
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 25682374673308920
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?productType",
    "?reviewCount"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 233562
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?productType",
      "property": "@type",
      "object": "bsbm:ProductType"
     }
    },
    {
     "estimate": {
      "row-count": 5217
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "@type",
      "object": "?productType"
     }
    },
    {
     "estimate": {
      "row-count": 2
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "bsbm:producer",
      "object": "?producer"
     }
    },
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?producer",
      "property": "bsbm:country",
      "object": "http://downlode.org/rdf/iso-3166/countries#DE"
     }
    },
    {
     "estimate": {
      "row-count": 10
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "bsbm:reviewFor",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 2
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "rev:reviewer",
      "object": "?reviewer"
     }
    },
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?reviewer",
      "property": "bsbm:country",
      "object": "http://downlode.org/rdf/iso-3166/countries#US"
     }
    }
   ]
  }
 ],
 "physical": {
  "op": "LimitOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "ProjectOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "SortOperator",
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "SubqueryOperator",
           "details": {
            "join-mode": true
           },
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "EmptyOperator",
              "est-rows": 1
             }
            },
            {
             "rel": "child",
             "node": {
              "op": "SubqueryBody",
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "ProjectOperator",
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "GroupAggregateOperator",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "NestedLoopJoinOperator",
                       "details": {
                        "right": "?v2 <14:country> <19:DE>"
                       },
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "NestedLoopJoinOperator",
                          "details": {
                           "right": "?v1 <14:producer> ?v2",
                           "hash-join-chosen": false,
                           "hash-join-reason": "subject-driven-forward-join",
                           "probe-count": 284826,
                           "driving-est": 1175036
                          },
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "NestedLoopJoinOperator",
                             "details": {
                              "right": "?v0 <3:type> <14:ProductType>"
                             },
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "details": {
                                 "right": "?v1 <3:type> ?v0",
                                 "hash-join-chosen": false,
                                 "hash-join-reason": "subject-driven-forward-join",
                                 "probe-count": 10458238,
                                 "driving-est": 587518
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "NestedLoopJoinOperator",
                                   "details": {
                                    "right": "?v3 <14:reviewFor> ?v1",
                                    "hash-join-chosen": false,
                                    "hash-join-reason": "subject-driven-forward-join",
                                    "probe-count": 2848260,
                                    "driving-est": 293759
                                   },
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "HashJoinOperator",
                                      "details": {
                                       "probe": "?v3 <http://purl.org/stuff/rev#reviewer> ?v4",
                                       "hash-join-chosen": true,
                                       "hash-join-reason": "cost-wins",
                                       "probe-count": 2848260,
                                       "driving-est": 15461,
                                       "scan-ratio": "184.2"
                                      },
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "NestedLoopJoinOperator",
                                         "est-rows": 10,
                                         "details": {
                                          "right": "?v4 <14:country> <19:US>"
                                         },
                                         "children": [
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "EmptyOperator",
                                            "est-rows": 1
                                           }
                                          }
                                         ]
                                        }
                                       },
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "DatasetOperator",
                                         "details": {
                                          "pattern": "?v3 <http://purl.org/stuff/rev#reviewer> ?v4"
                                         }
                                        }
                                       }
                                      ]
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---

## Q2

*Products sharing the most product-features with a given product.*

- **Benchmark @100M (seed 808080):** Fluree **0.12 (10 rows)** · Virtuoso 0.07 (10) · QLever 2.60 (10)
- **This-run (instantiated, single exec):** 0.27 s, 10 rows

**Physical plan (operator tree):**

- **LimitOperator**
  - **ProjectOperator**
    - **SortOperator**
      - **NestedLoopJoinOperator**  [right=?v0 <3:type> <14:Product>]
        - **FilterOperator**
          - **SubqueryOperator**  [join-mode=True]
            - **EmptyOperator**  [est-rows=1]
            - **SubqueryBody**
              - **ProjectOperator**
                - **GroupAggregateOperator**
                  - **NestedLoopJoinOperator**  [est-rows=100, right=?v0 <14:productFeature> ?v1, hash-join-chosen=False, hash-join-reason=scan-ratio-too-high, scan-ratio=263515.8, probe-count=5533832, driving-est=21]
                    - **NestedLoopJoinOperator**  [est-rows=10, right=<16:Product1> <14:productFeature> ?v1]
                      - **EmptyOperator**  [est-rows=1]

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> 

  SELECT ?otherProduct ?sameFeatures
  {
    ?otherProduct a bsbm:Product .
    FILTER(?otherProduct != <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1>)
    {
      SELECT ?otherProduct (count(?otherFeature) As ?sameFeatures)
      {
        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1> bsbm:productFeature ?feature .
        ?otherProduct bsbm:productFeature ?otherFeature .
        FILTER(?feature=?otherFeature)
      }
      Group By ?otherProduct
    }
  }
  Order By desc(?sameFeatures) ?otherProduct
  Limit 10
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 233562
   },
   "category": "source",
   "kind": "triple",
   "pattern": {
    "subject": "?otherProduct",
    "property": "@type",
    "object": "bsbm:Product"
   }
  },
  {
   "category": "deferred",
   "kind": "filter"
  },
  {
   "estimate": {
    "row-count": 116210472
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?otherProduct",
    "?sameFeatures"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 21
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1",
      "property": "bsbm:productFeature",
      "object": "?feature"
     }
    },
    {
     "estimate": {
      "row-count": 21
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?otherProduct",
      "property": "bsbm:productFeature",
      "object": "?otherFeature"
     }
    },
    {
     "category": "deferred",
     "kind": "filter"
    }
   ]
  }
 ],
 "physical": {
  "op": "LimitOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "ProjectOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "SortOperator",
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "NestedLoopJoinOperator",
           "details": {
            "right": "?v0 <3:type> <14:Product>"
           },
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "FilterOperator",
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "SubqueryOperator",
                 "details": {
                  "join-mode": true
                 },
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "EmptyOperator",
                    "est-rows": 1
                   }
                  },
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryBody",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "ProjectOperator",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "GroupAggregateOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "NestedLoopJoinOperator",
                             "est-rows": 100,
                             "details": {
                              "right": "?v0 <14:productFeature> ?v1",
                              "hash-join-chosen": false,
                              "hash-join-reason": "scan-ratio-too-high",
                              "probe-count": 5533832,
                              "driving-est": 21,
                              "scan-ratio": "263515.8"
                             },
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "est-rows": 10,
                                "details": {
                                 "right": "<16:Product1> <14:productFeature> ?v1"
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "EmptyOperator",
                                   "est-rows": 1
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---

## Q3

*Products with the largest review-count ratio between two consecutive months.*

- **Benchmark @100M (seed 808080):** Fluree **8.51 (10 rows)** · Virtuoso 1.34 (10) · QLever 0.25 (10)
- **This-run (instantiated, single exec):** 9.14 s, 10 rows

**Physical plan (operator tree):**

- **LimitOperator**
  - **ProjectOperator**
    - **SortOperator**
      - **BindOperator**
        - **BindOperator**
          - **SubqueryOperator**  [join-mode=True]
            - **SubqueryOperator**  [join-mode=True]
              - **EmptyOperator**  [est-rows=1]
              - **SubqueryBody**
                - **ProjectOperator**
                  - **GroupAggregateOperator**
                    - **NestedLoopJoinOperator**  [est-rows=100, right=?v0 <15:date> ?v2]
                      - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <14:reviewFor> ?v1]
                        - **EmptyOperator**  [est-rows=1]
            - **SubqueryBody**
              - **ProjectOperator**
                - **HavingOperator**
                  - **GroupAggregateOperator**
                    - **NestedLoopJoinOperator**  [est-rows=100, right=?v0 <15:date> ?v2]
                      - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <14:reviewFor> ?v1]
                        - **EmptyOperator**  [est-rows=1]

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
  prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
  prefix rev: <http://purl.org/stuff/rev#>
  prefix dc: <http://purl.org/dc/elements/1.1/>
  prefix xsd: <http://www.w3.org/2001/XMLSchema#>

  Select ?product (xsd:float(?monthCount)/?monthBeforeCount As ?ratio)
  {
    { Select ?product (count(?review) As ?monthCount)
      {
        ?review bsbm:reviewFor ?product .
        ?review dc:date ?date .
        Filter(?date >= "2008-02-01"^^<http://www.w3.org/2001/XMLSchema#date> && ?date < "2008-03-01"^^<http://www.w3.org/2001/XMLSchema#date>) 
      }
      Group By ?product
    }  {
      Select ?product (count(?review) As ?monthBeforeCount)
      {
        ?review bsbm:reviewFor ?product .
        ?review dc:date ?date .
        Filter(?date >= "2008-01-01"^^<http://www.w3.org/2001/XMLSchema#date> && ?date < "2008-02-01"^^<http://www.w3.org/2001/XMLSchema#date>) #
      }
      Group By ?product
      Having (count(?review)>0)
    }
  }
  Order By desc(xsd:float(?monthCount) / ?monthBeforeCount) ?product
  Limit 10
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 5696520
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?product",
    "?monthCount"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 2848260
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "bsbm:reviewFor",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 2
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "dc:date",
      "object": "?date"
     }
    },
    {
     "category": "deferred",
     "kind": "filter"
    }
   ]
  },
  {
   "estimate": {
    "row-count": 5696520
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?product",
    "?monthBeforeCount"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 10
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "bsbm:reviewFor",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 2
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "dc:date",
      "object": "?date"
     }
    },
    {
     "category": "deferred",
     "kind": "filter"
    }
   ]
  },
  {
   "category": "deferred",
   "kind": "bind",
   "var": "?ratio"
  }
 ],
 "physical": {
  "op": "LimitOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "ProjectOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "SortOperator",
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "BindOperator",
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "BindOperator",
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "SubqueryOperator",
                 "details": {
                  "join-mode": true,
                  "correlation-vars": [
                   "?v1"
                  ]
                 },
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryOperator",
                    "details": {
                     "join-mode": true
                    },
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "EmptyOperator",
                       "est-rows": 1
                      }
                     },
                     {
                      "rel": "child",
                      "node": {
                       "op": "SubqueryBody",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "ProjectOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "GroupAggregateOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "est-rows": 100,
                                "details": {
                                 "right": "?v0 <15:date> ?v2"
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "NestedLoopJoinOperator",
                                   "est-rows": 10,
                                   "details": {
                                    "right": "?v0 <14:reviewFor> ?v1"
                                   },
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "EmptyOperator",
                                      "est-rows": 1
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  },
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryBody",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "ProjectOperator",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "HavingOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "GroupAggregateOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "est-rows": 100,
                                "details": {
                                 "right": "?v0 <15:date> ?v2"
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "NestedLoopJoinOperator",
                                   "est-rows": 10,
                                   "details": {
                                    "right": "?v0 <14:reviewFor> ?v1"
                                   },
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "EmptyOperator",
                                      "est-rows": 1
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---

## Q4

*Per feature, the ratio of avg offer price for products WITH the feature vs WITHOUT it.*

- **Benchmark @100M (seed 808080):** Fluree **35.49 (10 rows)** · Virtuoso 120.0 (0, timeout) · QLever 72.08 (5, timeout)
- **This-run (instantiated, single exec):** 2.65 s, 10 rows

**Physical plan (operator tree):**

- **LimitOperator**
  - **ProjectOperator**
    - **SortOperator**
      - **BindOperator**
        - **BindOperator**
          - **SubqueryOperator**  [join-mode=True]
            - **SubqueryOperator**  [join-mode=True]
              - **EmptyOperator**  [est-rows=1]
              - **SubqueryBody**
                - **ProjectOperator**
                  - **GroupAggregateOperator**
                    - **NestedLoopJoinOperator**  [right=?v0 <14:productFeature> ?v1, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=5533832, driving-est=4671240]
                      - **NestedLoopJoinOperator**  [right=?v2 <14:price> ?v3]
                        - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=24.4, probe-count=5696520, driving-est=233562]
                          - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <3:type> <13:ProductType1974>]
                            - **EmptyOperator**  [est-rows=1]
                          - **DatasetOperator**
            - **SubqueryBody**
              - **ProjectOperator**
                - **BindOperator**
                  - **FilterOperator**
                    - **BindOperator**
                      - **BindOperator**
                        - **OptionalOperator**
                          - **SubqueryOperator**  [join-mode=True]
                            - **SubqueryOperator**  [join-mode=True]
                              - **EmptyOperator**  [est-rows=1]
                              - **SubqueryBody**
                                - **ProjectOperator**
                                  - **GroupAggregateOperator**
                                    - **NestedLoopJoinOperator**  [right=?v2 <14:price> ?v3]
                                      - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=24.4, probe-count=5696520, driving-est=233562]
                                        - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <3:type> <13:ProductType1974>]
                                          - **EmptyOperator**  [est-rows=1]
                                        - **DatasetOperator**
                            - **SubqueryBody**
                              - **DistinctOperator**  [est-rows=100]
                                - **ProjectOperator**  [est-rows=100]
                                  - **NestedLoopJoinOperator**  [est-rows=100, right=?v6 <14:productFeature> ?v1, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=5533832, driving-est=233562]
                                    - **NestedLoopJoinOperator**  [est-rows=10, right=?v6 <3:type> <13:ProductType1974>]
                                      - **EmptyOperator**  [est-rows=1]

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
  prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
  prefix xsd: <http://www.w3.org/2001/XMLSchema#>

  Select ?feature (?withFeaturePrice/?withoutFeaturePrice As ?priceRatio)
  {
    { Select ?feature (avg(xsd:float(xsd:string(?price))) As ?withFeaturePrice)
      {
        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> ;
                 bsbm:productFeature ?feature .
        ?offer bsbm:product ?product ;
               bsbm:price ?price .
      }
      Group By ?feature
    }
    { Select ?feature (avg(xsd:float(xsd:string(?price))) As ?withoutFeaturePrice)
      {
        { Select distinct ?feature { 
          ?p a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> ;
             bsbm:productFeature ?feature .
        } }
        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
        ?offer bsbm:product ?product ;
               bsbm:price ?price .
        FILTER NOT EXISTS { ?product bsbm:productFeature ?feature }
      }
      Group By ?feature
    }
  }
  Order By desc(?withFeaturePrice/?withoutFeaturePrice) ?feature
  Limit 10
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 98096040
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?feature",
    "?withFeaturePrice"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 233562
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "@type",
      "object": "bsbm-inst:ProductType1974"
     }
    },
    {
     "estimate": {
      "row-count": 21
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "bsbm:productFeature",
      "object": "?feature"
     }
    },
    {
     "estimate": {
      "row-count": 20
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:product",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:price",
      "object": "?price"
     }
    },
    {
     "category": "deferred",
     "kind": "bind",
     "var": "?__agg_expr_0"
    }
   ]
  },
  {
   "estimate": {
    "row-count": 22911507294480
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?feature",
    "?withoutFeaturePrice"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 4904802
     },
     "category": "source",
     "kind": "subquery",
     "select": [
      "?feature"
     ],
     "patterns": [
      {
       "estimate": {
        "row-count": 233562
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?p",
        "property": "@type",
        "object": "bsbm-inst:ProductType1974"
       }
      },
      {
       "estimate": {
        "row-count": 1
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?p",
        "property": "bsbm:productFeature",
        "object": "?feature"
       }
      }
     ]
    },
    {
     "estimate": {
      "row-count": 233562
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "@type",
      "object": "bsbm-inst:ProductType1974"
     }
    },
    {
     "estimate": {
      "row-count": 20
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:product",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:price",
      "object": "?price"
     }
    },
    {
     "category": "deferred",
     "kind": "not-exists",
     "patterns": [
      {
       "estimate": {
        "row-count": 1
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?product",
        "property": "bsbm:productFeature",
        "object": "?feature"
       }
      }
     ]
    },
    {
     "category": "deferred",
     "kind": "bind",
     "var": "?__agg_expr_1"
    }
   ]
  },
  {
   "category": "deferred",
   "kind": "bind",
   "var": "?priceRatio"
  }
 ],
 "physical": {
  "op": "LimitOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "ProjectOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "SortOperator",
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "BindOperator",
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "BindOperator",
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "SubqueryOperator",
                 "details": {
                  "join-mode": true,
                  "correlation-vars": [
                   "?v1"
                  ]
                 },
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryOperator",
                    "details": {
                     "join-mode": true
                    },
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "EmptyOperator",
                       "est-rows": 1
                      }
                     },
                     {
                      "rel": "child",
                      "node": {
                       "op": "SubqueryBody",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "ProjectOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "GroupAggregateOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "details": {
                                 "right": "?v0 <14:productFeature> ?v1",
                                 "hash-join-chosen": false,
                                 "hash-join-reason": "subject-driven-forward-join",
                                 "probe-count": 5533832,
                                 "driving-est": 4671240
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "NestedLoopJoinOperator",
                                   "details": {
                                    "right": "?v2 <14:price> ?v3"
                                   },
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "HashJoinOperator",
                                      "details": {
                                       "probe": "?v2 <14:product> ?v0",
                                       "hash-join-chosen": true,
                                       "hash-join-reason": "cost-wins",
                                       "probe-count": 5696520,
                                       "driving-est": 233562,
                                       "scan-ratio": "24.4"
                                      },
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "NestedLoopJoinOperator",
                                         "est-rows": 10,
                                         "details": {
                                          "right": "?v0 <3:type> <13:ProductType1974>"
                                         },
                                         "children": [
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "EmptyOperator",
                                            "est-rows": 1
                                           }
                                          }
                                         ]
                                        }
                                       },
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "DatasetOperator",
                                         "details": {
                                          "pattern": "?v2 <14:product> ?v0"
                                         }
                                        }
                                       }
                                      ]
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  },
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryBody",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "ProjectOperator",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "BindOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "FilterOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "BindOperator",
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "BindOperator",
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "OptionalOperator",
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "SubqueryOperator",
                                         "details": {
                                          "join-mode": true
                                         },
                                         "children": [
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "SubqueryOperator",
                                            "details": {
                                             "join-mode": true
                                            },
                                            "children": [
                                             {
                                              "rel": "child",
                                              "node": {
                                               "op": "EmptyOperator",
                                               "est-rows": 1
                                              }
                                             },
                                             {
                                              "rel": "child",
                                              "node": {
                                               "op": "SubqueryBody",
                                               "children": [
                                                {
                                                 "rel": "child",
                                                 "node": {
                                                  "op": "ProjectOperator",
                                                  "children": [
                                                   {
                                                    "rel": "child",
                                                    "node": {
                                                     "op": "GroupAggregateOperator",
                                                     "children": [
                                                      {
                                                       "rel": "child",
                                                       "node": {
                                                        "op": "NestedLoopJoinOperator",
                                                        "details": {
                                                         "right": "?v2 <14:price> ?v3"
                                                        },
                                                        "children": [
                                                         {
                                                          "rel": "child",
                                                          "node": {
                                                           "op": "HashJoinOperator",
                                                           "details": {
                                                            "probe": "?v2 <14:product> ?v0",
                                                            "hash-join-chosen": true,
                                                            "hash-join-reason": "cost-wins",
                                                            "probe-count": 5696520,
                                                            "driving-est": 233562,
                                                            "scan-ratio": "24.4"
                                                           },
                                                           "children": [
                                                            {
                                                             "rel": "child",
                                                             "node": {
                                                              "op": "NestedLoopJoinOperator",
                                                              "est-rows": 10,
                                                              "details": {
                                                               "right": "?v0 <3:type> <13:ProductType1974>"
                                                              },
                                                              "children": [
                                                               {
                                                                "rel": "child",
                                                                "node": {
                                                                 "op": "EmptyOperator",
                                                                 "est-rows": 1
                                                                }
                                                               }
                                                              ]
                                                             }
                                                            },
                                                            {
                                                             "rel": "child",
                                                             "node": {
                                                              "op": "DatasetOperator",
                                                              "details": {
                                                               "pattern": "?v2 <14:product> ?v0"
                                                              }
                                                             }
                                                            }
                                                           ]
                                                          }
                                                         }
                                                        ]
                                                       }
                                                      }
                                                     ]
                                                    }
                                                   }
                                                  ]
                                                 }
                                                }
                                               ]
                                              }
                                             }
                                            ]
                                           }
                                          },
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "SubqueryBody",
                                            "children": [
                                             {
                                              "rel": "child",
                                              "node": {
                                               "op": "DistinctOperator",
                                               "est-rows": 100,
                                               "children": [
                                                {
                                                 "rel": "child",
                                                 "node": {
                                                  "op": "ProjectOperator",
                                                  "est-rows": 100,
                                                  "children": [
                                                   {
                                                    "rel": "child",
                                                    "node": {
                                                     "op": "NestedLoopJoinOperator",
                                                     "est-rows": 100,
                                                     "details": {
                                                      "right": "?v6 <14:productFeature> ?v1",
                                                      "hash-join-chosen": false,
                                                      "hash-join-reason": "subject-driven-forward-join",
                                                      "probe-count": 5533832,
                                                      "driving-est": 233562
                                                     },
                                                     "children": [
                                                      {
                                                       "rel": "child",
                                                       "node": {
                                                        "op": "NestedLoopJoinOperator",
                                                        "est-rows": 10,
                                                        "details": {
                                                         "right": "?v6 <3:type> <13:ProductType1974>"
                                                        },
                                                        "children": [
                                                         {
                                                          "rel": "child",
                                                          "node": {
                                                           "op": "EmptyOperator",
                                                           "est-rows": 1
                                                          }
                                                         }
                                                        ]
                                                       }
                                                      }
                                                     ]
                                                    }
                                                   }
                                                  ]
                                                 }
                                                }
                                               ]
                                              }
                                             }
                                            ]
                                           }
                                          }
                                         ]
                                        }
                                       }
                                      ]
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---

## Q5

*Per country, the product with the most reviews, plus that product's average offer price.*

- **Benchmark @100M (seed 808080):** Fluree **15.38 (29 rows)** · Virtuoso 0.00 (0 rows) · QLever 0.45 (29)
- **This-run (instantiated, single exec):** 13.07 s, 12 rows

**Physical plan (operator tree):**

- **ProjectOperator**
  - **SortOperator**
    - **FilterOperator**
      - **SubqueryOperator**  [join-mode=True]
        - **SubqueryOperator**  [join-mode=True]
          - **SubqueryOperator**  [join-mode=True]
            - **EmptyOperator**  [est-rows=1]
            - **SubqueryBody**
              - **ProjectOperator**
                - **GroupAggregateOperator**
                  - **NestedLoopJoinOperator**  [right=?v0 <3:type> <13:ProductType1974>]
                    - **NestedLoopJoinOperator**  [right=?v1 <14:reviewFor> ?v0, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=2848260, driving-est=2937533]
                      - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=18.4, probe-count=2848260, driving-est=154607]
                        - **NestedLoopJoinOperator**  [est-rows=10, right=?v2 <14:country> ?v3]
                          - **EmptyOperator**  [est-rows=1]
                        - **DatasetOperator**
          - **SubqueryBody**
            - **ProjectOperator**
              - **GroupAggregateOperator**
                - **SubqueryOperator**  [join-mode=True]
                  - **EmptyOperator**  [est-rows=1]
                  - **SubqueryBody**
                    - **ProjectOperator**
                      - **GroupAggregateOperator**
                        - **NestedLoopJoinOperator**  [right=?v0 <3:type> <13:ProductType1974>]
                          - **NestedLoopJoinOperator**  [right=?v1 <14:reviewFor> ?v0, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=2848260, driving-est=2937533]
                            - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=18.4, probe-count=2848260, driving-est=154607]
                              - **NestedLoopJoinOperator**  [est-rows=10, right=?v2 <14:country> ?v3]
                                - **EmptyOperator**  [est-rows=1]
                              - **DatasetOperator**
        - **SubqueryBody**
          - **ProjectOperator**
            - **GroupAggregateOperator**
              - **BindOperator**
                - **NestedLoopJoinOperator**  [right=?v6 <14:price> ?v7]
                  - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=24.4, probe-count=5696520, driving-est=233562]
                    - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <3:type> <13:ProductType1974>]
                      - **EmptyOperator**  [est-rows=1]
                    - **DatasetOperator**

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
  prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
  prefix rev: <http://purl.org/stuff/rev#>
  prefix xsd: <http://www.w3.org/2001/XMLSchema#>

  Select ?country ?product ?nrOfReviews ?avgPrice
  {
    { Select ?country (max(?nrOfReviews) As ?maxReviews)
      {
        { Select ?country ?product (count(?review) As ?nrOfReviews)
          {
            ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
            ?review bsbm:reviewFor ?product ;
                    rev:reviewer ?reviewer .
            ?reviewer bsbm:country ?country .
          }
          Group By ?country ?product
        }
      }
      Group By ?country
    }
    { Select ?country ?product (avg(xsd:float(xsd:string(?price))) As ?avgPrice)
      {
        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
        ?offer bsbm:product ?product .
        ?offer bsbm:price ?price .
      }
      Group By ?country ?product
    }
    { Select ?country ?product (count(?review) As ?nrOfReviews)
      {
        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
        ?review bsbm:reviewFor ?product .
        ?review rev:reviewer ?reviewer .
        ?reviewer bsbm:country ?country .
      }
      Group By ?country ?product
    }
    FILTER(?nrOfReviews=?maxReviews)
  }
  Order By desc(?nrOfReviews) ?country ?product
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 361103201340
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?country",
    "?product",
    "?nrOfReviews"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 233562
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "@type",
      "object": "bsbm-inst:ProductType1974"
     }
    },
    {
     "estimate": {
      "row-count": 10
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "bsbm:reviewFor",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 2
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "rev:reviewer",
      "object": "?reviewer"
     }
    },
    {
     "estimate": {
      "row-count": 2
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?reviewer",
      "property": "bsbm:country",
      "object": "?country"
     }
    }
   ]
  },
  {
   "estimate": {
    "row-count": 361103201340000000
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?country",
    "?maxReviews"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 361103201340
     },
     "category": "source",
     "kind": "subquery",
     "select": [
      "?country",
      "?product",
      "?nrOfReviews"
     ],
     "patterns": [
      {
       "estimate": {
        "row-count": 1
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?product",
        "property": "@type",
        "object": "bsbm-inst:ProductType1974"
       }
      },
      {
       "estimate": {
        "row-count": 10
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?review",
        "property": "bsbm:reviewFor",
        "object": "?product"
       }
      },
      {
       "estimate": {
        "row-count": 2
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?review",
        "property": "rev:reviewer",
        "object": "?reviewer"
       }
      },
      {
       "estimate": {
        "row-count": 1
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?reviewer",
        "property": "bsbm:country",
        "object": "?country"
       }
      }
     ]
    }
   ]
  },
  {
   "estimate": {
    "row-count": 4671240
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?country",
    "?product",
    "?avgPrice"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "@type",
      "object": "bsbm-inst:ProductType1974"
     }
    },
    {
     "estimate": {
      "row-count": 20
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:product",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:price",
      "object": "?price"
     }
    },
    {
     "category": "deferred",
     "kind": "bind",
     "var": "?__agg_expr_0"
    }
   ]
  },
  {
   "category": "deferred",
   "kind": "filter"
  }
 ],
 "physical": {
  "op": "ProjectOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "SortOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "FilterOperator",
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "SubqueryOperator",
           "details": {
            "join-mode": true,
            "correlation-vars": [
             "?v3",
             "?v0"
            ]
           },
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "SubqueryOperator",
              "details": {
               "join-mode": true,
               "correlation-vars": [
                "?v3"
               ]
              },
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "SubqueryOperator",
                 "details": {
                  "join-mode": true
                 },
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "EmptyOperator",
                    "est-rows": 1
                   }
                  },
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryBody",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "ProjectOperator",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "GroupAggregateOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "NestedLoopJoinOperator",
                             "details": {
                              "right": "?v0 <3:type> <13:ProductType1974>"
                             },
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "details": {
                                 "right": "?v1 <14:reviewFor> ?v0",
                                 "hash-join-chosen": false,
                                 "hash-join-reason": "subject-driven-forward-join",
                                 "probe-count": 2848260,
                                 "driving-est": 2937533
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "HashJoinOperator",
                                   "details": {
                                    "probe": "?v1 <http://purl.org/stuff/rev#reviewer> ?v2",
                                    "hash-join-chosen": true,
                                    "hash-join-reason": "cost-wins",
                                    "probe-count": 2848260,
                                    "driving-est": 154607,
                                    "scan-ratio": "18.4"
                                   },
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "NestedLoopJoinOperator",
                                      "est-rows": 10,
                                      "details": {
                                       "right": "?v2 <14:country> ?v3"
                                      },
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "EmptyOperator",
                                         "est-rows": 1
                                        }
                                       }
                                      ]
                                     }
                                    },
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "DatasetOperator",
                                      "details": {
                                       "pattern": "?v1 <http://purl.org/stuff/rev#reviewer> ?v2"
                                      }
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               },
               {
                "rel": "child",
                "node": {
                 "op": "SubqueryBody",
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "ProjectOperator",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "GroupAggregateOperator",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "SubqueryOperator",
                          "details": {
                           "join-mode": true
                          },
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "EmptyOperator",
                             "est-rows": 1
                            }
                           },
                           {
                            "rel": "child",
                            "node": {
                             "op": "SubqueryBody",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "ProjectOperator",
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "GroupAggregateOperator",
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "NestedLoopJoinOperator",
                                      "details": {
                                       "right": "?v0 <3:type> <13:ProductType1974>"
                                      },
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "NestedLoopJoinOperator",
                                         "details": {
                                          "right": "?v1 <14:reviewFor> ?v0",
                                          "hash-join-chosen": false,
                                          "hash-join-reason": "subject-driven-forward-join",
                                          "probe-count": 2848260,
                                          "driving-est": 2937533
                                         },
                                         "children": [
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "HashJoinOperator",
                                            "details": {
                                             "probe": "?v1 <http://purl.org/stuff/rev#reviewer> ?v2",
                                             "hash-join-chosen": true,
                                             "hash-join-reason": "cost-wins",
                                             "probe-count": 2848260,
                                             "driving-est": 154607,
                                             "scan-ratio": "18.4"
                                            },
                                            "children": [
                                             {
                                              "rel": "child",
                                              "node": {
                                               "op": "NestedLoopJoinOperator",
                                               "est-rows": 10,
                                               "details": {
                                                "right": "?v2 <14:country> ?v3"
                                               },
                                               "children": [
                                                {
                                                 "rel": "child",
                                                 "node": {
                                                  "op": "EmptyOperator",
                                                  "est-rows": 1
                                                 }
                                                }
                                               ]
                                              }
                                             },
                                             {
                                              "rel": "child",
                                              "node": {
                                               "op": "DatasetOperator",
                                               "details": {
                                                "pattern": "?v1 <http://purl.org/stuff/rev#reviewer> ?v2"
                                               }
                                              }
                                             }
                                            ]
                                           }
                                          }
                                         ]
                                        }
                                       }
                                      ]
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            },
            {
             "rel": "child",
             "node": {
              "op": "SubqueryBody",
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "ProjectOperator",
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "GroupAggregateOperator",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "BindOperator",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "NestedLoopJoinOperator",
                          "details": {
                           "right": "?v6 <14:price> ?v7"
                          },
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "HashJoinOperator",
                             "details": {
                              "probe": "?v6 <14:product> ?v0",
                              "hash-join-chosen": true,
                              "hash-join-reason": "cost-wins",
                              "probe-count": 5696520,
                              "driving-est": 233562,
                              "scan-ratio": "24.4"
                             },
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "est-rows": 10,
                                "details": {
                                 "right": "?v0 <3:type> <13:ProductType1974>"
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "EmptyOperator",
                                   "est-rows": 1
                                  }
                                 }
                                ]
                               }
                              },
                              {
                               "rel": "child",
                               "node": {
                                "op": "DatasetOperator",
                                "details": {
                                 "pattern": "?v6 <14:product> ?v0"
                                }
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---

## Q6

*Reviewers whose average rating of a producer's products exceeds 1.5x the overall average.*

- **Benchmark @100M (seed 808080):** Fluree **0.88 (37 rows)** · Virtuoso 0.04 (37) · QLever 0.14 (37)
- **This-run (instantiated, single exec):** 3.85 s, 62 rows

**Physical plan (operator tree):**

- **ProjectOperator**
  - **HavingOperator**
    - **GroupAggregateOperator**
      - **UnionOperator**
        - **NestedLoopJoinOperator**  [right=?v1 <http://purl.org/stuff/rev#reviewer> ?v5, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=2848260, driving-est=460]
          - **NestedLoopJoinOperator**  [right=?v1 <14:reviewFor> ?v0, hash-join-chosen=False, hash-join-reason=scan-ratio-too-high, scan-ratio=61918.7, probe-count=2848260, driving-est=46]
            - **NestedLoopJoinOperator**  [right=?v0 <14:producer> <4206:Producer2095>]
              - **SubqueryOperator**  [join-mode=True]
                - **EmptyOperator**  [est-rows=1]
                - **SubqueryBody**
                  - **ProjectOperator**
                    - **GroupAggregateOperator**
                      - **UnionOperator**  [est-rows=200]
                        - **NestedLoopJoinOperator**  [est-rows=100, right=?v1 <14:reviewFor> ?v0, hash-join-chosen=False, hash-join-reason=scan-ratio-too-high, scan-ratio=61918.7, probe-count=2848260, driving-est=46]
                          - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <14:producer> <4206:Producer2095>]
                            - **EmptyOperator**  [est-rows=1]

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
  prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
  prefix rev: <http://purl.org/stuff/rev#>
  prefix xsd: <http://www.w3.org/2001/XMLSchema#>

  Select ?reviewer (avg(xsd:float(?score)) As ?reviewerAvgScore)
  {
    { Select (avg(xsd:float(?score)) As ?avgScore)
      {
        ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2095/Producer2095> .
        ?review bsbm:reviewFor ?product .
        { ?review bsbm:rating1 ?score . } UNION
        { ?review bsbm:rating2 ?score . } UNION
        { ?review bsbm:rating3 ?score . } UNION
        { ?review bsbm:rating4 ?score . }
      }
    }
    ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2095/Producer2095> .
    ?review bsbm:reviewFor ?product .
    ?review rev:reviewer ?reviewer .
    { ?review bsbm:rating1 ?score . } UNION
    { ?review bsbm:rating2 ?score . } UNION
    { ?review bsbm:rating3 ?score . } UNION
    { ?review bsbm:rating4 ?score . }
  }
  Group By ?reviewer
  Having (avg(xsd:float(?score)) > min(?avgScore) * 1.5)
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 1
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?avgScore"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 46
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "bsbm:producer",
      "object": "bsbm-inst:dataFromProducer2095/Producer2095"
     }
    },
    {
     "estimate": {
      "row-count": 10
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?review",
      "property": "bsbm:reviewFor",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 3988005991798994432
     },
     "category": "source",
     "kind": "union",
     "branches": [
      [
       {
        "estimate": {
         "row-count": 3988005991797
        },
        "category": "source",
        "kind": "union",
        "branches": [
         [
          {
           "estimate": {
            "row-count": 3988004
           },
           "category": "source",
           "kind": "union",
           "branches": [
            [
             {
              "estimate": {
               "row-count": 2
              },
              "category": "source",
              "kind": "triple",
              "pattern": {
               "subject": "?review",
               "property": "bsbm:rating1",
               "object": "?score"
              }
             }
            ],
            [
             {
              "estimate": {
               "row-count": 1
              },
              "category": "source",
              "kind": "triple",
              "pattern": {
               "subject": "?review",
               "property": "bsbm:rating2",
               "object": "?score"
              }
             }
            ]
           ]
          }
         ],
         [
          {
           "estimate": {
            "row-count": 2
           },
           "category": "source",
           "kind": "triple",
           "pattern": {
            "subject": "?review",
            "property": "bsbm:rating3",
            "object": "?score"
           }
          }
         ]
        ]
       }
      ],
      [
       {
        "estimate": {
         "row-count": 1
        },
        "category": "source",
        "kind": "triple",
        "pattern": {
         "subject": "?review",
         "property": "bsbm:rating4",
         "object": "?score"
        }
       }
      ]
     ]
    },
    {
     "category": "deferred",
     "kind": "bind",
     "var": "?__agg_expr_0"
    }
   ]
  },
  {
   "estimate": {
    "row-count": 46
   },
   "category": "source",
   "kind": "triple",
   "pattern": {
    "subject": "?product",
    "property": "bsbm:producer",
    "object": "bsbm-inst:dataFromProducer2095/Producer2095"
   }
  },
  {
   "estimate": {
    "row-count": 10
   },
   "category": "source",
   "kind": "triple",
   "pattern": {
    "subject": "?review",
    "property": "bsbm:reviewFor",
    "object": "?product"
   }
  },
  {
   "estimate": {
    "row-count": 2
   },
   "category": "source",
   "kind": "triple",
   "pattern": {
    "subject": "?review",
    "property": "rev:reviewer",
    "object": "?reviewer"
   }
  },
  {
   "estimate": {
    "row-count": 3988005991798994432
   },
   "category": "source",
   "kind": "union",
   "branches": [
    [
     {
      "estimate": {
       "row-count": 3988005991797
      },
      "category": "source",
      "kind": "union",
      "branches": [
       [
        {
         "estimate": {
          "row-count": 3988004
         },
         "category": "source",
         "kind": "union",
         "branches": [
          [
           {
            "estimate": {
             "row-count": 2
            },
            "category": "source",
            "kind": "triple",
            "pattern": {
             "subject": "?review",
             "property": "bsbm:rating1",
             "object": "?score"
            }
           }
          ],
          [
           {
            "estimate": {
             "row-count": 1
            },
            "category": "source",
            "kind": "triple",
            "pattern": {
             "subject": "?review",
             "property": "bsbm:rating2",
             "object": "?score"
            }
           }
          ]
         ]
        }
       ],
       [
        {
         "estimate": {
          "row-count": 2
         },
         "category": "source",
         "kind": "triple",
         "pattern": {
          "subject": "?review",
          "property": "bsbm:rating3",
          "object": "?score"
         }
        }
       ]
      ]
     },
     {
      "category": "deferred",
      "kind": "bind",
      "var": "?__agg_expr_1"
     }
    ],
    [
     {
      "estimate": {
       "row-count": 1
      },
      "category": "source",
      "kind": "triple",
      "pattern": {
       "subject": "?review",
       "property": "bsbm:rating4",
       "object": "?score"
      }
     },
     {
      "category": "deferred",
      "kind": "bind",
      "var": "?__agg_expr_1"
     }
    ]
   ]
  }
 ],
 "physical": {
  "op": "ProjectOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "HavingOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "GroupAggregateOperator",
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "UnionOperator",
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "NestedLoopJoinOperator",
              "details": {
               "right": "?v1 <http://purl.org/stuff/rev#reviewer> ?v5",
               "hash-join-chosen": false,
               "hash-join-reason": "subject-driven-forward-join",
               "probe-count": 2848260,
               "driving-est": 460
              },
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "NestedLoopJoinOperator",
                 "details": {
                  "right": "?v1 <14:reviewFor> ?v0",
                  "hash-join-chosen": false,
                  "hash-join-reason": "scan-ratio-too-high",
                  "probe-count": 2848260,
                  "driving-est": 46,
                  "scan-ratio": "61918.7"
                 },
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "NestedLoopJoinOperator",
                    "details": {
                     "right": "?v0 <14:producer> <4206:Producer2095>"
                    },
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "SubqueryOperator",
                       "details": {
                        "join-mode": true
                       },
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "EmptyOperator",
                          "est-rows": 1
                         }
                        },
                        {
                         "rel": "child",
                         "node": {
                          "op": "SubqueryBody",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "ProjectOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "GroupAggregateOperator",
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "UnionOperator",
                                   "est-rows": 200,
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "NestedLoopJoinOperator",
                                      "est-rows": 100,
                                      "details": {
                                       "right": "?v1 <14:reviewFor> ?v0",
                                       "hash-join-chosen": false,
                                       "hash-join-reason": "scan-ratio-too-high",
                                       "probe-count": 2848260,
                                       "driving-est": 46,
                                       "scan-ratio": "61918.7"
                                      },
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "NestedLoopJoinOperator",
                                         "est-rows": 10,
                                         "details": {
                                          "right": "?v0 <14:producer> <4206:Producer2095>"
                                         },
                                         "children": [
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "EmptyOperator",
                                            "est-rows": 1
                                           }
                                          }
                                         ]
                                        }
                                       }
                                      ]
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---

## Q7

*Products of a type that have NO offer from any vendor in a given country.*

- **Benchmark @100M (seed 808080):** Fluree **2.45 (83 rows)** · Virtuoso 0.09 (83) · QLever 0.16 (84)
- **This-run (instantiated, single exec):** 3.18 s, 0 rows
- ⚠️ **Unrepresentative parameters — validate before using as a planner target.** This
  param set returned **0 rows** vs the benchmark's ~83, i.e. every product of
  `ProductType1974` has at least one US-vendor offer, so `FILTER NOT EXISTS` eliminates
  everything. The plan/timing reflect an empty-result case. Re-run with a product
  type + country that actually yields rows (e.g. a rarer vendor country).

**Physical plan (operator tree):**

- **ProjectOperator**
  - **SemijoinOperator**
    - **SubqueryOperator**  [join-mode=False]
      - **EmptyOperator**  [est-rows=1]
      - **SubqueryBody**
        - **LimitOperator**
          - **ProjectOperator**
            - **SortOperator**
              - **SubqueryOperator**  [join-mode=True]
                - **EmptyOperator**  [est-rows=1]
                - **SubqueryBody**
                  - **ProjectOperator**
                    - **GroupAggregateOperator**
                      - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=24.4, probe-count=5696520, driving-est=233562]
                        - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <3:type> <13:ProductType1974>]
                          - **EmptyOperator**  [est-rows=1]
                        - **DatasetOperator**

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
  prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
  prefix xsd: <http://www.w3.org/2001/XMLSchema#>

  Select ?product
  {
    { Select ?product
      { 
        { Select ?product (count(?offer) As ?offerCount)
          { 
            ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
            ?offer bsbm:product ?product .
          }
          Group By ?product
        }
      }
      Order By desc(?offerCount)
      Limit 1000
    }
    FILTER NOT EXISTS
    {
      ?offer bsbm:product ?product .
      ?offer bsbm:vendor ?vendor .
      ?vendor bsbm:country ?country .
      FILTER(?country=<http://downlode.org/rdf/iso-3166/countries#US>)
    }
  }
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 1000
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?product"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 4671240
     },
     "category": "source",
     "kind": "subquery",
     "select": [
      "?product",
      "?offerCount"
     ],
     "patterns": [
      {
       "estimate": {
        "row-count": 233562
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?product",
        "property": "@type",
        "object": "bsbm-inst:ProductType1974"
       }
      },
      {
       "estimate": {
        "row-count": 20
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?offer",
        "property": "bsbm:product",
        "object": "?product"
       }
      }
     ]
    }
   ]
  },
  {
   "category": "deferred",
   "kind": "not-exists",
   "patterns": [
    {
     "estimate": {
      "row-count": 20
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:product",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:vendor",
      "object": "?vendor"
     }
    },
    {
     "estimate": {
      "row-count": 2
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?vendor",
      "property": "bsbm:country",
      "object": "?country"
     }
    },
    {
     "category": "deferred",
     "kind": "filter"
    }
   ]
  }
 ],
 "physical": {
  "op": "ProjectOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "SemijoinOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "SubqueryOperator",
        "details": {
         "join-mode": false
        },
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "EmptyOperator",
           "est-rows": 1
          }
         },
         {
          "rel": "child",
          "node": {
           "op": "SubqueryBody",
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "LimitOperator",
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "ProjectOperator",
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "SortOperator",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "SubqueryOperator",
                       "details": {
                        "join-mode": true
                       },
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "EmptyOperator",
                          "est-rows": 1
                         }
                        },
                        {
                         "rel": "child",
                         "node": {
                          "op": "SubqueryBody",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "ProjectOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "GroupAggregateOperator",
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "HashJoinOperator",
                                   "details": {
                                    "probe": "?v1 <14:product> ?v0",
                                    "hash-join-chosen": true,
                                    "hash-join-reason": "cost-wins",
                                    "probe-count": 5696520,
                                    "driving-est": 233562,
                                    "scan-ratio": "24.4"
                                   },
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "NestedLoopJoinOperator",
                                      "est-rows": 10,
                                      "details": {
                                       "right": "?v0 <3:type> <13:ProductType1974>"
                                      },
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "EmptyOperator",
                                         "est-rows": 1
                                        }
                                       }
                                      ]
                                     }
                                    },
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "DatasetOperator",
                                      "details": {
                                       "pattern": "?v1 <14:product> ?v0"
                                      }
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---

## Q8

*Vendors ranked by the ratio of their below-average-priced offers to their total offers.*

- **Benchmark @100M (seed 808080):** Fluree **19.63 (10 rows)** · Virtuoso 13.51 (10) · QLever 5.61 (10)
- **This-run (instantiated, single exec):** 3.97 s, 10 rows
- ⚠️ **Unrepresentative parameters — validate before using as a planner target.** This
  run (3.97 s) is ~5× faster than the benchmark average (19.63 s) because
  `ProductType1974` is a small leaf type (~38 products); the benchmark draws larger
  types. The plan shape is likely the same, but the cardinalities/cost here understate
  the real case. Re-run with a larger product type to reproduce the ~20 s behavior.

**Physical plan (operator tree):**

- **LimitOperator**
  - **ProjectOperator**
    - **SortOperator**
      - **BindOperator**
        - **BindOperator**
          - **SubqueryOperator**  [join-mode=True]
            - **SubqueryOperator**  [join-mode=True]
              - **EmptyOperator**  [est-rows=1]
              - **SubqueryBody**
                - **ProjectOperator**
                  - **GroupAggregateOperator**
                    - **NestedLoopJoinOperator**  [right=?v1 <14:vendor> ?v2, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=5696520, driving-est=4671240]
                      - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=24.4, probe-count=5696520, driving-est=233562]
                        - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <3:type> <13:ProductType1974>]
                          - **EmptyOperator**  [est-rows=1]
                        - **DatasetOperator**
            - **SubqueryBody**
              - **ProjectOperator**
                - **GroupAggregateOperator**
                  - **FilterOperator**
                    - **SubqueryOperator**  [join-mode=True]
                      - **EmptyOperator**  [est-rows=1]
                      - **SubqueryBody**
                        - **ProjectOperator**
                          - **SubqueryOperator**  [join-mode=True]
                            - **NestedLoopJoinOperator**  [right=?v1 <14:price> ?v3, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=5696520, driving-est=4671240]
                              - **NestedLoopJoinOperator**  [right=?v1 <14:vendor> ?v2, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=5696520, driving-est=4671240]
                                - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=24.4, probe-count=5696520, driving-est=233562]
                                  - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <3:type> <13:ProductType1974>]
                                    - **EmptyOperator**  [est-rows=1]
                                  - **DatasetOperator**
                            - **SubqueryBody**
                              - **ProjectOperator**
                                - **GroupAggregateOperator**
                                  - **NestedLoopJoinOperator**  [right=?v1 <14:price> ?v3]
                                    - **NestedLoopJoinOperator**  [right=?v1 <14:vendor> ?v2, hash-join-chosen=False, hash-join-reason=subject-driven-forward-join, probe-count=5696520, driving-est=4671240]
                                      - **HashJoinOperator**  [hash-join-chosen=True, hash-join-reason=cost-wins, scan-ratio=24.4, probe-count=5696520, driving-est=233562]
                                        - **NestedLoopJoinOperator**  [est-rows=10, right=?v0 <3:type> <13:ProductType1974>]
                                          - **EmptyOperator**  [est-rows=1]
                                        - **DatasetOperator**

<details><summary>Full instantiated query</summary>

```sparql
prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
  prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
  prefix xsd: <http://www.w3.org/2001/XMLSchema#>

  Select ?vendor (xsd:float(?belowAvg)/?offerCount As ?cheapExpensiveRatio)
  {
    { Select ?vendor (count(?offer) As ?belowAvg)
      {
        { ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
          ?offer bsbm:product ?product .
          ?offer bsbm:vendor ?vendor .
          ?offer bsbm:price ?price .
          { Select ?product (avg(xsd:float(xsd:string(?price))) As ?avgPrice)
            {
              ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
              ?offer bsbm:product ?product .
              ?offer bsbm:vendor ?vendor .
              ?offer bsbm:price ?price .
            }
            Group By ?product
          }
        } .
        FILTER (xsd:float(xsd:string(?price)) < ?avgPrice)
      }
      Group By ?vendor
    }
    { Select ?vendor (count(?offer) As ?offerCount)
      {
        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1974> .
        ?offer bsbm:product ?product .
        ?offer bsbm:vendor ?vendor .
      }
      Group By ?vendor
    }
  }
  Order by desc(xsd:float(?belowAvg)/?offerCount) ?vendor
  limit 10
```
</details>

<details><summary>Full explain JSON (logical + physical)</summary>

```json
{
 "logical": [
  {
   "estimate": {
    "row-count": 4671240
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?vendor",
    "?offerCount"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 233562
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?product",
      "property": "@type",
      "object": "bsbm-inst:ProductType1974"
     }
    },
    {
     "estimate": {
      "row-count": 20
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:product",
      "object": "?product"
     }
    },
    {
     "estimate": {
      "row-count": 1
     },
     "category": "source",
     "kind": "triple",
     "pattern": {
      "subject": "?offer",
      "property": "bsbm:vendor",
      "object": "?vendor"
     }
    }
   ]
  },
  {
   "estimate": {
    "row-count": 93424800000000
   },
   "category": "source",
   "kind": "subquery",
   "select": [
    "?vendor",
    "?belowAvg"
   ],
   "patterns": [
    {
     "estimate": {
      "row-count": 93424800
     },
     "category": "source",
     "kind": "subquery",
     "select": [
      "?product",
      "?offer",
      "?vendor",
      "?price",
      "?avgPrice"
     ],
     "patterns": [
      {
       "estimate": {
        "row-count": 233562
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?product",
        "property": "@type",
        "object": "bsbm-inst:ProductType1974"
       }
      },
      {
       "estimate": {
        "row-count": 20
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?offer",
        "property": "bsbm:product",
        "object": "?product"
       }
      },
      {
       "estimate": {
        "row-count": 1
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?offer",
        "property": "bsbm:vendor",
        "object": "?vendor"
       }
      },
      {
       "estimate": {
        "row-count": 1
       },
       "category": "source",
       "kind": "triple",
       "pattern": {
        "subject": "?offer",
        "property": "bsbm:price",
        "object": "?price"
       }
      },
      {
       "estimate": {
        "row-count": 4671240
       },
       "category": "source",
       "kind": "subquery",
       "select": [
        "?product",
        "?avgPrice"
       ],
       "patterns": [
        {
         "estimate": {
          "row-count": 1
         },
         "category": "source",
         "kind": "triple",
         "pattern": {
          "subject": "?product",
          "property": "@type",
          "object": "bsbm-inst:ProductType1974"
         }
        },
        {
         "estimate": {
          "row-count": 1
         },
         "category": "source",
         "kind": "triple",
         "pattern": {
          "subject": "?offer",
          "property": "bsbm:product",
          "object": "?product"
         }
        },
        {
         "estimate": {
          "row-count": 1
         },
         "category": "source",
         "kind": "triple",
         "pattern": {
          "subject": "?offer",
          "property": "bsbm:vendor",
          "object": "?vendor"
         }
        },
        {
         "estimate": {
          "row-count": 1
         },
         "category": "source",
         "kind": "triple",
         "pattern": {
          "subject": "?offer",
          "property": "bsbm:price",
          "object": "?price"
         }
        },
        {
         "category": "deferred",
         "kind": "bind",
         "var": "?__agg_expr_0"
        }
       ]
      }
     ]
    },
    {
     "category": "deferred",
     "kind": "filter"
    }
   ]
  },
  {
   "category": "deferred",
   "kind": "bind",
   "var": "?cheapExpensiveRatio"
  }
 ],
 "physical": {
  "op": "LimitOperator",
  "children": [
   {
    "rel": "child",
    "node": {
     "op": "ProjectOperator",
     "children": [
      {
       "rel": "child",
       "node": {
        "op": "SortOperator",
        "children": [
         {
          "rel": "child",
          "node": {
           "op": "BindOperator",
           "children": [
            {
             "rel": "child",
             "node": {
              "op": "BindOperator",
              "children": [
               {
                "rel": "child",
                "node": {
                 "op": "SubqueryOperator",
                 "details": {
                  "join-mode": true,
                  "correlation-vars": [
                   "?v2"
                  ]
                 },
                 "children": [
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryOperator",
                    "details": {
                     "join-mode": true
                    },
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "EmptyOperator",
                       "est-rows": 1
                      }
                     },
                     {
                      "rel": "child",
                      "node": {
                       "op": "SubqueryBody",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "ProjectOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "GroupAggregateOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "NestedLoopJoinOperator",
                                "details": {
                                 "right": "?v1 <14:vendor> ?v2",
                                 "hash-join-chosen": false,
                                 "hash-join-reason": "subject-driven-forward-join",
                                 "probe-count": 5696520,
                                 "driving-est": 4671240
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "HashJoinOperator",
                                   "details": {
                                    "probe": "?v1 <14:product> ?v0",
                                    "hash-join-chosen": true,
                                    "hash-join-reason": "cost-wins",
                                    "probe-count": 5696520,
                                    "driving-est": 233562,
                                    "scan-ratio": "24.4"
                                   },
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "NestedLoopJoinOperator",
                                      "est-rows": 10,
                                      "details": {
                                       "right": "?v0 <3:type> <13:ProductType1974>"
                                      },
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "EmptyOperator",
                                         "est-rows": 1
                                        }
                                       }
                                      ]
                                     }
                                    },
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "DatasetOperator",
                                      "details": {
                                       "pattern": "?v1 <14:product> ?v0"
                                      }
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  },
                  {
                   "rel": "child",
                   "node": {
                    "op": "SubqueryBody",
                    "children": [
                     {
                      "rel": "child",
                      "node": {
                       "op": "ProjectOperator",
                       "children": [
                        {
                         "rel": "child",
                         "node": {
                          "op": "GroupAggregateOperator",
                          "children": [
                           {
                            "rel": "child",
                            "node": {
                             "op": "FilterOperator",
                             "children": [
                              {
                               "rel": "child",
                               "node": {
                                "op": "SubqueryOperator",
                                "details": {
                                 "join-mode": true
                                },
                                "children": [
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "EmptyOperator",
                                   "est-rows": 1
                                  }
                                 },
                                 {
                                  "rel": "child",
                                  "node": {
                                   "op": "SubqueryBody",
                                   "children": [
                                    {
                                     "rel": "child",
                                     "node": {
                                      "op": "ProjectOperator",
                                      "children": [
                                       {
                                        "rel": "child",
                                        "node": {
                                         "op": "SubqueryOperator",
                                         "details": {
                                          "join-mode": true,
                                          "correlation-vars": [
                                           "?v0"
                                          ]
                                         },
                                         "children": [
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "NestedLoopJoinOperator",
                                            "details": {
                                             "right": "?v1 <14:price> ?v3",
                                             "hash-join-chosen": false,
                                             "hash-join-reason": "subject-driven-forward-join",
                                             "probe-count": 5696520,
                                             "driving-est": 4671240
                                            },
                                            "children": [
                                             {
                                              "rel": "child",
                                              "node": {
                                               "op": "NestedLoopJoinOperator",
                                               "details": {
                                                "right": "?v1 <14:vendor> ?v2",
                                                "hash-join-chosen": false,
                                                "hash-join-reason": "subject-driven-forward-join",
                                                "probe-count": 5696520,
                                                "driving-est": 4671240
                                               },
                                               "children": [
                                                {
                                                 "rel": "child",
                                                 "node": {
                                                  "op": "HashJoinOperator",
                                                  "details": {
                                                   "probe": "?v1 <14:product> ?v0",
                                                   "hash-join-chosen": true,
                                                   "hash-join-reason": "cost-wins",
                                                   "probe-count": 5696520,
                                                   "driving-est": 233562,
                                                   "scan-ratio": "24.4"
                                                  },
                                                  "children": [
                                                   {
                                                    "rel": "child",
                                                    "node": {
                                                     "op": "NestedLoopJoinOperator",
                                                     "est-rows": 10,
                                                     "details": {
                                                      "right": "?v0 <3:type> <13:ProductType1974>"
                                                     },
                                                     "children": [
                                                      {
                                                       "rel": "child",
                                                       "node": {
                                                        "op": "EmptyOperator",
                                                        "est-rows": 1
                                                       }
                                                      }
                                                     ]
                                                    }
                                                   },
                                                   {
                                                    "rel": "child",
                                                    "node": {
                                                     "op": "DatasetOperator",
                                                     "details": {
                                                      "pattern": "?v1 <14:product> ?v0"
                                                     }
                                                    }
                                                   }
                                                  ]
                                                 }
                                                }
                                               ]
                                              }
                                             }
                                            ]
                                           }
                                          },
                                          {
                                           "rel": "child",
                                           "node": {
                                            "op": "SubqueryBody",
                                            "children": [
                                             {
                                              "rel": "child",
                                              "node": {
                                               "op": "ProjectOperator",
                                               "children": [
                                                {
                                                 "rel": "child",
                                                 "node": {
                                                  "op": "GroupAggregateOperator",
                                                  "children": [
                                                   {
                                                    "rel": "child",
                                                    "node": {
                                                     "op": "NestedLoopJoinOperator",
                                                     "details": {
                                                      "right": "?v1 <14:price> ?v3"
                                                     },
                                                     "children": [
                                                      {
                                                       "rel": "child",
                                                       "node": {
                                                        "op": "NestedLoopJoinOperator",
                                                        "details": {
                                                         "right": "?v1 <14:vendor> ?v2",
                                                         "hash-join-chosen": false,
                                                         "hash-join-reason": "subject-driven-forward-join",
                                                         "probe-count": 5696520,
                                                         "driving-est": 4671240
                                                        },
                                                        "children": [
                                                         {
                                                          "rel": "child",
                                                          "node": {
                                                           "op": "HashJoinOperator",
                                                           "details": {
                                                            "probe": "?v1 <14:product> ?v0",
                                                            "hash-join-chosen": true,
                                                            "hash-join-reason": "cost-wins",
                                                            "probe-count": 5696520,
                                                            "driving-est": 233562,
                                                            "scan-ratio": "24.4"
                                                           },
                                                           "children": [
                                                            {
                                                             "rel": "child",
                                                             "node": {
                                                              "op": "NestedLoopJoinOperator",
                                                              "est-rows": 10,
                                                              "details": {
                                                               "right": "?v0 <3:type> <13:ProductType1974>"
                                                              },
                                                              "children": [
                                                               {
                                                                "rel": "child",
                                                                "node": {
                                                                 "op": "EmptyOperator",
                                                                 "est-rows": 1
                                                                }
                                                               }
                                                              ]
                                                             }
                                                            },
                                                            {
                                                             "rel": "child",
                                                             "node": {
                                                              "op": "DatasetOperator",
                                                              "details": {
                                                               "pattern": "?v1 <14:product> ?v0"
                                                              }
                                                             }
                                                            }
                                                           ]
                                                          }
                                                         }
                                                        ]
                                                       }
                                                      }
                                                     ]
                                                    }
                                                   }
                                                  ]
                                                 }
                                                }
                                               ]
                                              }
                                             }
                                            ]
                                           }
                                          }
                                         ]
                                        }
                                       }
                                      ]
                                     }
                                    }
                                   ]
                                  }
                                 }
                                ]
                               }
                              }
                             ]
                            }
                           }
                          ]
                         }
                        }
                       ]
                      }
                     }
                    ]
                   }
                  }
                 ]
                }
               }
              ]
             }
            }
           ]
          }
         }
        ]
       }
      }
     ]
    }
   }
  ]
 }
}
```
</details>

---
