# Virtual-Dataset (Iceberg / R2RML) Perf — Benchmark Corpus Design (WP5)

**Date:** 2026-07-10
**Branch:** `bench/virtual-dataset-corpus` (worktree `db-vbench`)
**Companions:** `01-pathway-inventory.md` (strategies §N), `02-hypothesis-map.md` (H1–H8, shape tags). This doc is the **spec** the WP4 manifest + `.rq` files are generated from — it does **not** contain runnable `.rq` bodies (those land with the WP4 manifest format). Precision bar = the inventory doc: every predicate is the real `edw:` spelling from the mapping, every expected-row count is derived from a stated invariant or bounded `[min,max]`.
**Model source:** `/Users/ajohnson/Downloads/bplatz-handoff/enterprise.ttl` (the ENTERPRISE_DEMO.DW R2RML mapping; line anchors below are into that file).

The framing: an end-user has bound this Snowflake warehouse as a virtual dataset and asks an LLM *"what are the highest-value BI questions this dataset answers?"*. The LLM sees the model (16 classes, their predicates, the FK graph) and proposes analytical questions (BI-01…), each answered by a small set of SPARQL queries. The corpus is that set, tagged so every performance hypothesis is exercised by ≥3 queries and every SPARQL feature by ≥2.

---

## 1. The model (grounded in `enterprise.ttl`)

**16 TriplesMaps = 8 dimensions + 8 facts**, one class each, subject `http://data.fluree.dev/edw/<kind>/{<KEY>}`. Snowflake folds identifiers to UPPERCASE, so physical columns/templates are uppercase; the `edw:` predicate spellings below are exact.

### Dimensions

| Class | Table | Key | Notable predicates (datatype) | FK edges |
|---|---|---|---|---|
| `edw:Date` `[ttl:17]` | `DIM_DATE` | `DATE_KEY` | `date`(date), `year`(int), `quarter`(int), `monthNum`(int), `monthName`(str), `weekOfYear`(int), `isWeekend`(bool) | *root* |
| `edw:Geography` `[ttl:32]` | `DIM_GEOGRAPHY` | `GEOGRAPHY_KEY` | `region`, `country`, `stateProvince`, `city`, `postalCode`(str), `latitude`/`longitude`(**double**) | *root* |
| `edw:Supplier` `[ttl:44]` | `DIM_SUPPLIER` | `SUPPLIER_KEY` | `name`, `contactEmail`, `leadTimeDays`(int), `rating`(**double**) | `geography`→Geography |
| `edw:Account` `[ttl:55]` | `DIM_ACCOUNT` | `ACCOUNT_KEY` | `name`, `industry`, `tier`, `employeeCount`(int), `annualRevenue`(**double**), `createdDate`(date) | `geography`→Geography |
| `edw:Employee` `[ttl:68]` | `DIM_EMPLOYEE` | `EMPLOYEE_KEY` | `name`(FULL_NAME), `email`, `role`, `department`, `hireDate`(date), `isActive`(bool) | `store`→Store, **`manager`→Employee (self-join)** `[ttl:80]` |
| `edw:Store` `[ttl:83]` | `DIM_STORE` | `STORE_KEY` | `name`, `channel`, `storeType`, `openDate`(date) | `geography`→Geography, `regionManager`→Employee `[ttl:93]` |
| `edw:Customer` `[ttl:96]` | `DIM_CUSTOMER` | `CUSTOMER_KEY` | `name`, `email`, `segment`, `gender`, `birthYear`(int), `signupDate`(date), **`isCurrent`(bool) + `scdValidFrom`/`scdValidTo`** `[ttl:107-109]` | `geography`→Geography, `account`→Account |
| `edw:Product` `[ttl:115]` | `DIM_PRODUCT` | `PRODUCT_KEY` | `name`, `brand`, `category`, `subcategory`, `department`, `unitCost`/`listPrice`(**double**), **`isCurrent`(bool)** `[ttl:126]` | `supplier`→Supplier |

### Facts

| Class | Table | Key | Measures (datatype) | FK edges (→ = fact-to-fact) |
|---|---|---|---|---|
| `edw:Order` `[ttl:132]` | `FACT_ORDER` | `ORDER_KEY` | `orderTotal`(**double**), `orderStatus`, `orderChannel`, `currency`, `orderDate`(date) | `customer`, `account`, `store`, `salesRep`→Employee, `dateDim`→Date |
| `edw:OrderLine` `[ttl:152]` | `FACT_ORDER_LINE` | `ORDER_LINE_KEY` | `quantity`(int), `unitPrice`/`extendedAmount`/`discountPct`(**double**), `lineNumber`(int) | **`order`→Order** `[ttl:161]`, `product`→Product |
| `edw:InventorySnapshot` `[ttl:166]` | `FACT_INVENTORY_SNAPSHOT` | `INVENTORY_KEY` | `onHandQty`/`reservedQty`/`reorderPoint`/`unitsOnOrder`(int), `snapshotDate`(date) | `product`, `store`, `dateDim`→Date |
| `edw:Shipment` `[ttl:181]` | `FACT_SHIPMENT` | `SHIPMENT_KEY` | `shipCost`(**double**), `carrier`, `shipMethod`, `shipStatus`, `shipDate`(date) | **`order`→Order** `[ttl:191]`, `destGeography`→Geography, `dateDim`→Date |
| `edw:Payment` `[ttl:198]` | `FACT_PAYMENT` | `PAYMENT_KEY` | `amount`(**double**), `tenderType`, `paymentStatus`, `paymentDate`(date) | **`order`→Order** `[ttl:207]`, `customer`, `dateDim`→Date |
| `edw:GlJournalEntry` `[ttl:214]` | `FACT_GL_JOURNAL` | `JOURNAL_KEY` | **`debitAmount`/`creditAmount`(xsd:decimal)** `[ttl:222-223]`, `glAccountCode`(int), `glAccountName`, `costCenter`, `postingDate`(date) | `dateDim`→Date |
| `edw:WebEvent` `[ttl:229]` | `FACT_WEB_EVENT` | `EVENT_KEY` | `eventType`, `deviceType`, `browser`, `referrer`, `pageUrl`, `sessionId`(int), `eventDate`(date), `eventTs`(**dateTime**) | `customer`, `product`, `dateDim`→Date |
| `edw:SupportTicket` `[ttl:248]` | `FACT_SUPPORT_TICKET` | `TICKET_KEY` | `csatScore`(int), `resolutionHours`(**double**), `status`, `priority`, `category`, `openDate`/`closeDate`(date) | `customer`, `product`, `agent`→Employee, `dateDim`→Date |

### Four structural facts that shape the corpus

1. **`GlJournalEntry.debitAmount`/`creditAmount` are the only `xsd:decimal` columns** in the whole model `[ttl:222-223]`. Every other money measure (`orderTotal`, `amount`, `extendedAmount`, `annualRevenue`, `shipCost`, `unitCost`, `listPrice`) is `xsd:double`. Both decimal and double are pruning-blind (inventory §6), so **H4's cleanest exercise is a decimal FILTER on GL** with an integer/date FILTER as the pruning-positive control.
2. **SCD-2 on `Customer` and `Product`** (`edw:isCurrent`). A customer/product question that omits `isCurrent = true` silently counts historical versions (see §5 invariants). Every customer/product query states its `isCurrent` stance explicitly.
3. **Three fact→fact FKs** — `OrderLine`/`Shipment`/`Payment` all reference `Order` `[ttl:161,191,207]`. These are the H3 stress shape: the join's **parent** is a 180K-row fact, violating "parents assumed small" (inventory §8/§9).
4. **Two dimension self-joins** — `Employee.manager`→Employee `[ttl:80]`, `Store.regionManager`→Employee `[ttl:93]` — give H3 a dims-only correlated-join control and a natural property-path (H8) target.

---

## 2. Feature tags (closed enum) and hypothesis linkage

Feature tags (from the assignment; every query carries ≥1): `bgp_star`, `join`, `fk_chain`, `filter_range`, `filter_string`, `filter_date`, `filter_iri`, `optional`, `union`, `aggregate`, `count`, `group_by`, `having`, `order_by`, `distinct`, `subquery`, `values`, `negation`, `property_path`, `construct`. (`filter_iri` was added this revision — an IRI / `=` / `IN` equality on a term-typed value, distinct from lexical `filter_string` and numeric/date `filter_range`; it labels the FILTER arm of the VALUES-vs-FILTER A/B, Q40/Q41.)

Hypotheses (see `02-hypothesis-map.md`): **H1** fact decode wall · **H2** budget-absorb modifiers · **H3** correlated-join rebuild · **H4** decimal/double pruning blindness · **H5** no COUNT manifest shortcut · **H6** aggregate+join misses fused path · **H7** cold/warm structure · **H8** non-lowered forms (VALUES/subquery/path).

`dims-only` = touches only dimension tables (each 1 Iceberg file → no decode wall; serves as an H1/H2/H3 *control*). `fact-touching` = scans ≥1 fact table.

---

## 3. BI questions and their queries

Query IDs are flat (`Q01`…) with a parent BI. Each entry gives: SPARQL sketch (WHERE shape, not a runnable body), feature tags, tables, dims-only|fact-touching, hypotheses exercised, expected rows (SF=0.1), order-sensitivity, float/decimal-projection flag.

Notation: all queries run inside `GRAPH <edw-source> { … }`; `a` = `rdf:type`; subjects use the `edw/<kind>/{KEY}` templates above.

### BI-01 — Store directory *(single-dim lookup; cardinality: small)*
Business: list stores with channel and type.
- **Q01** `?s a edw:Store ; edw:name ?n ; edw:channel ?ch ; edw:storeType ?t` — tags: `bgp_star`. Tables: Store. **dims-only**. H: — (dim-scan baseline; the "fast path works" reference). Rows: **exactly 500**. order: none. float: no.

### BI-02 — Store detail *(point lookup; positive control for prefix-prune)*
Business: show everything about one store.
- **Q02** `<edw/store/42> ?p ?o` — tags: `bgp_star` (bound-subject wildcard). Tables: Store (via prefix-prune, inventory §7). **dims-only**. H: H2/H3 **positive control** (should stay ~1 table, sub-second). Rows: **~7-8** (one per Store predicate — `storeId`/`name`/`channel`/`storeType`/`openDate`/`geography`/`regionManager`; +`rdf:type` if the wildcard emits the class). order: none. float: `latitude`/`longitude` absent on Store — no.

### BI-03 — Geographic coverage *(single-dim; distinct + string filter)*
Business: where do we operate?
- **Q03** `SELECT DISTINCT ?region WHERE { ?g a edw:Geography ; edw:region ?region }` — tags: `distinct`, `bgp_star`. Tables: Geography. **dims-only**. H: — (dims-only DISTINCT; a "DISTINCT drains its input" illustration, but with **no LIMIT** it is not an H2 budget case — it is here for `distinct` feature coverage). Rows: **[4,8]** (distinct regions). order: none (sort by value). float: no.
- **Q04** `?g a edw:Geography ; edw:region "EMEA" ; edw:city ?city` — tags: `filter_string`, `bgp_star`. Tables: Geography. **dims-only**. H: — (string equality via class scan). Rows: **[~500, ~6000]** (cities in one region; bound by 25K geography rows). order: none. float: no.

### BI-04 — Supplier scorecard *(dim⋈dim join; H3 + H2 dims-only controls)*
Business: suppliers and their region, best-rated first.
- **Q05** `?sup a edw:Supplier ; edw:name ?n ; edw:rating ?r ; edw:geography ?g . ?g edw:region ?region ORDER BY DESC(?r) LIMIT 20` — tags: `join`, `fk_chain`, `order_by`, `bgp_star`. Tables: Supplier⋈Geography. **dims-only**. H: **H3 dims-only control** (dim⋈dim correlated join), **H2 dims-only ORDER BY+LIMIT control**. Rows: **exactly 20**. order: by_keys (ORDER BY rating, ties on ?sup). float: **yes** (`rating`).

### BI-05 — Account book *(single-dim; range filter + single-table rollup)*
Business: enterprise accounts by revenue; account mix by industry.
- **Q06** `?a a edw:Account ; edw:tier "Enterprise" ; edw:annualRevenue ?rev . FILTER(?rev > 10000000)` — tags: `filter_string`, `filter_range`, `bgp_star`. Tables: Account. **dims-only**. H: **H4 double control** (double FILTER → operator-only, no pushdown; dims-only so no decode wall). Rows: **[0, 15000]** (tier+revenue selective). order: none. float: **yes** (`annualRevenue`).
- **Q07** `SELECT ?ind (COUNT(?a) AS ?c) WHERE { ?a a edw:Account ; edw:industry ?ind } GROUP BY ?ind` — tags: `aggregate`, `count`, `group_by`, `bgp_star`. Tables: Account. **dims-only**. H: **H6 fused-agg positive control** (single-table GROUP BY → fused path eligible, inventory §11). Rows: **[6,20]** (industries). order: none (by keys). float: no.

### BI-06 — Revenue by region *(fact⋈dim rollup; the canonical H6)*
Business: total order revenue by customer region.
- **Q08** `SELECT ?region (SUM(?tot) AS ?rev) WHERE { ?o a edw:Order ; edw:orderTotal ?tot ; edw:customer ?c . ?c edw:geography ?g . ?g edw:region ?region } GROUP BY ?region` — tags: `aggregate`, `group_by`, `join`, `fk_chain`. Tables: Order⋈Customer⋈Geography. **fact-touching**. H: **H6** (agg over a join → fused path declines), **H3** (fact⋈dim), **H1** (180K order scan). Rows: **[4,8]** (regions). order: none (by keys). float: **yes** (SUM of double). *SCD note:* customer join is on `CUSTOMER_KEY`; each order references exactly one customer version, so revenue is unaffected by SCD, but the region label comes from that version — acceptable.
- **Q09** add `HAVING (SUM(?tot) > 5000000)` to Q08 — tags: +`having`. Same tables/H. Rows: **[0,8]**. order: none. float: yes.

### BI-07 — Revenue time-series *(fact⋈date; fiscal fields; H2 grouped ORDER BY)*
Business: order revenue by fiscal year and quarter.
- **Q10** `SELECT ?year ?q (SUM(?tot) AS ?rev) WHERE { ?o a edw:Order ; edw:orderTotal ?tot ; edw:dateDim ?d . ?d edw:year ?year ; edw:quarter ?q } GROUP BY ?year ?q ORDER BY ?year ?q` — tags: `aggregate`, `group_by`, `order_by`, `join`, `fk_chain`. Tables: Order⋈Date. **fact-touching**. H: **H6**, **H3** (fact⋈date), **H1**. (No `LIMIT`, so *not* an H2 case — the ORDER BY is on the small grouped output; the 180K scan is unavoidable regardless.) Rows: **[~12, ~84]** (distinct order-years × 4 quarters; DIM_DATE spans ~21 yrs / 7,670 days, so ≤ 84). order: by_keys. float: **yes**.
- **Q11** `?o a edw:Order ; edw:orderDate ?od . FILTER(?od >= "2024-01-01"^^xsd:date && ?od < "2024-04-01"^^xsd:date)` — tags: `filter_date`, `bgp_star`. Tables: Order. **fact-touching**. H: **H1**, **H4 date-positive-control** (date FILTER on a physically-date column **prunes**, inventory §6/§7; the ttl keeps `*_DATE` literals precisely for partition pruning `[ttl:5-6]`). Rows: **[~5000, ~15000]** (one quarter of 180K). order: none. float: no.

### BI-08 — Top products by units *(multi-fact→dim; top-k; SCD; subquery)*
Business: best-selling products.
- **Q12** `SELECT ?pn (SUM(?qty) AS ?u) WHERE { ?ol a edw:OrderLine ; edw:quantity ?qty ; edw:product ?p . ?p edw:name ?pn ; edw:isCurrent true } GROUP BY ?pn ORDER BY DESC(?u) LIMIT 10` — tags: `aggregate`, `group_by`, `order_by`, `join`, `fk_chain`. Tables: OrderLine⋈Product. **fact-touching**. H: **H6**, **H3** (fact⋈dim), **H1** (600K order-line scan), **H2** (top-k over fact). SCD: `isCurrent true`. Rows: **exactly 10**. order: by_keys (units desc, tie ?pn). float: **no** (`quantity` is `xsd:integer` ⇒ integer SUM — exact, no canonicalization).
- **Q13** `SELECT ?pn ?u WHERE { { <Q12 inner GROUP BY> } FILTER(?u > <subquery: AVG units>) }` — tags: `subquery`, `aggregate`, `group_by`, `join`, `fk_chain`. Tables: OrderLine⋈Product. **fact-touching**. H: **H6**, **H8** (subquery not lowered — evaluated generically), **H1**. Rows: **[0, 37500]**. order: none. float: no.

### BI-09 — Channel mix *(single-table fact rollup; fused-agg positive)*
Business: order volume and revenue by channel.
- **Q14** `SELECT ?ch (COUNT(?o) AS ?n) (SUM(?tot) AS ?rev) WHERE { ?o a edw:Order ; edw:orderChannel ?ch ; edw:orderTotal ?tot } GROUP BY ?ch` — tags: `aggregate`, `count`, `group_by`, `bgp_star`. Tables: Order. **fact-touching**. H: **H6 negative control** (single-table GROUP BY → fused path *taken*; contrast with Q08's joined GROUP BY that declines), **H1** (still scans 180K). Rows: **[3,6]** (channels). order: none. float: **yes** (SUM double).

### BI-10 — Fulfillment SLA *(multi-fact chain order→shipment; H3 fact⋈fact stress; optional; negation)*
Business: shipment status per order; unshipped orders.
- **Q15** `?sh a edw:Shipment ; edw:shipStatus ?st ; edw:shipCost ?sc ; edw:order ?o . ?o edw:orderId ?oid ; edw:orderStatus ?ost` — tags: `join`, `fk_chain`, `bgp_star`. Tables: Shipment⋈Order. **fact-touching**. H: **H3 fact⋈fact stress** (parent Order is a 180K fact — violates small-parent assumption, inventory §8/§9), **H1** (180K shipment + 180K order). Rows: **[~150000, 180000]** (shipments with a matched order). order: none. float: **yes** (`shipCost`).
- **Q16** `?o a edw:Order ; edw:orderId ?oid OPTIONAL { ?sh edw:order ?o ; edw:shipStatus ?st }` — tags: `optional`, `join`, `fk_chain`. Tables: Order⋉Shipment. **fact-touching**. H: **H1**, **H3**. Rows: **≥180000** (all orders, shipment cols nullable). order: none. float: no.
- **Q17** `?o a edw:Order ; edw:orderId ?oid FILTER NOT EXISTS { ?sh edw:order ?o }` — tags: `negation`, `join`. Tables: Order, Shipment. **fact-touching**. H: **H1**, **H3**. Rows: **[0, 30000]** (orders never shipped). order: none. float: no.

### BI-11 — Payment reconciliation *(multi-fact order→payment; double money filter)*
Business: large payments by tender type.
- **Q18** `SELECT ?tender (COUNT(?p) AS ?n) (SUM(?amt) AS ?tot) WHERE { ?p a edw:Payment ; edw:tenderType ?tender ; edw:amount ?amt . FILTER(?amt > 5000) } GROUP BY ?tender` — tags: `aggregate`, `count`, `group_by`, `filter_range`, `bgp_star`. Tables: Payment. **fact-touching**. H: **H4 double** (money FILTER on double → no pushdown; full 200K scan), **H1**, **H6** (GROUP BY + FILTER single-table → fused-agg eligible per §11's cost guard, since FILTER-with-GROUP-BY is allowed). Rows: **[3,6]** (tenders). order: none. float: **yes** (SUM double).

### BI-12 — GL journal money filter *(H4 primary — the only decimal columns; ±pruning controls)*
Business: large journal postings; audit windows.
- **Q19** `?j a edw:GlJournalEntry ; edw:glAccountName ?acct ; edw:debitAmount ?deb ; edw:postingDate ?pd . FILTER(?deb > 1000000)` — tags: `filter_range`, `bgp_star`. Tables: GlJournalEntry. **fact-touching**. H: **H4 PRIMARY** (decimal FILTER → `prunable_stats` returns None `[pruning.rs:279-281]`, `files_pruned=0`), **H1** (250K scan). Rows: **[0, 250000]**. order: none. float: **yes (decimal projection — canonicalize `debitAmount`)**.
- **Q20** `?j a edw:GlJournalEntry ; edw:debitAmount ?deb ; edw:postingDate ?pd . FILTER(?pd >= "2024-01-01"^^xsd:date && ?pd < "2024-04-01"^^xsd:date)` — tags: `filter_date`, `bgp_star`. Tables: GlJournalEntry. **fact-touching**. H: **H4 date-positive-control** (date **prunes** — direct A/B against Q19 on the same table), **H1**. Rows: **[~15000, ~65000]**. order: none. float: yes (decimal projection).
- **Q21** `?j a edw:GlJournalEntry ; edw:glAccountCode ?code ; edw:debitAmount ?deb . FILTER(?code >= 40000 && ?code < 50000)` — tags: `filter_range`, `bgp_star`. Tables: GlJournalEntry. **fact-touching**. H: **H4 int-positive-control** (integer column **prunes**, inventory §6), **H1**. Rows: **[0, 250000]**. order: none. float: yes (decimal projection).

### BI-13 — Customer segmentation *(SCD-2 is_current; the correctness trap)*
Business: current customers by segment; the mistake if you forget SCD.
- **Q22** `SELECT ?seg (COUNT(?c) AS ?n) WHERE { ?c a edw:Customer ; edw:isCurrent true ; edw:segment ?seg } GROUP BY ?seg` — tags: `aggregate`, `count`, `group_by`, `bgp_star`. Tables: Customer. **dims-only**. H: **H6 fused-agg positive** (single-dim GROUP BY → fused; bool FILTER `isCurrent true` is a scan-local filter, inventory §13). (No LIMIT ⇒ not H2.) Rows: **[4,8]** (segments); **COUNT sums to 300000** (current). order: none. float: no.
- **Q23** same as Q22 **without** `isCurrent true` — tags: same. **dims-only**. H: **correctness-nuance pair** (documents the SCD trap). Rows: segments same, **COUNT sums to 390000** (300K current + 90K history). order: none. float: no. *This pair is a correctness oracle, not a perf pair: the counts MUST differ by the 90K SCD history rows.*
- **Q24** `?c a edw:Customer ; edw:isCurrent true ; edw:birthYear ?by . FILTER(?by >= 1980 && ?by <= 1989)` — tags: `filter_range`, `bgp_star`. Tables: Customer. **dims-only**. H: **H4 int control** (integer prunes — but dims-only, 1 file). Rows: **[~30000, ~90000]** (1980s cohort of 300K current). order: none. float: no.

### BI-14 — Support quality *(behavioral; HAVING; single-table vs joined agg)*
Business: problem product categories; SLA by priority.
- **Q25** `SELECT ?cat (AVG(?csat) AS ?avg) (COUNT(?t) AS ?n) WHERE { ?t a edw:SupportTicket ; edw:csatScore ?csat ; edw:product ?p . ?p edw:category ?cat } GROUP BY ?cat HAVING (AVG(?csat) < 3)` — tags: `aggregate`, `group_by`, `having`, `join`, `fk_chain`. Tables: SupportTicket⋈Product. **fact-touching**. H: **H6** (agg+join declines fused), **H3**, **H1** (40K ticket scan). Rows: **[0, ~20]** (categories below 3). order: none. float: **yes** (AVG). *SCD: joins Product on key; category is version-stable — no isCurrent needed.*
- **Q26** `SELECT ?prio (AVG(?rh) AS ?avghrs) WHERE { ?t a edw:SupportTicket ; edw:priority ?prio ; edw:resolutionHours ?rh } GROUP BY ?prio ORDER BY DESC(?avghrs)` — tags: `aggregate`, `group_by`, `order_by`, `bgp_star`. Tables: SupportTicket. **fact-touching**. H: **H6 negative control** (single-table → fused; contrast Q25), **H1**. (No LIMIT ⇒ not H2; ORDER BY is on the small grouped output.) Rows: **[3,5]** (priorities). order: by_keys. float: **yes** (AVG).

### BI-15 — Web behavior *(largest fact, 1M rows; funnel; union; date)*
Business: engagement by type/device; purchase funnel.
- **Q27** `SELECT ?et ?dev (COUNT(?e) AS ?n) WHERE { ?e a edw:WebEvent ; edw:eventType ?et ; edw:deviceType ?dev } GROUP BY ?et ?dev` — tags: `aggregate`, `count`, `group_by`, `bgp_star`. Tables: WebEvent. **fact-touching**. H: **H1 heaviest** (1M-row single-table scan), **H6 fused positive**. Rows: **[~15, ~40]** (types×devices). order: none. float: no.
- **Q28** `?e a edw:WebEvent ; edw:eventType "purchase" ; edw:product ?p . ?p edw:name ?pn` — tags: `filter_string`, `join`, `fk_chain`. Tables: WebEvent⋈Product. **fact-touching**. H: **H1** (1M scan), **H3**. Rows: **[~50000, ~200000]** (purchase events). order: none. float: no.
- **Q29** `{ ?e a edw:WebEvent ; edw:eventType "purchase" } UNION { ?e a edw:WebEvent ; edw:eventType "add_to_cart" } LIMIT 100` — tags: `union`, `bgp_star`. Tables: WebEvent. **fact-touching**. H: **H1** (union of two full scans, inventory §13 routes each branch), **H2** (UnionOperator absorbs the LIMIT — both branch scans still run full; §12). Rows: **exactly 100** (LIMIT; [~100000, ~400000] match). order: none. float: no.
- **Q30** `?e a edw:WebEvent ; edw:eventDate ?ed . FILTER(?ed >= "2024-06-01"^^xsd:date && ?ed < "2024-07-01"^^xsd:date)` — tags: `filter_date`, `bgp_star`. Tables: WebEvent. **fact-touching**. H: **H1**, **H4 date control** (prunes on 1M-row table — the highest-value pruning demo). Rows: **[~80000, ~90000]** (one month of 1M). order: none. float: no.

### BI-16 — Inventory positions *(stockout risk; var-vs-var filter; store rollup)*
Business: products below reorder point; on-hand by store.
- **Q31** `?inv a edw:InventorySnapshot ; edw:onHandQty ?oh ; edw:reorderPoint ?rp ; edw:product ?p . ?p edw:name ?pn . FILTER(?oh < ?rp)` — tags: `filter_range`, `join`, `fk_chain`. Tables: InventorySnapshot⋈Product. **fact-touching**. H: **H1** (300K scan), **H3**. Note: `?oh < ?rp` is **var-vs-var** → never pushable (operator-only; not a constant-bound prune). Rows: **[0, 300000]**. order: none. float: no.
- **Q32** `SELECT ?sn (SUM(?oh) AS ?tot) WHERE { ?inv a edw:InventorySnapshot ; edw:onHandQty ?oh ; edw:store ?st . ?st edw:name ?sn } GROUP BY ?sn` — tags: `aggregate`, `group_by`, `join`, `fk_chain`. Tables: InventorySnapshot⋈Store. **fact-touching**. H: **H6**, **H3**, **H1**. Rows: **exactly 500** (stores). order: none. float: no (int SUM).

### BI-17 — Org hierarchy *(dim self-join; property paths → H8)*
Business: reporting lines.
- **Q33** `?e a edw:Employee ; edw:name ?en ; edw:manager ?m . ?m edw:name ?mn` — tags: `join`, `fk_chain`, `bgp_star`. Tables: Employee⋈Employee (self). **dims-only**. H: **H3 dims-only self-join control**. Rows: **[~4900, 5000]** (employees with a manager). order: none. float: no.
- **Q34** `?e a edw:Employee ; edw:name ?en . ?e edw:manager+ ?boss` — tags: `property_path`, `bgp_star`. Tables: Employee. **dims-only**. H: **H8** (property paths are NOT lowered to R2RML, inventory §13 — evaluated generically over an R2RML source with no ledger index). **Expected behavior is a known limitation:** likely empty/degenerate or error; document the observed outcome as a boundary result, not a perf number. order: n/a. float: no.
- **Q35** `?e a edw:Employee ; edw:manager/edw:manager ?grandboss` — tags: `property_path`, `join`. Tables: Employee. **dims-only**. H: **H8** (sequence path; same non-lowering caveat as Q34). Rows: boundary. order: n/a. float: no.

### BI-18 — Row counts per class *(H5 — the "does this dataset have data" probe)*
Business: how big is each table?
- **Q36** `SELECT (COUNT(*) AS ?c) WHERE { ?s a edw:Order }` — tags: `count`, `aggregate`. Tables: Order. **fact-touching**. H: **H5** (no manifest shortcut → full 180K scan for a count that is free in metadata, inventory §11). Rows: **exactly 1** (value **180000**). order: n/a. float: no.
- **Q37** `SELECT (COUNT(*) AS ?c) WHERE { ?s a edw:WebEvent }` — tags: `count`, `aggregate`. Tables: WebEvent. **fact-touching**. H: **H5** (worst case — 1M scan for value **1000000**). Rows: 1. order: n/a. float: no.
- **Q38** `SELECT (COUNT(*) AS ?c) WHERE { ?s a edw:Customer ; edw:isCurrent true }` — tags: `count`, `aggregate`, `bgp_star`. Tables: Customer. **dims-only**. H: **H5 dims-only control** (value **300000**; the SCD-correct count). Rows: 1. order: n/a. float: no.
- **Q39** `SELECT (COUNT(*) AS ?c) WHERE { ?s a edw:GlJournalEntry }` — tags: `count`, `aggregate`. Tables: GlJournalEntry. **fact-touching**. H: **H5** (value **250000**). Rows: 1. order: n/a. float: no.

### BI-19 — Constrained-subject equivalence *(H8: VALUES vs FILTER vs bound-subject)*
Business: revenue for three specific stores — expressed three ways.
- **Q40** `SELECT ?tot WHERE { VALUES ?store { <edw/store/1> <edw/store/2> <edw/store/3> } ?o a edw:Order ; edw:store ?store ; edw:orderTotal ?tot }` — tags: `values`, `join`, `fk_chain`. Tables: Order⋈Store. **fact-touching**. H: **H8** (VALUES not lowered → the store constraint does NOT become a scan filter; full 180K order scan), **H1**. Rows: **[0, ~1000]**. order: none. float: **yes**.
- **Q41** `?o a edw:Order ; edw:store ?store ; edw:orderTotal ?tot FILTER(?store = <edw/store/1> || ?store = <edw/store/2> || ?store = <edw/store/3>)` — tags: `filter_iri`, `join`, `fk_chain`. Tables: Order⋈Store. **fact-touching**. H: **H8 contrast** (FILTER form — same full scan; the A/B twin of Q40). Rows: same as Q40. order: none. float: yes.
- **Q42** `{ <edw/store/1> ?p1 ?o1 } UNION { <edw/store/2> ?p2 ?o2 } UNION { <edw/store/3> ?p3 ?o3 }` — tags: `union`, `values`(-semantics), `bgp_star`. Tables: Store (prefix-prune per bound subject). **dims-only**. H: **H8 positive control** (bound subjects → prefix-prune, inventory §7 — the *fast* way to pin specific entities). Rows: **~21-24** (3 × ~7-8 Store predicates). order: none. float: no.

### BI-20 — Error boundary *(unconvertible bound objects → whole-GRAPH error)*
Business: (adversarial) a natural-language filter an LLM might emit that the R2RML router rejects.
- **Q43 (EXPECTED ERROR)** `?c a edw:Customer ; edw:name "Aziz"@fr` — tags: `bgp_star`, `filter_string`. Tables: Customer. H: **boundary** — a language-tagged object is unconvertible (`is_loose_matchable_datatype` false, `rewrite.rs:476-482`), so `convert_triple_to_r2rml` returns None `[rewrite.rs:942-962]` and `unconverted_count > 0` **errors the whole GRAPH scope** `[graph.rs:245-253]`. Expected result: **InvalidQuery error**, not rows. order: n/a.
- **Q44 (EXPECTED ERROR)** `?j a edw:GlJournalEntry ; edw:debitAmount "1000"^^<http://custom/money>` — tags: `bgp_star`. Tables: GlJournalEntry. H: **boundary** — a custom (non-XSD) datatype object is unconvertible (same path). Expected: **InvalidQuery error**. order: n/a.

### BI-21 — LIMIT early-termination A/B *(H2 mechanism isolation)*
Business: "show me 10 orders" vs "show me the 10 biggest orders" — same LIMIT, opposite cost.
- **Q45** `?o a edw:Order ; edw:orderId ?oid ; edw:orderTotal ?tot LIMIT 10` — tags: `bgp_star`. Tables: Order. **fact-touching**. H: **H2 positive control** (pure row-preserving chain → the budget reaches the scan and it **early-terminates** after ~one batch, inventory §5). Rows: **exactly 10**. order: none. float: yes.
- **Q46** `?o a edw:Order ; edw:orderId ?oid ; edw:orderTotal ?tot ORDER BY DESC(?tot) LIMIT 10` — tags: `order_by`, `bgp_star`. Tables: Order. **fact-touching**. H: **H2 primary** (top-k heap; scan still **fully drained**, inventory §12). Rows: exactly 10. order: by_keys. float: yes. *Q45/Q46 are the same query ±ORDER BY — the load-bearing H2 A/B pair; wall(Q46) ≫ wall(Q45) is the H2 signature.*
- **Q47** `SELECT DISTINCT ?orderChannel WHERE { ?o a edw:Order ; edw:orderChannel ?orderChannel } LIMIT 5` — tags: `distinct`, `bgp_star`. Tables: Order. **fact-touching**. H: **H2 distinct case** (DISTINCT drains the full scan though only 5 distinct values exist — the "could early-terminate but doesn't" case, §12). Rows: **[3,5]**. order: none. float: no.

### BI-22 — Denormalized views *(CONSTRUCT; graph output)*
Business: export a flattened order/customer graph.
- **Q48** `CONSTRUCT { ?o edw:orderTotal ?tot ; edw:custRegion ?region } WHERE { ?o a edw:Order ; edw:orderTotal ?tot ; edw:customer ?c . ?c edw:geography ?g . ?g edw:region ?region }` — tags: `construct`, `join`, `fk_chain`. Tables: Order⋈Customer⋈Geography. **fact-touching**. H: **H1**, **H3** (CONSTRUCT's WHERE is R2RML-rewritten normally; the template builds triples). Rows: **~1 triple pair per order** (~360000 triples). order: none (graph). float: **yes**.
- **Q49** `CONSTRUCT { ?c edw:inRegion ?region } WHERE { ?c a edw:Customer ; edw:isCurrent true ; edw:geography ?g . ?g edw:region ?region }` — tags: `construct`, `join`, `fk_chain`. Tables: Customer⋈Geography. **dims-only**. H: **H3 dims-only**. Rows: **300000** triples (current customers). order: none. float: no.

### BI-23 — Optional attributes *(left-join semantics)*
Business: products with an optional supplier rating.
- **Q50** `?p a edw:Product ; edw:isCurrent true ; edw:name ?pn OPTIONAL { ?p edw:supplier ?s . ?s edw:rating ?r }` — tags: `optional`, `join`, `fk_chain`, `bgp_star`. Tables: Product⋉Supplier. **dims-only**. H: **H3 dims-only optional**. Rows: **[~30000, 37500]** (current products). order: none. float: **yes** (`rating`, nullable).

### BI-24 — Above-average performers *(subquery #2)*
Business: stores whose order count exceeds the store average.
- **Q51** `?st a edw:Store ; edw:name ?sn { SELECT (AVG(?cnt) AS ?avgcnt) WHERE { SELECT ?s2 (COUNT(?o) AS ?cnt) WHERE { ?o a edw:Order ; edw:store ?s2 } GROUP BY ?s2 } } …` — tags: `subquery`, `aggregate`, `group_by`, `join`. Tables: Order⋈Store. **fact-touching**. H: **H8** (nested subquery not lowered), **H6**, **H1**. Rows: **[0, 500]**. order: none. float: **yes** (AVG). *Complex; the subquery is the point — confirms non-lowered subquery scans the fact fully.*

### BI-25 — Behavioral cohort *(VALUES #2; customers with no orders)*
Business: web events for a set of watch-list products; dormant customers.
- **Q52** `SELECT ?et WHERE { VALUES ?p { <edw/product/10> <edw/product/20> } ?e a edw:WebEvent ; edw:product ?p ; edw:eventType ?et }` — tags: `values`, `join`, `fk_chain`. Tables: WebEvent⋈Product. **fact-touching**. H: **H8** (VALUES over a 1M-row fact → full scan), **H1**. Rows: **[0, ~2000]**. order: none. float: no.
- **Q53** `?c a edw:Customer ; edw:isCurrent true ; edw:customerId ?cid FILTER NOT EXISTS { ?o a edw:Order ; edw:customer ?c }` — tags: `negation`, `join`, `bgp_star`. Tables: Customer, Order. **fact-touching**. H: **H1** (order scan under NOT EXISTS), **H3**. Rows: **[0, 300000]** (customers with no order). order: none. float: no. SCD: `isCurrent true`.

### BI-26 — IRI-equality control *(filter_iri #2; object-IRI equality gets no prune)*
Business: employees assigned to a specific store — the object-IRI-equality form (the dims-only twin of Q41's fact-side `filter_iri`).
- **Q54** `?e a edw:Employee ; edw:name ?en ; edw:store ?st FILTER(?st = <edw/store/1>)` — tags: `filter_iri`, `bgp_star`. Tables: Employee (the store IRI is the materialized FK object — no join to DIM_STORE). **dims-only**. H: — (dims-only control; object-IRI equality is **operator-only** — unlike a bound *subject* it does not prefix-prune, inventory §6/§7 — so a full Employee scan runs. This is the `filter_iri` ≥2 control and the dims-only twin of Q41). Rows: **[~5, ~60]** (employees at one store; 5,000 employees / 500 stores ≈ 10 avg). order: none. float: no.

**Total: 54 queries across 26 BI questions** (within the 40–80 target).

---

## 4. Coverage matrices

### 4.1 Hypothesis × queries (each H ≥ 3; dims-only control noted)

| H | Queries | Count | Dims-only control present? |
|---|---|---|---|
| **H1** fact decode wall | Q08,Q10,Q11,Q12,Q13,Q14,Q15,Q16,Q17,Q18,Q19,Q20,Q21,Q25,Q26,Q27,Q28,Q29,Q30,Q31,Q32,Q36,Q37,Q39,Q40,Q41,Q45,Q46,Q47,Q48,Q51,Q52,Q53 | 33 | n/a (H1 is fact-only; dim scans Q01/Q03 are the "no decode wall" baseline) |
| **H2** budget-absorb *(every member has a LIMIT — the budget-absorption is the mechanism)* | Q05(dim ORDER BY+LIMIT), Q12(fact GROUP BY+ORDER BY+LIMIT), Q29(fact UNION+LIMIT), Q45(fact pure-LIMIT **control**), Q46(fact ORDER BY+LIMIT **primary**), Q47(fact DISTINCT+LIMIT) | 6 | **yes** — Q05 dims-only ORDER BY+LIMIT control; Q46 fact ORDER BY+LIMIT; Q45 pure-LIMIT early-terminate control. (Q03/Q10/Q22/Q26 exercise the same operators *without* a LIMIT, so they are H1/H6 not H2 — see their entries.) |
| **H3** correlated join | Q05(dim⋈dim), Q08(fact⋈dim), Q12, Q15(fact⋈fact), Q16, Q25, Q31, Q32, Q33(dim self-join), Q48, Q49, Q50 | 12 | **yes** — Q05, Q33, Q49, Q50 (dims-only); fact⋈dim Q08; fact⋈fact stress Q15 |
| **H4** decimal/double blind | Q19(decimal PRIMARY), Q18(double), Q06(double dim) ‖ controls: Q20(date-prune), Q21(int-prune), Q24(int), Q30(date on 1M) | 3 blind + 4 controls | **yes** — Q06/Q24 dims-only; Q19 fact decimal vs Q20/Q21 pruning positives on the **same** GL table |
| **H5** COUNT no manifest | Q36(Order 180K), Q37(WebEvent 1M), Q38(Customer 300K), Q39(GL 250K) | 4 | **yes** — Q38 dims-only |
| **H6** agg+join fallback | Q08,Q09,Q10,Q12,Q13,Q25,Q32,Q51 (joined → decline) ‖ fused positives: Q07,Q14,Q22,Q26,Q27 | 8 fallback + 5 fused | **yes** — Q07,Q22 dims-only fused; joined Q08/Q25 |
| **H7** cold/warm | designated cold-protocol subset: Q01(dim), Q19(decimal fact), Q27(1M fact), Q36(count), Q46(top-k), Q08(fact⋈dim) | 6 | **yes** — Q01 dim |
| **H8** non-lowered | Q13(subquery), Q34/Q35(property_path), Q40/Q52(VALUES), Q41(FILTER twin), Q42(bound-subj control), Q51(subquery) | 8 | **yes** — Q34,Q35,Q42 dims-only |

### 4.2 Feature × queries (each ≥ 2)

| Feature | Queries | Count |
|---|---|---|
| `bgp_star` | Q01,Q02,Q03,Q04,Q05,Q06,Q07,Q14,Q22,Q23,Q24,Q26,Q27,Q29,Q30,Q33,Q34,Q41,Q42,Q43,Q44,Q45,Q46,Q47,Q50,Q53 | 26 |
| `join` | Q05,Q08,Q10,Q12,Q13,Q15,Q16,Q17,Q25,Q28,Q31,Q32,Q33,Q35,Q40,Q41,Q48,Q49,Q50,Q51,Q52,Q53 | 22 |
| `fk_chain` | Q05,Q08,Q10,Q12,Q13,Q15,Q16,Q25,Q28,Q31,Q32,Q33,Q40,Q41,Q48,Q49,Q50,Q52 | 18 |
| `filter_range` | Q06,Q18,Q19,Q21,Q24,Q31 | 6 |
| `filter_string` | Q04,Q06,Q28,Q43 | 4 |
| `filter_date` | Q11,Q20,Q30 | 3 |
| `filter_iri` | Q41,Q54 | 2 |
| `optional` | Q16,Q50 | 2 |
| `union` | Q29,Q42 | 2 |
| `aggregate` | Q07,Q08,Q09,Q10,Q12,Q13,Q14,Q18,Q22,Q25,Q26,Q27,Q32,Q36,Q37,Q38,Q39,Q51 | 18 |
| `count` | Q07,Q14,Q18,Q22,Q27,Q36,Q37,Q38,Q39 | 9 |
| `group_by` | Q07,Q08,Q09,Q10,Q12,Q13,Q14,Q18,Q22,Q23,Q25,Q26,Q27,Q32,Q51 | 15 |
| `having` | Q09,Q25 | 2 |
| `order_by` | Q05,Q10,Q12,Q26,Q46 | 5 |
| `distinct` | Q03,Q47 | 2 |
| `subquery` | Q13,Q51 | 2 |
| `values` | Q40,Q42,Q52 | 3 |
| `negation` | Q17,Q53 | 2 |
| `property_path` | Q34,Q35 | 2 |
| `construct` | Q48,Q49 | 2 |

Every hypothesis ≥ 3, every feature ≥ 2. Thinnest margins (worth watching if queries are cut): `optional`, `union`, `having`, `distinct`, `subquery`, `negation`, `property_path`, `construct`, `filter_iri` each sit at exactly 2.

---

## 5. Generator invariants & expected-rows derivation (SF = 0.1)

Base cardinalities (from the assignment; these are the row-count oracles):

| Table | Rows | Note |
|---|---|---|
| DIM_STORE | 500 | |
| DIM_CUSTOMER | 390,000 | **300,000 current + 90,000 SCD history (30%)** |
| DIM_PRODUCT | 37,500 | incl. SCD history; current subset < 37,500 |
| DIM_GEOGRAPHY | 25,000 | |
| DIM_EMPLOYEE | 5,000 | |
| DIM_SUPPLIER | 2,000 | |
| DIM_ACCOUNT | 15,000 | |
| DIM_DATE | 7,670 | ⇒ ~21 years of days |
| FACT_ORDER | 180,000 | |
| FACT_ORDER_LINE | 600,000 | ~3.3 lines/order |
| FACT_PAYMENT | 200,000 | |
| FACT_SHIPMENT | 180,000 | ~1/order |
| FACT_INVENTORY_SNAPSHOT | 300,000 | |
| FACT_GL_JOURNAL | 250,000 | **only xsd:decimal measures** |
| FACT_WEB_EVENT | 1,000,000 | largest |
| FACT_SUPPORT_TICKET | 40,000 | |

**Blessing from the native ledger (the correctness oracle).** The derivations below are *sanity bounds*, not the source of truth. The canonical expected result for every query is **blessed by running it against the WP2-native ledger** (`enterprise-sf01.ttl.zst`, imported + indexed — task WP2-native): the native store holds the identical SF=0.1 data, so its answer (row set + values) is the ground truth the virtual-dataset (Iceberg/R2RML) run must match. The bless step (WP6) executes each `.rq` against the native ledger, canonicalizes per this section's order/float rules, and stores the result in the manifest; the virtual run then asserts equality (correctness) *while* measuring perf (the hypothesis signal). The bounds here let a reviewer spot a grossly wrong bless before it is trusted, and pin the expected-error queries (Q43/Q44) which have no native answer. Two caveats: the native ledger must carry the same SCD history (so Q22/Q23 diverge by 90K — §7 item 3), and the two expected-error queries are asserted by the R2RML router (native has no R2RML layer, so they simply return rows there — do **not** bless them from native).

**Exact-count queries** (derivable from a single table's base count or key domain): Q01 (500), Q32 (500), Q36 (180000), Q37 (1000000), Q38 (300000), Q39 (250000), Q05/Q12 (LIMIT k = exact k), Q45/Q46/Q47 (LIMIT k). **`isCurrent` invariant:** any Customer aggregate is 300,000 with `isCurrent true` and 390,000 without (Q22 vs Q23 — a correctness oracle, not a perf pair). Product current-count is not separately given; treat `< 37,500`, and always pin `isCurrent true` for a stable oracle.

**Bounded `[min,max]` queries** (depend on data distribution the generator controls but we don't have): all `filter_*` result sets, all grouped rollups where the group is a distribution (segments/regions/channels). Where a group domain is structural (regions ≤ Geography.region cardinality; stores = 500) the row count is exact; where it is a value distribution (order status, event type) it is bounded. The WP4 generator should emit the actual group-domain sizes into the manifest so these tighten from `[min,max]` to exact at bless time.

**Join fan-out notes:** fact→fact joins do not multiply when the FK is many-to-one to the parent's PK (each order-line has exactly one order), so Q12/Q15 rows are bounded by the child fact's count, not a product. `OPTIONAL`/`NOT EXISTS` (Q16, Q17, Q53) preserve/anti-select the left cardinality.

### 5.1 Determinism amendment (hash-gate policy)

The first SF01 parity run (`04-findings-register.md` F4) exposed two queries whose native and virtual results were *equally correct* but hashed differently — a **corpus determinism defect**, not an engine bug: `Q05` (`ORDER BY DESC(rating) LIMIT 20` with many `rating=4.99` ties) and `Q49` (`LIMIT 5000` over ~300K rows with no `ORDER BY`). A nondeterministic selection cannot be hash-gated, and left unaddressed it would mask real divergences. Policy, applied to **every LIMIT-bearing query** (audited by `.rq` file against the native truncation):

1. **`ORDER BY … LIMIT` (top-k) → unique tiebreaker.** Append a unique var (the subject IRI or a key/group var) to the sort key so the selected rows — and the exact result hash — are deterministic across engines. The perf shape is unchanged (still top-k over a full scan; H2 intact). Applied to **Q05** (`… DESC(?r) ?sup`), **Q12** (`… DESC(?u) ?pn`), **Q46** (`… DESC(?tot) ?oid`). These stay hash-gated (`Full`).
2. **Unordered `LIMIT` that truncates a larger set → `hash_gate: "rows_only"`.** When any `k` rows are a valid answer, gate on **row count + invariants**, not an exact hash. New optional manifest field `hash_gate` (default `"full"`; enum `full`/`rows_only` in `corpus.rs`). Applied to the 9 queries whose native result equals the LIMIT (truncated): **Q15, Q16, Q28, Q29, Q31, Q45, Q48, Q49, Q53**.
3. **`LIMIT` as a non-binding cap (native rows < LIMIT) → stays `Full`.** The complete unordered set is an order-independent multiset and hashes deterministically (Q17, Q19, Q20, Q21, Q30, Q34, Q35, Q47, Q52).

Each affected `.rq` header records its treatment (a `# Determinism:` line). **Compare/bless wiring:** the gate reads `QueryDef.hash_gate` and, on `RowsOnly`, must **skip the result-hash equality assertion** and pass on row-count-within-`expected_rows` instead (the hook belongs in the WP6 `baseline::check` correctness path; described for that owner, not wired here to avoid a concurrent-edit collision on `baseline.rs`).

---

## 6. Ordering & nondeterminism (per query, for the bless oracle)

- **`order_sensitive: by_keys`** — the query has `ORDER BY` on a total key (comparison is order-preserving): Q05, Q10, Q12, Q26, Q46. For these the bless oracle compares row *sequences*.
- **`order_sensitive: none`** — no `ORDER BY`, or `ORDER BY` on a non-total key: everything else. The oracle must **sort by all projected columns** before comparing (GROUP BY output order is unspecified; a top-k without a tiebreak — none here — would be ambiguous).
- **Float / decimal projections requiring canonicalization** (IEEE-754 or BigDecimal scale can vary across the fold vs materialize paths, inventory §11): Q05 (`rating`), Q06 (`annualRevenue`), Q08/Q09/Q10/Q14 (SUM double), Q15 (`shipCost`), Q18 (SUM double), **Q19/Q20/Q21 (`debitAmount` — xsd:decimal, scale-sensitive)**, Q25/Q26 (AVG), Q40/Q41 (`orderTotal`), Q48 (`orderTotal`), Q50 (`rating`), Q51 (AVG). The manifest must flag these columns so the oracle compares numerically (with an epsilon for double, exact-value for decimal), not lexically. Integer sums (Q12 units, Q32 on-hand) are exact — no flag.
- **Expected-error queries** (Q43, Q44) have no result oracle — the assertion is that execution returns an `InvalidQuery` error naming the unconvertible pattern.

---

## 7. Open questions for WP4 (manifest generation)

1. **Feature-enum gap: IRI equality. — RESOLVED (team, 2026-07-10).** `filter_iri` added to the enum (§2), Q41 retagged, and a dims-only control Q54 added so the tag meets the ≥2 rule. `filter_iri` = IRI / `=` / `IN` equality on a term-typed value.
2. **Property-path outcome (Q34/Q35). — DEFERRED, empirical (team, 2026-07-10).** Kept as **observed-behavior probes**, NOT expected-error. The behavior of an unlowered path over the R2RML source will be settled empirically once virtual-SF01 is registered (DW_SF01 load in flight) and reclassified based on what actually happens; do not pre-bless a result.
3. **`isCurrent` / SCD history. — CONFIRMED live (team, 2026-07-10).** The native sibling carries exactly **300,000 `isCurrent=true` / 390,000 total** Customers, so the Q22/Q23 oracle is exact (diverges by the 90K history rows).
4. **Group-domain sizes.** To convert `[min,max]` rollup counts to exact oracles, the WP4 generator should export the distinct-value cardinality of each grouping column (region, channel, segment, tenderType, eventType, priority, industry, category) into the manifest.
5. **Cold-protocol subset (H7).** Designated: Q01, Q08, Q19, Q27, Q36, Q46 (dim, fact⋈dim, decimal-fact, 1M-fact, count, top-k). These run under all three conditions (cold / hot-process / warm-disk); the rest run hot-process only unless a hypothesis deep-dive needs otherwise.
