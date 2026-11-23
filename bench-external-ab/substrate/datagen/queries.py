"""SQL generation templates (DuckDB dialect).

One `string.Template` per table. Placeholders use `$name` and are filled by
generate.py from a context dict (parent-dimension counts, date range, etc.).

Design notes
------------
* FKs are valid *by construction*: a foreign key is just a random integer in the
  parent dimension's surrogate-key range (`rkey(n)` = 1..n). No joins needed.
* Dates on the big facts are derived *monotonically* from the row index / parent
  key, so each ~128 MB Parquet part covers a contiguous date range -> excellent
  Iceberg min/max file pruning, and no expensive global sort.
* SCD-2 dims (customer, product) generate `n_current` current rows (is_current =
  true) followed by historical versions that reuse an existing business key.
  Facts reference only the current range.
"""

from string import Template

# Macros installed once per connection (see generate.py SETUP_SQL).
SETUP_SQL = """
INSTALL icu; LOAD icu;
CREATE OR REPLACE MACRO rkey(n)        AS CAST(1 + floor(random()*n) AS BIGINT);          -- 1..n
CREATE OR REPLACE MACRO rint(lo, hi)   AS CAST(lo + floor(random()*(hi-lo+1)) AS BIGINT); -- lo..hi
CREATE OR REPLACE MACRO rdec(lo, hi, s) AS round((lo + random()*(hi-lo)), s);
CREATE OR REPLACE MACRO rday(span)     AS CAST(floor(random()*(span+1)) AS INTEGER); -- 0..span as INT (for DATE +)
CREATE OR REPLACE MACRO choice(arr)    AS arr[CAST(1 + floor(random()*len(arr)) AS INT)];
CREATE OR REPLACE MACRO maybe(p, v)    AS CASE WHEN random() < p THEN v ELSE NULL END;
CREATE OR REPLACE MACRO dkey(d)        AS CAST(strftime(d, '%Y%m%d') AS INT);
"""

QUERIES = {
    # ---------------- dimensions ----------------
    "dim_date": Template("""
SELECT
  dkey(d)                                   AS date_key,
  d                                         AS date,
  CAST(isodow(d) AS INT)                     AS day_of_week,
  dayname(d)                                AS day_name,
  CAST(day(d) AS INT)                        AS day_of_month,
  CAST(dayofyear(d) AS INT)                  AS day_of_year,
  CAST(week(d) AS INT)                       AS week_of_year,
  CAST(month(d) AS INT)                      AS month_num,
  monthname(d)                              AS month_name,
  CAST(quarter(d) AS INT)                    AS quarter_num,
  CAST(year(d) AS INT)                       AS year_num,
  (isodow(d) >= 6)                           AS is_weekend,
  CAST(year(d) AS INT)                       AS fiscal_year,
  CAST(quarter(d) AS INT)                    AS fiscal_quarter
FROM (SELECT DATE '$date_start' + CAST(i AS INTEGER) AS d FROM range(0, $n_rows) t(i))
"""),

    "dim_geography": Template("""
WITH b AS (
  SELECT i, CAST(floor(random()*10) AS INT) AS ci FROM range(0, $n_rows) t(i)
)
SELECT
  i + 1                                                                AS geography_key,
  (['US','GB','DE','FR','CA','AU','JP','BR','IN','MX'])[ci+1]          AS country_code,
  (['United States','United Kingdom','Germany','France','Canada',
    'Australia','Japan','Brazil','India','Mexico'])[ci+1]             AS country,
  'Region-'   || CAST(rint(1, 9)  AS VARCHAR)                          AS region,
  'State-'    || CAST(rint(1, 50) AS VARCHAR)                          AS state_province,
  'City-'     || CAST(rint(1, 800) AS VARCHAR)                         AS city,
  lpad(CAST(rint(1000, 99999) AS VARCHAR), 5, '0')                     AS postal_code,
  round(-90.0  + random()*180.0, 6)                                    AS latitude,
  round(-180.0 + random()*360.0, 6)                                    AS longitude
FROM b
"""),

    "dim_supplier": Template("""
SELECT
  i + 1                                              AS supplier_key,
  'SUP-' || lpad(CAST(i+1 AS VARCHAR), 7, '0')        AS supplier_id,
  'Supplier ' || CAST(i+1 AS VARCHAR)                AS supplier_name,
  rkey($n_geography)                                 AS geography_key,
  'contact' || CAST(i+1 AS VARCHAR) || '@supplier.example.com' AS contact_email,
  CAST(rint(2, 45) AS INT)                            AS lead_time_days,
  rdec(2.0, 5.0, 2)                                  AS rating
FROM range(0, $n_rows) t(i)
"""),

    "dim_account": Template("""
SELECT
  i + 1                                              AS account_key,
  'ACCT-' || lpad(CAST(i+1 AS VARCHAR), 8, '0')       AS account_id,
  'Account ' || CAST(i+1 AS VARCHAR)                 AS account_name,
  choice(['Retail','Manufacturing','Healthcare','Finance','Technology',
          'Education','Government','Logistics','Energy','Media'])      AS industry,
  CAST(rint(10, 50000) AS INT)                        AS employee_count,
  rdec(100000.0, 500000000.0, 2)                     AS annual_revenue,
  choice(['Bronze','Silver','Gold','Platinum'])      AS tier,
  rkey($n_geography)                                 AS geography_key,
  DATE '$date_start' + rday($span_days)           AS created_date
FROM range(0, $n_rows) t(i)
"""),

    "dim_store": Template("""
SELECT
  i + 1                                              AS store_key,
  'STORE-' || lpad(CAST(i+1 AS VARCHAR), 5, '0')      AS store_id,
  'Store ' || CAST(i+1 AS VARCHAR)                   AS store_name,
  choice(['Online','Online','Retail','Retail','Wholesale'])           AS channel,
  choice(['Flagship','Standard','Outlet','Popup','Warehouse'])        AS store_type,
  rkey($n_geography)                                 AS geography_key,
  DATE '$date_start' + rday($span_days)           AS open_date,
  rkey($n_employee)                                  AS region_manager_key
FROM range(0, $n_rows) t(i)
"""),

    "dim_employee": Template("""
SELECT
  i + 1                                              AS employee_key,
  'EMP-' || lpad(CAST(i+1 AS VARCHAR), 7, '0')        AS employee_id,
  'Employee ' || CAST(i+1 AS VARCHAR)                AS full_name,
  'emp' || CAST(i+1 AS VARCHAR) || '@corp.example.com' AS email,
  choice(['Sales Rep','Sales Rep','Support Agent','Support Agent','Manager',
          'Analyst','Buyer','Warehouse'])                            AS role,
  choice(['Sales','Support','Finance','Operations','Marketing','IT']) AS department,
  maybe(0.7, rkey($n_store))                          AS store_key,
  CASE WHEN i < ($n_rows / 10) THEN NULL ELSE rint(1, ($n_rows/10)+1) END AS manager_key,
  DATE '$date_start' + rday($span_days)           AS hire_date,
  (random() < 0.9)                                   AS is_active
FROM range(0, $n_rows) t(i)
"""),

    # SCD-2: rows [0, n_current) are current; the rest are historical versions.
    "dim_customer": Template("""
SELECT
  i + 1                                                                AS customer_key,
  'CUST-' || lpad(CAST((CASE WHEN i < $n_current THEN i
                             ELSE rint(0, $n_current-1) END) AS VARCHAR), 9, '0') AS customer_id,
  'Customer ' || CAST(i+1 AS VARCHAR)                                  AS full_name,
  'user' || CAST(i+1 AS VARCHAR) || '@example.com'                     AS email,
  '+1-' || lpad(CAST(rint(2000000000, 9999999999) AS VARCHAR), 10, '0') AS phone,
  choice(['Consumer','Consumer','Consumer','Consumer','SMB','Enterprise']) AS segment,
  choice(['F','M','U'])                                                AS gender,
  CAST(rint(1940, 2007) AS INT)                                        AS birth_year,
  rkey($n_geography)                                                  AS geography_key,
  maybe(0.25, rkey($n_account))                                       AS account_key,
  DATE '$date_start' + rday($span_days)                            AS signup_date,
  DATE '$date_start' + rday($span_days)                            AS scd_valid_from,
  CASE WHEN i < $n_current THEN DATE '9999-12-31'
       ELSE DATE '$date_start' + rday($span_days) END              AS scd_valid_to,
  (i < $n_current)                                                    AS is_current
FROM range(0, $n_rows) t(i)
"""),

    "dim_product": Template("""
SELECT
  i + 1                                                                AS product_key,
  'SKU-' || lpad(CAST((CASE WHEN i < $n_current THEN i
                            ELSE rint(0, $n_current-1) END) AS VARCHAR), 9, '0') AS product_id,
  'Product ' || CAST(i+1 AS VARCHAR)                                   AS product_name,
  choice(['Acme','Globex','Initech','Umbrella','Soylent','Stark',
          'Wayne','Wonka','Hooli','Vandelay'])                        AS brand,
  choice(['Electronics','Apparel','Home','Grocery','Toys','Sports',
          'Beauty','Automotive','Office','Garden'])                   AS category,
  'Subcat-' || CAST(rint(1, 12) AS VARCHAR)                            AS subcategory,
  choice(['Hardlines','Softlines','Consumables','Services'])          AS department,
  rkey($n_supplier)                                                  AS supplier_key,
  rdec(1.0, 800.0, 2)                                                 AS unit_cost,
  rdec(2.0, 1500.0, 2)                                                AS list_price,
  DATE '$date_start' + rday($span_days)                            AS scd_valid_from,
  CASE WHEN i < $n_current THEN DATE '9999-12-31'
       ELSE DATE '$date_start' + rday($span_days) END              AS scd_valid_to,
  (i < $n_current)                                                    AS is_current
FROM range(0, $n_rows) t(i)
"""),

    # ---------------- facts ----------------
    # order_key is monotonic in i -> order_date monotonic -> files time-sorted.
    "fact_order": Template("""
SELECT
  i + 1                                                                AS order_key,
  'ORD-' || lpad(CAST(i+1 AS VARCHAR), 12, '0')                        AS order_id,
  rkey($n_customer_cur)                                               AS customer_key,
  maybe(0.25, rkey($n_account))                                       AS account_key,
  rkey($n_store)                                                     AS store_key,
  maybe(0.6, rkey($n_employee))                                       AS sales_rep_key,
  dkey(DATE '$date_start' + CAST((i+1) * $span_days / $n_rows AS INT)) AS order_date_key,
  DATE '$date_start' + CAST((i+1) * $span_days / $n_rows AS INT)       AS order_date,
  choice(['Completed','Completed','Completed','Shipped','Cancelled','Returned']) AS order_status,
  choice(['Web','Web','Mobile','Store','Phone'])                      AS order_channel,
  rdec(10.0, 5000.0, 2)                                               AS order_total,
  'USD'                                                              AS currency
FROM range(0, $n_rows) t(i)
"""),

    # order_key derived so it stays within fact_order range and stays monotonic.
    "fact_order_line": Template("""
WITH b AS (
  -- floor (not CAST, which rounds in DuckDB) so okey stays within 1..$n_order
  SELECT i, 1 + CAST(floor(CAST(i AS DOUBLE) * $n_order / $n_rows) AS BIGINT) AS okey
  FROM range(0, $n_rows) t(i)
)
SELECT
  i + 1                                                               AS order_line_key,
  okey                                                                AS order_key,
  rkey($n_product_cur)                                               AS product_key,
  CAST((i % 8) + 1 AS INT)                                            AS line_number,
  CAST(rint(1, 10) AS INT)                                            AS quantity,
  rdec(2.0, 1500.0, 2)                                               AS unit_price,
  rdec(0.0, 0.4, 4)                                                  AS discount_pct,
  rdec(2.0, 9000.0, 2)                                               AS extended_amount,
  DATE '$date_start' + CAST(okey * $span_days / $n_order AS INT)      AS order_date
FROM b
"""),

    "fact_inventory_snapshot": Template("""
SELECT
  i + 1                                                               AS inventory_key,
  dkey(DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT))     AS snapshot_date_key,
  DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT)           AS snapshot_date,
  rkey($n_product_cur)                                               AS product_key,
  rkey($n_store)                                                     AS store_key,
  CAST(rint(0, 5000) AS INT)                                          AS on_hand_qty,
  CAST(rint(0, 500) AS INT)                                           AS reserved_qty,
  CAST(rint(10, 200) AS INT)                                          AS reorder_point,
  CAST(rint(0, 1000) AS INT)                                          AS units_on_order
FROM range(0, $n_rows) t(i)
"""),

    "fact_shipment": Template("""
SELECT
  i + 1                                                               AS shipment_key,
  'SHP-' || lpad(CAST(i+1 AS VARCHAR), 12, '0')                       AS shipment_id,
  rkey($n_order)                                                     AS order_key,
  dkey(DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT))     AS ship_date_key,
  DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT)           AS ship_date,
  choice(['UPS','FedEx','USPS','DHL','Amazon'])                      AS carrier,
  choice(['Ground','Express','TwoDay','Overnight','Economy'])        AS ship_method,
  choice(['Delivered','Delivered','InTransit','Pending','Returned']) AS ship_status,
  'TRK' || lpad(CAST(rint(1, 999999999) AS VARCHAR), 12, '0')         AS tracking_number,
  rdec(2.0, 120.0, 2)                                                AS ship_cost,
  rkey($n_geography)                                                 AS dest_geography_key
FROM range(0, $n_rows) t(i)
"""),

    "fact_payment": Template("""
SELECT
  i + 1                                                               AS payment_key,
  'PAY-' || lpad(CAST(i+1 AS VARCHAR), 12, '0')                       AS payment_id,
  rkey($n_order)                                                     AS order_key,
  rkey($n_customer_cur)                                              AS customer_key,
  dkey(DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT))     AS payment_date_key,
  DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT)           AS payment_date,
  choice(['Card','Card','Card','PayPal','BankTransfer','GiftCard'])  AS tender_type,
  rdec(5.0, 5000.0, 2)                                               AS amount,
  'USD'                                                             AS currency,
  choice(['Captured','Captured','Authorized','Refunded','Failed'])  AS payment_status
FROM range(0, $n_rows) t(i)
"""),

    "fact_gl_journal": Template("""
SELECT
  i + 1                                                               AS journal_key,
  'GL-' || lpad(CAST(i+1 AS VARCHAR), 12, '0')                        AS journal_id,
  dkey(DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT))     AS posting_date_key,
  DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT)           AS posting_date,
  CAST(rint(1000, 9999) AS INT)                                       AS gl_account_code,
  choice(['Revenue','COGS','Opex','Payroll','Tax','Inventory',
          'Receivable','Payable','Cash','Depreciation'])             AS gl_account_name,
  'CC-' || CAST(rint(1, 200) AS VARCHAR)                              AS cost_center,
  CAST(maybe(0.5, rdec(0.0, 100000.0, 2)) AS DECIMAL(18,2))           AS debit_amount,
  CAST(maybe(0.5, rdec(0.0, 100000.0, 2)) AS DECIMAL(18,2))           AS credit_amount,
  'USD'                                                             AS currency,
  choice(['Orders','Payments','Inventory','Payroll','Manual'])      AS source_module
FROM range(0, $n_rows) t(i)
"""),

    "fact_web_event": Template("""
SELECT
  i + 1                                                               AS event_key,
  'EVT-' || lpad(CAST(i+1 AS VARCHAR), 14, '0')                       AS event_id,
  CAST(i / 6 AS BIGINT) + 1                                           AS session_id,
  maybe(0.6, rkey($n_customer_cur))                                  AS customer_key,
  dkey(DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT))     AS event_date_key,
  DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT)           AS event_date,
  (DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT))::TIMESTAMP
     + to_seconds(CAST(rint(0, 86399) AS INT))                        AS event_ts,
  choice(['page_view','page_view','page_view','add_to_cart','search',
          'checkout','purchase','remove_from_cart'])                 AS event_type,
  '/p/' || CAST(rint(1, 100000) AS VARCHAR)                           AS page_url,
  maybe(0.5, rkey($n_product_cur))                                   AS product_key,
  choice(['desktop','desktop','mobile','mobile','tablet'])           AS device_type,
  choice(['Chrome','Safari','Firefox','Edge','Other'])              AS browser,
  choice(['google','direct','email','social','affiliate'])          AS referrer
FROM range(0, $n_rows) t(i)
"""),

    "fact_support_ticket": Template("""
SELECT
  i + 1                                                               AS ticket_key,
  'TCK-' || lpad(CAST(i+1 AS VARCHAR), 12, '0')                       AS ticket_id,
  rkey($n_customer_cur)                                              AS customer_key,
  maybe(0.7, rkey($n_product_cur))                                   AS product_key,
  rkey($n_employee)                                                 AS agent_key,
  dkey(DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT))     AS open_date_key,
  DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT)           AS open_date,
  DATE '$date_start' + CAST(i * $span_days / $n_rows AS INT) + CAST(rint(0, 30) AS INT) AS close_date,
  choice(['Closed','Closed','Closed','Open','Escalated'])           AS status,
  choice(['Low','Medium','Medium','High','Urgent'])                AS priority,
  choice(['Billing','Shipping','Product','Returns','Account','Other']) AS category,
  CAST(rint(1, 5) AS INT)                                            AS csat_score,
  rdec(0.5, 240.0, 1)                                               AS resolution_hours
FROM range(0, $n_rows) t(i)
"""),
}
