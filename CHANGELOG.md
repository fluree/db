
# Changelog
All notable changes to FlureeDB will be documented in this file.

## 1.0.0
- Add support for cas (compare and set) SmartFunction (FC-956)
- Fix issues for nodejs permissions (FC-786)
- Update ledger/db to use try*/catch* macro (FC-918)
- Add subs, not= SmartFunctions and stream-line unreversed-var (FC-920)
- Include :storage-list in connection for raft (FC-914)
- Fix local ledger update to return go-channel 
- Improve syncTo by registering a listener w/callback to return results (FC-849, FC-847 , FC-846)
- Fix query cache issue (FC-887)
- Add docs for Clojure API (FC-111)
- Fix issue where collection-default SmartFunctions not always triggering properly (FC-879)
- Fix permission validation to support multi-cardinality roles (FC-879)
- Remove deprecation syntax warnings as both syntax formats will be supported (FC-837)
- Fix validation issues for block query and history query (FC-861)
- Add promise version of history query (FC-878)
- Add support for SQL queries in Clojure and Node.js APIs (FC-501, FC-815, FC-816, FC-817, FC-883)
- Fix sync-to-db API (FC-849)
- Add block-event->map API function (FC-832)
- Add tx-hash generation utility function (FC-809)
- Fix Clojure API connection to pass options (FC-808)
- Standardize try/catch blocks for Clojure/ClojureScript to use macro (FC-636, FC-768)
- Fix GraphQL history query so it works (FC-754, FC-548)
- Fix omit :opts when validating block query format (FC-717)
- Replace read-string macro with reference to read-string (FC-703)
- Update root-role? check to validate against both auth-roles and user-roles (FC-705)
- Remove separate 'opts' parameter for queries in favor of :opts key in query map (FC-626)
- Node.js: override default cljs behavior to support permissions, smart-function code (FC-915)
- Add "predicate-name" Clojure API function (FC-831)
- Fix issue where adding a listener doesn't trigger events if ledger/db was not previously loaded (FC-829)
- Move generate-merkle-root fn to db library for reuse (FC-824)
- Update shared Clojure code to support ClojureScript (FC-784)
- nodejs: Fix issue where run-time cannot find dbfunctions in bindings (FC-785)


## 0.16.0-rc2
- Collection default smartfunctions not always triggering properly (FC-879)

## 0.16.0-rc1
- Production release-candidate

## 0.15.7
- fix issue with combination of string? where, :orderBy and :offset (FC-743)

## 0.15.6
- fix issue where :offset excluded ref data (FC-736)

## 0.15.5
- wire-up auth to subscription (cljs & web-socket) (FC-693)

## 0.15.4
- fix for clj->js to preserve namespaces (FC-672)
- Permissioned DB will fail with empty blocks (FC-688)
- Fix for root permissions to skip filtering (FC-689)

## 0.15.3
- upd root-role? to handle nil or string auth (FC-659)

## 0.15.2
- Modifications for On-Demand access thru JavaScript apis (FC-607)

## 0.15.1
- Fixed prettyPrint bug, where variable names were inaccurate

## 0.15.0
- Deleting a subject that includes already deleted predicates, simply retracts those predicates, rather than throwing an error. [ES-33]  
- Support Mutability [FC-116]
- Create Clojure / Javascript APIs (likely nodejs only) for working with JWT tokens [FC-161]
- Query with sync-to block [FC-182]
- Pretty-print in block and history query blows stack when sufficiently large [FC-183]
- Fluree-js webworker, reset() connection does not appear to proactively close old websocket connection (but it will timeout) [FC-198]
- Transpiler: SQL to analytical queries [FC-210]
- Handle remove-from-idx feature [FC-229]
- Version number not updating in Raft state [FC-259]
- When selecting from a subject, flakes where object is false should return [FC-300]
- db-with shouldn't throw when an entire tx is filtered by permissions [FC-306]
- JavaScript/NodeJS: Add API to generate a new user auth record/account [FC-327]
- Password Auth: Sync api logic with http-api [FC-358]
- JavaScript Libraries - Handle expiration of token(s) on cached db queries [FC-365]
- JavaScript Libraries - Password Auth: Implement the createUser option [FC-366]
- Fluree Service Worker - Handle jwt expiration [FC-368]
- Remove old datalog references in [fluree.db.query.analytical :as datalog] [FC-395]
- JavaScript Library - Support client-override of default logging level (warn) [FC-403]
- SPARQL union - issue with different variable names on left vs. right side [FC-404]
- Mutable: Switch to _tx/hash, not _block/hash [FC-422]
- Mutable: Block-version use url-safe separator (not :) [FC-423]
- Fluree React/Worker Service - Add additional exported API within fluree-react package that allows sending of various messages to flureeworker service worker (should probably match Fluree's HTTP API endpoints, see comment) [FC-431]
- Fluree React/Worker Service - Current query registration doesn't allow queries that would require multi-query, block, or history endpoints [FC-432]
- fluree.db.time-travel works with 't' or a string time, but will not return the correct :block on the db in those cases [FC-458]
- Add dbproto/-latest-db to be able to get the most current db from any db [FC-466]
- api/db can be made more efficient with multi-arity [FC-467]
- flureedb CLJS API (db...) requires clojure map as options - should be JS object [FC-472]
- Flureeworker - allow :forceTime key for query options to use default block/time for time travel (works with js-react-wrapper) [FC-473]
- Allow flureeworker cljs compiler to used optimizations :none for easier dev debugging [FC-474]
- CLJS history query formatting too restrictive [FC-492]
- flureeworker has infinite loop when react wrapper query is a function [FC-493]
- Make JS history query pretty-print camel case [FC-494]
- JS Libraries - using a private key does not filter new blocks from websocket [FC-499]
- Full-text indexes should not be supported in-memory [FC-513]
- Having multiple select-type keys in a query should throw an error, i.e. selectOne and select [FC-538]
- 1.0- Deprecate top-level component, limit, offset, and orderBy for basic queries [FC-539]
- pretty-print option in queries should be prettyPrint to better support JS. Probably support both formats but deprecate pretty-print [FC-550]
- show-auth option in history query should be showAuth to better support JS. Probably support both formats but deprecate show-auth [FC-551]
- Basic query where clause should throw error when attempting to filter on non-indexed predicate [FC-560]
- Block range with pretty-print does not include block and t keys [FC-562]
- Analytical query aggregate functions should offer the option to perform aggregate function on distinct vs. non-distinct values [FC-564]
- GraphQL API - support _id: and ident: arguments to select a single entity [FC-571]
- If predicate does not exist, returns all flakes [FC-573]
- Analytical query object as filter not working any longer [FC-574]
- If predicate is not indexed, if we provide an object in analytical query, it will not return anything [FC-576]
- Analytical query: expand map and var can't be in select [FC-577]
- Analytical queries: If using recur should not forward substitute if possible [FC-578]
- Analytical queries: Recur-depth not working, just returns maximum recursion depth [FC-579]
- Analytical query- move groupBy, limit, etc to top-level opts map [FC-580]
- flureedb CLJS API (db...) requires clojure map as options - should be JS object [FC-592]

## 0.14.0
- Promoted 0.13.6 to stable [FC-437]

## 0.13.6
- Stop transducer when fuel >= max-fuel [FC-324]
- selectOne with 1mil+ record doesn't trigger fuel error or time out [FC-426]
- fix exception in shared oode for cljs [FC-465]
- fix bug where pred-objects-unique threw error if empty [FC-468]
- JS Libraries - fix/verify multi-source queries [FC-489]

## 0.13.5
- Sort does not handle upper/lowercase in the expected manner. [FC-471]

## 0.13.4
- Fix Union bug where unmatched b-tuple rows were not added to results properly [FC-404]
- When selecting from a predicate where predicate is false,val to appear in results [FC-300]
- (cljs) Add pwgenerate & transact functions/messages to flureeworker [FC-431]
- Add dbproto/-latest-db to be able to get the most current db from any db [FC-466]
- Fix time travel bugs to allow any 't' any block, and any time and return a correct db [FC-458]
- make api/db call a bit more efficient [FC-467]
- Fix time travel exception capture not CLJS compliant [FC-458]
- (cljs) flureedb cljs API should accept JS object as options for (db ..) [FC-472]
- (cljs) flureeworker - Allow forceTime query option to support time travel in js-react-wrapper [FC-473] 

## 0.13.3
- JavaScript/NodeJS: Add API to generate a new user auth record/account [FC-327]
- NodeJS Library - Support client override of default log level (warn) [FC-360]
- JavaScript Libraries - Password Auth: Implement the createUser option [FC-366]
- JavaScript Library - Support client-override of default logging level (warn) [FC-403]

## 0.13.2
- (cljs) Support keep-alive option to attempt a reconnect to the Fluree server after 'ping' detects a socket-error. [FC-198]

## 0.13.1
- (cljs) Password Auth return ExceptionInfo as promise "reject" [FC-384]

## 0.13.0
### Added
- Provide APIs (cljs/clj) to retrieve ledger-info and ledger-stats [FC-221] 
- Support for cross-database, cross-time, cross-network analytical queries [FC-140]
- Full-text search in 10 languages enabled in analytical queries [FC-114]
- Support for recursion in analytical queries [FC-26]
- Support for commands with dependencies and unsigned commands when using an open API [FC-177]
- Analytical query expansion - OR, UNION, etc [FC-189]
- Support for `strStarts` and `strEnds` in analytical queries [FC-191]
- Bug fix - predicates that become `index` or `unique` get added to the :post index [FC-192]
- Support for escaped strings in analytical queries, esp. for string literals that begin with a `?` [FC-194]
- Support for offset as a sub-select option [FC-211]
- History queries can now return auth information, or be filter to only include results submitted by particular auth records [FC-133]
- Sync cljs with clj apis [FC-231]
- Refactor end-point block-range-with-txn-async [FC-238]
- Short-circuit flake filtering across web socket when root auth is used (open-api=true) [FC-294]
- Add new :compact true option in new :opts key for queries [FC-295]
- Add :cache true option to :opts in query map to cache a query in object cache [FC-296]
- HTTP-signatures need to work in cljs/javascript natively, remove host from sig [FC-303]
- Error messages not propagated through the web socket (:db/invalid-query, :db/invalid-auth) [FC-330]
- Password Auth: Sync api logic with http-api [FC-358]
- NodeJS Library [FC-106, FC-284, FC-289, FC-326]

## 0.12.4
- If vector wrapped in a vector gets passed to hash-set, then apply hash-set to the interior vector.

## 0.12.3
- Deleting a subject that includes already deleted predicates, simply retracts those predicates, rather than throwing an error. 

## 0.12.2
- Ensure queries skip over any empty index nodes, rather than attempting to read them

## 0.12.0
- React Wrapper: Improve Time Travel Widget [FC-89]
- Fix Queries where last subject is split across multiple nodes, only includes part of the subject [FC-166]
- Verify that large transactions (up to 2MB) are properly handled, without crashing [FC-175]
- Fluree-React Wrapper not receiving updated blocks when ledger updated [FC-234]
- Fix block range query was not responding [FC-240]
- ISO-8601 strings not accepted in /block queries [FC-241]
- History query not working when block not provided [FC-268]
- Flureeworker ignores block when specified in a query [FC-273]
- Flureedb transaction ids do not match expected values/formats [FC-274]

## 0.11.7

- Ensure history qurey returns proper response without block having to be specified.

## 0.11.5 
- Fix block range query was not responding

## 0.11.3
- Fix: Flureeworker executing queries before cached db is updated [FC-234]

## 0.11.0
### Added
- Support (relationship?) smart function [FC-3]
- Support combined 'from' collection and 'where' SQL-like clause [FC-10]
- Support Password Authentication for Private Key Generation [FC-13]
- Support authority in signed queries [FC-25]
- Support sort/order by feature in all query languages [FC-29]
- Archive databases + create database from archive [FC-30]
- Support Group-By in Analytical Queries [FC-49]
- Analytical queries should be able to use interim aggregate values [FC-63]
- Replace datalog query namespace with our own custom query logic parsing [FC-64]
- Predicate type - bytes [FC-73]
- JavaScript APIs [FC-77]
- Support Delete Ledger action [FC-90]
- SPARQL supports new analytical query format [FC-92]
- Add permissioned flake filtering to index and block retrieval, along with block push via websockets [FC-93]
- Create API to get Password-Auth JWT token [FC-95]
- Support usage of "now" as a function in filtering [FC-96]
- Update SHA3 library for compilation with ClojureScript APIs [FC-104]
- History query with paging (some combination of block, limit, offset) [FC-134]
- Analytical queries support selectDistinct ability [FC-137]
- Create api to renew JWT tokens [FC-158]
- Create Build constants namespace/file [FC-168]
- Support analytical query with vars [FC-188]
- Create "_compress" option for queries, that will remove the namespaces of results [FC-195]
- fluree-react - handle query status and error reporting [FC-196]
- Support query vars with JavaScript/React Library [FC-197]

### Changed
- 'Predicate is multi-cardinality and the value must be vector/array:' error should identify predicate [FC-32]
- Support int as object in SPARQL [FC-33]
- Analytical query - includ. "optional" for analytical query should not be dependent on order [FC-34]
- Undo deprecation of db-functions [FC-35]
- Ensure analytical queries with select value like `{"?dbs": ["*"}` returns a vector, not a vector wrapped in a vector [FC-36]]
- Fix: Analytical query should throw better error on no var in query [FC-39]
- Check the predicate naming has good regex. GraphQL throws error when we use '-' in predicate names [FC-42]
- Select from a single subject should return a map, not a vector [FC-45]
- Fix: Admin UI has no icons in Safari [FC-46]
- Properly handle blocks of time before start of the db [FC-47]
- Limit not working in 0.9.6? [FC-56]
- Multi-queries should provide more semantic errors when the query keys are reserved words like "block" and "select" [FC-68]
- Make db names a-z0-9-, including upgrade script [FC-70]
- Ability to filter based on ?username [FC-71]
- Deletion of multiple predicates at once was failing in certain circumstances [FC-75]
- Align predicate naming with what is allowed in GraphQl, part/serial-number is allowed to be created, but fails in GraphQL [FC-84]
- Propagate errors for multi-query requests [FC-91]
- Fix: Wikidata errors in new analytical queries not propagating up [FC-94]
- Fix: Improper filter in analytical query is returning an unhelpful error [FC-102]
- When issuing a query for a predicate of type 'tag', response shows object of predicate as "[collection]/[predicate]:[object]" instead of just "[object]" if query only mentions predicate and not collection/predicate [FC-145]
- Minimize footprint of compiled JavaScript files: flureedb.js & flureeworker.js [FC-146]
- JavaScript-Compiled APIs: Improve Identification of 'stacktrace' Error for (signed/unsigned) Transactions [FC-147]
- JavaScript-Compiled APIs: Signed queries should be submitted as commands to the server [FC-148]
- Update JavaScript API/Library documentation to exclude references to GraphQL and sparQL [FC-149]
- Incorporate Password-Auth JWT token in cljs api library [FC-150]
- JavaScript Library Docs - Provide connect' example [FC-157]
- Check for JWT tokens in http API and sign requests as needed [FC-162]
- Data version upgrade triggers with new instance [FC-167]
- Fluree React Wrapper: Wire-Up DB Connection Event [FC-170]
- Test that large transactions (up to 2MB) are properly handled, without crashing. [FC-175]
- Lagging back-slash causes 404 [FC-178]
- Fluree React Wrapper: Wire-up Login/Password Auth [FC-184]
- Change "archive" naming to "snapshot" [FC-185]
- Analytical query fails/gives wrong results with boolean types [FC-186]
- Fluree JS webworker - add back in connection close and reset actions [FC-199]
- Query options don't work with ad-hoc queries (i.e. 'compact: true') [FC-200]
- Clean Build for cljs libraries [FC-201]
- No discernable order in select from multiple subjects [FC-205]
- Create database from snapshot should throw error if snapshot file incorrect. [FC-212]

### Removed
- Removed fdb-sendgrid-auth from server settings [FC-86]
- Document reverse refs not supported [FC-105]



