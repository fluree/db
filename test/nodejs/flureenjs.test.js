const flureenjs = require("@fluree/flureenjs");
const fs = require("fs");
const path = require("path");

test("expect all flureenjs functions to be defined", () => {
  expect(Object.keys(flureenjs).sort()).toStrictEqual([
    "accountId",
    "blockEventToMap",
    "blockQuery",
    "blockRange",
    "blockRangeWithTxn",
    "close",
    "closeListener",
    "collectionFlakes",
    "collectionId",
    "connect",
    "db",
    "deleteLedger",
    "forwardTimeTravel",
    "graphql",
    "historyQuery",
    "httpSignature",
    "isForwardTimeTravelDb",
    "jldCommit",
    "jldConnect",
    "jldCreate",
    "jldDb",
    "jldLoad",
    "jldQuery",
    "jldStage",
    "jldStatus",
    "ledgerInfo",
    "ledgerList",
    "listen",
    "listeners",
    "monitorTx",
    "multiQuery",
    "newLedger",
    "newPrivateKey",
    "passwordGenerate",
    "passwordLogin",
    "predicateId",
    "predicateName",
    "publicKey",
    "publicKeyFromPrivate",
    "query",
    "queryWith",
    "renewToken",
    "resolveLedger",
    "search",
    "session",
    "setDefaultKey",
    "setLogging",
    "sign",
    "sparql",
    "sql",
    "subid",
    "transact",
    "txToCommand",
  ]);
});

test("expect conn, ledger, stage, commit, and query to work", async () => {

  const conn = await flureenjs.jldConnect({
    method: "memory",
    defaults: {
      context: {
        id: "@id",
        type: "@type",
        schema: "http://schema.org/",
        rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        rdfs: "http://www.w3.org/2000/01/rdf-schema#",
        wiki: "https://www.wikidata.org/wiki/",
        skos: "http://www.w3.org/2008/05/skos#",
        f: "https://ns.flur.ee/ledger#",
      },
      did: {
        id: "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
        public:
          "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
        private:
          "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c",
      },
    },
  });

  const ledger = await flureenjs.jldCreate(conn, "testledger");

   const results = await flureenjs.jldQuery(
     flureenjs.jldDb(ledger),
     {
       select: { "?s": ["*"] },
       where: [["?s", "rdf:type", "https://ns.flur.ee/ledger#DID"]]
     }
   )

   expect(results).toStrictEqual(
       [
         {
           id: 'did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6',
           'rdf:type': [ 'f:DID' ],
           'f:role': { id: 'fluree-root-role' }
         }
       ]
   );

   // test providing context works and remaps keys
   const contextResults = await flureenjs.jldQuery(
     flureenjs.jldDb(ledger),
     { "@context": {"flhubee": "https://ns.flur.ee/ledger#role"},
       select: { "?s": ["*"] },
       where: [["?s", "rdf:type", "https://ns.flur.ee/ledger#DID"]]
     }
   );

   expect(contextResults).toStrictEqual(
    [
      {
        id: 'did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6',
        flhubee: { id: 'fluree-root-role' },
        'rdf:type': [ 'f:DID' ]
      }
    ]
   );


  const db = await flureenjs.jldStage(ledger, {
    "@id": "uniqueId",
    foo: "foo",
    bar: "bar",
  });

//  await flureenjs.jldCommit(db);

  const results2 = await flureenjs.jldQuery(db, {
    select: { "?s": ["*"] },
    where: [["?s", "@id", "uniqueId"]],
  });

  expect(results2).toStrictEqual([
    { "id": "uniqueId", foo: "foo", bar: "bar" },
  ]);
});

// TODO: Fix and uncomment
//test("file conn", async () => {
//  const testJson = {
//    "@context": "https://schema.org",
//    id: "https://www.wikidata.org/wiki/Q836821",
//    type: ["Movie"],
//    name: "The Hitchhiker's Guide to the Galaxy",
//    disambiguatingDescription:
//      "2005 British-American comic science fiction film directed by Garth Jennings",
//    titleEIDR: "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
//    isBasedOn: {
//      id: "https://www.wikidata.org/wiki/Q3107329",
//      type: "Book",
//      name: "The Hitchhiker's Guide to the Galaxy",
//      isbn: "0-330-25864-8",
//      author: {
//        "@id": "https://www.wikidata.org/wiki/Q42",
//        "@type": "Person",
//        name: "Douglas Adams",
//      },
//    },
//  };
//
//  const connOpts = {
//    method: "file",
//    "storage-path": "store/",
//    context: {
//      id: "@id",
//      type: "@type",
//      schema: "http://schema.org/",
//      rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
//      rdfs: "http://www.w3.org/2000/01/rdf-schema#",
//      wiki: "https://www.wikidata.org/wiki/",
//      skos: "http://www.w3.org/2008/05/skos#",
//      f: "https://ns.flur.ee/ledger#",
//    },
//  };
//  const conn = await flureenjs.jldConnect(connOpts);
//  const ledgerName = "jld/one";
//  const newLedger = await flureenjs.jldCreate(conn, ledgerName);
//  const db = await flureenjs.jldStage(newLedger, testJson);
//
//  await flureenjs.jldCommit(newLedger, db, {
//    message: "commit!",
//    push: true,
//  });
//  // console.log("DB", db);
//
//  const results = await flureenjs.jldQuery(db, {
//    select: { "?s": ["id", "isbn", "name", "type"] },
//    where: [["?s", "id", "https://www.wikidata.org/wiki/Q3107329"]],
//  });
//
//  expect(results).toStrictEqual([
//    {
//      id: "https://www.wikidata.org/wiki/Q3107329",
//      type: "Book",
//      name: "The Hitchhiker's Guide to the Galaxy",
//      isbn: "0-330-25864-8",
//    },
//  ]);
//
//  const head = fs.readFileSync(
//    path.resolve(".", connOpts["storage-path"], ledgerName, "main/HEAD"),
//    "utf8"
//  );
//  const commit = JSON.parse(fs.readFileSync(head, "utf8"));
//
//  expect(commit.data.id).toBe(
//    "fluree:db:sha256:bbymrlnzurn2b2ehcay25dcd2s3vcec3a2uq6ks2nfeqt5iylzvh6"
//  );
//});
