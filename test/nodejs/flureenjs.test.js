const flureenjs = require("@fluree/flureenjs");

test('expect all flureenjs functions to be defined', () => {
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
})

test('expect conn, ledger, and query to work', async () => {
  const conn = await flureenjs.jldConnect({
      method: "memory",
      defaults: {
        context: {id: "@id",
                  type: "@type",
                  schema: "http://schema.org/",
                  rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                  rdfs: "http://www.w3.org/2000/01/rdf-schema#",
                  wiki: "https://www.wikidata.org/wiki/",
                  skos: "http://www.w3.org/2008/05/skos#",
                  f: "https://ns.flur.ee/ledger#"},
        did: {
          id: "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
          public: "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
          private: "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c",
        },
      },
  });

  const ledger = await flureenjs.jldCreate(conn, 'testledger');

  // TODO: why does this return []?
  // const results = await flureenjs.jldQuery(
  //   flureenjs.jldDb(ledger),
  //   { 
  //     select: { "?s": ["*"] }, 
  //     where: [["?s", "rdf:type", "https://ns.flur.ee/ledger#DID"]]
  //   }
  // )
  
  // expect(results).toStrictEqual(
  //   [{
  //     id: "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh", 
  //     ":rdf/type": ["f/DID"],
  //     "f/role": {id: "fluree-root-role"}
  //   }]
  // );

  await flureenjs.jldStage(ledger, {"@id": "uniqueId", foo: "foo", bar: "bar"});
  await flureenjs.jldCommit(ledger);

  const results = await flureenjs.jldQuery(
    flureenjs.jldDb(ledger),
    { 
      select: { "?s": ["*"] }, 
      where: [["?s", "@id", "uniqueId"]]
    }
  )

  expect(results).toStrictEqual([{"@id": "uniqueId", foo: "foo", bar: "bar"}])
})

