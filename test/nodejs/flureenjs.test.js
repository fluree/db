const flureenjs = require("@fluree/fluree-node-sdk");
const fs = require("fs");
const path = require("path");

test("expect all flureenjs functions to be defined", () => {
  expect(Object.keys(flureenjs).sort()).toStrictEqual([
    "commit",
    "connect",
    "connectFile",
    "connectMemory",
    "create",
    "db",
    "exists",
    "load",
    "query",
    "setLogging",
    "stage",
    "status"
  ]);
});

test("expect conn, ledger, stage, commit, and query to work", async () => {

  const defaultCtx = {
    id: "@id",
    type: "@type",
    schema: "http://schema.org/",
    rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    rdfs: "http://www.w3.org/2000/01/rdf-schema#",
    wiki: "https://www.wikidata.org/wiki/",
    skos: "http://www.w3.org/2008/05/skos#",
    f: "https://ns.flur.ee/ledger#",
    ex: "http://example.org/ns/"
  };

  const conn = await flureenjs.connectMemory({
    defaults: {
      identity: {
        id: "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
        public:
          "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
        private:
          "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c",
      },
    },
  });

  const ledger = await flureenjs.create(conn, "testledger");

  const db = await flureenjs.db(ledger);

  const db1 = await flureenjs.stage(db, {
    insert: {
      "@context": defaultCtx,
      id: "ex:john",
      "@type": "ex:User",
      "schema:name": "John"
    }
  });

  const results = await flureenjs.query(
    db1,
    {
      "@context": defaultCtx,
      select: { "?s": ["*"] },
      where: {
        "id": "?s",
        "type": "ex:User"
      }
    }
  );


  expect(results).toStrictEqual(
    [
      {
        id: 'ex:john',
        type: 'ex:User',
        'schema:name': "John"
      }
    ]
  );

  // test providing context works and remaps keys
  const contextResults = await flureenjs.query(
    db1,
    {
      "@context": [defaultCtx, { "flhubee": "http://schema.org/name" }],
      select: { "?s": ["*"] },
      where: {
        "id": "?s",
        "type": "ex:User"
      }
    }
  );

  expect(contextResults).toStrictEqual(
    [
      {
        id: 'ex:john',
        type: 'ex:User',
        flhubee: 'John'
      }
    ]
  );


  const db2 = await flureenjs.stage(db, {
    insert: {
      "@context": defaultCtx,
      "@id": "uniqueId",
      foo: "foo",
      bar: "bar",
      "fake:iri/baz": "baz"
    }
  });

  //  await flureenjs.commit(db);

  const results2 = await flureenjs.query(db2, {
    "@context": [defaultCtx, { b: "fake:iri/" }],
    select: { "uniqueId": ["*"] },
  });

  expect(results2).toStrictEqual([
    { "id": "uniqueId", foo: "foo", bar: "bar", "b:baz": "baz" },
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
//    defaults: {
//      context: {
//        id: "@id",
//        type: "@type",
//        schema: "http://schema.org/",
//        rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
//        rdfs: "http://www.w3.org/2000/01/rdf-schema#",
//        wiki: "https://www.wikidata.org/wiki/",
//        skos: "http://www.w3.org/2008/05/skos#",
//        f: "https://ns.flur.ee/ledger#",
//      },
//    },
//  };
//  const conn = await flureenjs.connect(connOpts);
//  const ledgerName = "jld/one";
//  const newLedger = await flureenjs.create(conn, ledgerName);
//  const db0 = flureenjs.db(newLedger);
//  const db1 = await flureenjs.stage(db0, testJson);
//
//  const db2 = await flureenjs.commit(newLedger, db1, {
//    message: "commit!",
//    push: true,
//  });
//  // console.log("DB", db1);
//
//  const results = await flureenjs.query(db2, {
//    select: { "?s": ["id", "isbn", "name", "type"] },
//    where: [["?s", "id", "https://www.wikidata.org/wiki/Q3107329"]],
//  });
//
//  expect(results).toStrictEqual([
//    {
//      id:   "wiki:Q3107329",
//      type: "schema:Book",
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
