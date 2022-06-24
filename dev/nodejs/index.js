const flureenjs = require("@fluree/flureenjs");

const flureeServerUrl = "http://localhost:8090";


// console.log('flureenjs', flureenjs);


async function go() {
  const conn = await flureenjs.jldConnect(
    {method: "ipfs",
     context: {
       id: "@id",
       type: "@type",
       schema: "http://schema.org/",
       rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
       rdfs: "http://www.w3.org/2000/01/rdf-schema#",
       wiki: "https://www.wikidata.org/wiki/",
       skos: "http://www.w3.org/2008/05/skos#",
       f: "https://ns.flur.ee/ledger#"},
     did: {
       id: "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
       "public": "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
       "private": "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"}});

  const ledger = await flureenjs.jldCreate(conn, "dan/test1");

  const q = await flureenjs.jldQuery(
    flureenjs.jldDb(ledger),
    {"select": ["*"],
     "from": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}
  )
  console.log('q', q)

  const q0 = await flureenjs.jldQuery(
    flureenjs.jldDb(ledger),
    {"select": {"?s": ["*", {"f:role": ["*"]}]},
     "where": [["?s", "rdf:type", "f:DID"]]}
  )
  console.log("q0", q0)

  const db1 = await flureenjs.jldStage(
    ledger,
    {"@context":   "https://schema.org",
     "@id":        "https://www.wikidata.org/wiki/Q836821",
     "@type":      ["Movie"],
     "name":       "The Hitchhiker's Guide to the Galaxy",
     "disambiguatingDescription":     "2005 British-American comic science fiction film directed by Garth Jennings",
     "titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
     "isBasedOn":  {"id": "https://www.wikidata.org/wiki/Q3107329",
                  "type": "Book",
                  "name": "The Hitchhiker's Guide to the Galaxy",
                  "isbn": "0-330-25864-8",
                  "author": {"id": "https://www.wikidata.org/wiki/Q42",
                           "type": "Person",
                           "name": "Douglas Adams"}}}
  )

  const q1 = await flureenjs.jldQuery(
    db1,
    { select: ["*", {"schema:isBasedOn": ["*"]}], from: "wiki:Q836821" }
  )
  console.log('q1', q1)

  const db2 = await flureenjs.jldStage(
    ledger,
    {"@context": "https://schema.org",
     "@graph":
     [{"id": "https://www.wikidata.org/wiki/Q836821",
       "name": "NEW TITLE: The Hitchhiker's Guide to the Galaxy",
       "commentCount": 42}]}
  )

  const q2 = await flureenjs.jldQuery(
    db1,
    { select: ["*", {"schema:isBasedOn": ["*"]}], from: "wiki:Q836821" }
  )
  console.log('q2', q2)

  const commit1 = await flureenjs.jldCommit(
    db2,
    { message: "First commit: 2 transactions!", "push?": true}
  )


}


go();
