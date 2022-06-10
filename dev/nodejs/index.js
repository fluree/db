const flureenjs = require("@fluree/flureenjs");

const flureeServerUrl = "http://localhost:8090";
const ledger = "dan/test1";

console.log('flureenjs', flureenjs);

async function go() {

  const conn = await flureenjs.connect(flureeServerUrl);
  const db = await flureenjs.db(conn, ledger);

  const results = await flureenjs.query(db, {select: ["*"], from: "_collection"})

  console.log(results);
};

go();
