//! Bounded fixed-index diagnostic. See the archived runner/report for comparability.
//! Commands: import ROOT INPUT; run MODE ROOT OUTPUT REQUESTS; verify MODE ROOT OUTPUT.
use fluree_db_api::local_journal_ledger::JournalLedger;
use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, IndexConfig, LedgerHandle};
use fluree_db_core::{ContentId, ContentStore};
use fluree_db_cypher::ParamMap;
use fluree_db_transact::{CommitOpts, CommitReceipt};
use serde_json::{json, Value};
use std::{fs, io::Write, path::Path, time::Instant};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const LEDGER: &str = "pokec:main";

fn write_json(path: &Path, value: &Value) -> Result<()> {
    let mut f = fs::File::create(path)?;
    f.write_all(&serde_json::to_vec(value)?)?;
    f.sync_all()?;
    fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}
fn rows(v: &Value) -> Value {
    json!(v["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["row"].clone())
        .collect::<Vec<_>>())
}
fn config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1_000_000,
        reindex_max_bytes: 10_000_000,
    }
}
enum Engine {
    Wal(JournalLedger),
    Ordinary(Fluree, LedgerHandle),
}
impl Engine {
    async fn open(mode: &str, root: &Path) -> Result<Self> {
        Ok(match mode {
            "wal" => Self::Wal(JournalLedger::open(root.into()).await?),
            "ordinary" => {
                let f = FlureeBuilder::file(root.to_string_lossy().into_owned())
                    .without_indexing()
                    .build()?;
                let h = f.ledger_cached(LEDGER).await?;
                Self::Ordinary(f, h)
            }
            _ => return Err("mode must be ordinary or wal".into()),
        })
    }
    async fn query(&self, text: &str, params: Option<&ParamMap>) -> Result<Value> {
        match self {
            Self::Wal(w) => Ok(w.query_cypher(text, params).await?),
            Self::Ordinary(f, h) => {
                let state = h.lock_for_write().await.clone_state();
                let view = GraphDb::from_ledger_state(&state)
                    .with_default_context(f.get_default_context(LEDGER).await?);
                Ok(f.query_cypher_with_params(&view, text, params)
                    .await?
                    .to_cypher_json_async(view.as_graph_db_ref())
                    .await?)
            }
        }
    }
    async fn content(&self, id: &ContentId) -> Result<Vec<u8>> {
        match self {
            Self::Wal(w) => Ok(w.content(id).await?),
            Self::Ordinary(f, _) => Ok(f.content_store(LEDGER).get(id).await?),
        }
    }
    async fn head(&self) -> Result<(ContentId, i64, i64)> {
        match self {
            Self::Wal(w) => {
                let h = w.head().await?.ok_or("no head")?;
                // The fixed index t is recorded from the baseline in the run manifest.
                Ok((h.id.ok_or("no head CID")?, h.t, 0))
            }
            Self::Ordinary(_, h) => {
                let s = h.lock_for_write().await.clone_state();
                Ok((
                    s.ns_record
                        .as_ref()
                        .unwrap()
                        .commit_head_id
                        .clone()
                        .unwrap(),
                    s.t(),
                    s.snapshot.t,
                ))
            }
        }
    }
    async fn transact(
        &self,
        text: &str,
        params: &ParamMap,
    ) -> Result<(CommitReceipt, Option<Value>)> {
        match self {
            Self::Wal(w) => {
                let x = w.transact_cypher(text, Some(params)).await?;
                Ok((x.commit.ok_or("unexpected no-op")?.commit, x.result))
            }
            Self::Ordinary(f, h) => {
                let state = h.lock_for_write().await.clone_state();
                let plan_return =
                    fluree_db_api::cypher_write::plan_write_return_source(text, Some(params))?;
                let skolem = plan_return
                    .as_ref()
                    .map(|_| fluree_db_transact::generate_txn_id());
                let plan = f
                    .cypher_write_plan_with_skolem(
                        text,
                        Some(params),
                        LEDGER,
                        &state.snapshot,
                        skolem.clone(),
                    )
                    .await?;
                let opts = CommitOpts::default().with_raw_txn_spawned(
                    f.content_store(LEDGER),
                    json!({"cypher":text,"params":params}),
                );
                let builder = f.stage(h).index_config(config()).commit_opts(opts);
                let (x, ret) = match plan {
                    fluree_db_api::cypher_write::WritePlan::Single(txn) => {
                        let x = builder.txn(*txn).execute().await?;
                        let s = h.lock_for_write().await.clone_state();
                        let ret = match (&plan_return, &skolem) {
                            (Some(p), Some(id)) => Some(
                                fluree_db_api::cypher_write::write_return_rows(p, id, &s).await?,
                            ),
                            _ => None,
                        };
                        (x, ret)
                    }
                    fluree_db_api::cypher_write::WritePlan::Sequential(plan) => {
                        let x = builder
                            .cypher_sequential(fluree_db_api::cypher_seq::CypherSeqInput {
                                plan: *plan,
                                governance: Default::default(),
                                skolem_txn_id: None,
                            })
                            .execute()
                            .await?;
                        let ret = x.cypher_return.clone();
                        (x, ret)
                    }
                    _ => return Err("frozen diagnostic does not support conditional writes".into()),
                };
                Ok((x.receipt, ret))
            }
        }
    }
}
async fn observations(e: &Engine, requests: &Value) -> Result<Value> {
    let mut values = Vec::new();
    for text in [
        "MATCH (n) RETURN count(n)",
        "MATCH (n:User) RETURN count(n)",
        "MATCH (n:L1) RETURN count(n)",
        "MATCH ()-[r]->() RETURN count(r)",
        "MATCH (n:UserTemp) RETURN n ORDER BY n",
        "MATCH (n:L1) RETURN n ORDER BY n",
        "MATCH (n:L1) RETURN n.p1,n.p2,n.p3,n.p4,n.p5,n.p6,n.p7",
        "MATCH (n:UserTemp) RETURN n.id ORDER BY n.id",
        "MATCH (a)-[:TempEdge]->(b) RETURN a,b ORDER BY a,b",
        "MATCH ()-[r:knows]->() RETURN r.weight",
        "MATCH ()-[r:Temp]->() RETURN r ORDER BY r",
        "MATCH ()-[r:TempEdge]->() RETURN r ORDER BY r",
    ] {
        values.push(json!({"text":text,"rows":rows(&e.query(text,None).await?)}));
    }
    for request in requests
        .as_array()
        .ok_or("requests array")?
        .iter()
        .filter(|r| r["name"] == "update__vertex_on_property")
    {
        let text = "MATCH (n:User {id:$id}) RETURN n.id,n.property";
        let params = request["params"].as_object().unwrap();
        let result = rows(&e.query(text, Some(params)).await?);
        assert_eq!(result, json!([[params["id"], -1]]));
        values.push(json!({"text":text,"params":params,"rows":result}));
    }
    Ok(json!(values))
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize| args.get(i).map(String::as_str).ok_or("missing argument");
    match arg(1)? {
        "query" => {
            let e = Engine::open(arg(2)?, Path::new(arg(3)?)).await?;
            println!("{}", e.query(arg(4)?, None).await?);
        }
        "inspect" => {
            let e = Engine::open(arg(2)?, Path::new(arg(3)?)).await?;
            let id: ContentId = arg(4)?.parse()?;
            let c = fluree_db_core::commit::codec::read_commit(&e.content(&id).await?)?;
            println!("{:?}", c.flakes);
        }
        "import" => {
            let root = Path::new(arg(2)?);
            fs::create_dir(root)?;
            let input = Path::new(arg(3)?);
            let f = FlureeBuilder::file(root.to_string_lossy().into_owned())
                .without_indexing()
                .build()?;
            let converted = root.parent().unwrap().join("import.jsonld");
            let input = if input.extension().is_some_and(|e| e == "cypher") {
                let data = fluree_db_api::cypher_import::cypher_to_jsonld(
                    &fs::read_to_string(input)?,
                    &Default::default(),
                )?;
                fs::write(&converted, serde_json::to_vec(&json!({"@graph":data}))?)?;
                &converted
            } else {
                input
            };
            let start = Instant::now();
            let imported = f.create(LEDGER).import(input).execute().await?;
            if imported.has_annotations {
                f.reindex(LEDGER, Default::default()).await?;
            }
            println!(
                "{}",
                json!({"import_ms":start.elapsed().as_secs_f64()*1000.,"t":imported.t,"root_id":imported.root_id,"has_annotations":imported.has_annotations})
            );
        }
        "run" => {
            let mode = arg(2)?;
            let root = Path::new(arg(3)?);
            let out = Path::new(arg(4)?);
            fs::create_dir(out)?;
            let requests: Value = serde_json::from_slice(&fs::read(arg(5)?)?)?;
            write_json(&out.join("requests.json"), &requests)?;
            let start = Instant::now();
            let e = if mode == "wal" {
                fs::create_dir(root)?;
                Engine::Wal(
                    JournalLedger::bootstrap(
                        root.into(),
                        Path::new(arg(6)?).into(),
                        LEDGER.into(),
                        "diagnostic-1".into(),
                    )
                    .await?,
                )
            } else {
                Engine::open(mode, root).await?
            };
            let load_ms = start.elapsed().as_secs_f64() * 1000.;
            let (baseline, t, _) = e.head().await?;
            let before = rows(&e.query("MATCH (n:User) RETURN count(n)", None).await?);
            write_json(
                &out.join("baseline.json"),
                &json!({"id":baseline,"t":t,"load_ms":load_ms,"users":before,"mode":mode,"fsync":std::env::var("FLUREE_STORAGE_FSYNC").ok()}),
            )?;
            let mut ack_file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(out.join("acks.jsonl"))?;
            let mut prev = baseline;
            for (i, r) in requests
                .as_array()
                .ok_or("requests array")?
                .iter()
                .enumerate()
            {
                let text = r["text"].as_str().ok_or("text")?;
                let params = r["params"].as_object().ok_or("params")?;
                let start = Instant::now();
                let (receipt, ret) = e.transact(text, params).await?;
                // The same JSON response serialization is included on both paths.
                let response = serde_json::to_vec(&ret)?;
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.;
                assert_eq!(receipt.t, t + i as i64 + 1);
                assert!(receipt.flake_count > 0);
                if text.contains("RETURN") {
                    assert_eq!(
                        rows(ret.as_ref().ok_or("missing RETURN")?)
                            .as_array()
                            .unwrap()
                            .len(),
                        1
                    );
                }
                let bytes = e.content(&receipt.commit_id).await?;
                let c = fluree_db_core::commit::codec::read_commit(&bytes)?;
                assert_eq!(c.parents, vec![prev.clone()]);
                let raw_id = c.txn.ok_or("raw request missing")?;
                let raw = e.content(&raw_id).await?;
                assert_eq!(
                    serde_json::from_slice::<Value>(&raw)?,
                    json!({"cypher":text,"params":params})
                );
                assert!(receipt.commit_id.verify(&bytes));
                assert!(raw_id.verify(&raw));
                let ack = json!({"i":i,"name":r["name"],"warmup":r["warmup"],"ms":elapsed_ms,"id":receipt.commit_id,"t":receipt.t,"flakes":receipt.flake_count,"raw_id":raw_id,"commit":bytes,"raw":raw,"response":response});
                serde_json::to_writer(&mut ack_file, &ack)?;
                writeln!(ack_file)?;
                ack_file.sync_all()?;
                prev = receipt.commit_id;
            }
            fs::File::open(out)?.sync_all()?;
            write_json(
                &out.join("observations.json"),
                &observations(&e, &requests).await?,
            )?;
            write_json(
                &out.join("complete.json"),
                &json!({"head":prev,"t":e.head().await?.1,"count":requests.as_array().unwrap().len()}),
            )?;
        }
        "verify" => {
            let e = Engine::open(arg(2)?, Path::new(arg(3)?)).await?;
            let out = Path::new(arg(4)?);
            let requests: Value = serde_json::from_slice(&fs::read(out.join("requests.json"))?)?;
            let baseline: Value = serde_json::from_slice(&fs::read(out.join("baseline.json"))?)?;
            let mut prev: ContentId = serde_json::from_value(baseline["id"].clone())?;
            let mut count = 0;
            for line in fs::read_to_string(out.join("acks.jsonl"))?.lines() {
                let ack: Value = serde_json::from_str(line)?;
                let id: ContentId = serde_json::from_value(ack["id"].clone())?;
                let bytes = e.content(&id).await?;
                assert_eq!(json!(bytes), ack["commit"]);
                assert!(id.verify(&bytes));
                let c = fluree_db_core::commit::codec::read_commit(&bytes)?;
                assert_eq!(c.parents, vec![prev]);
                assert_eq!(c.t, baseline["t"].as_i64().unwrap() + count + 1);
                let raw: ContentId = serde_json::from_value(ack["raw_id"].clone())?;
                assert_eq!(c.txn, Some(raw.clone()));
                assert_eq!(json!(e.content(&raw).await?), ack["raw"]);
                prev = id;
                count += 1;
            }
            assert_eq!(e.head().await?.0, prev);
            assert_eq!(count as usize, requests.as_array().unwrap().len());
            assert_eq!(
                observations(&e, &requests).await?,
                serde_json::from_slice::<Value>(&fs::read(out.join("observations.json"))?)?
            );
            println!("{}", json!({"verified":count,"head":prev}));
        }
        _ => return Err("expected import, run, or verify".into()),
    }
    Ok(())
}
