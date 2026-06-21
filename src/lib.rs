//! stryke-neo4j — Neo4j graph cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn neo4j__*` is a JSON-string-in /
//! JSON-string-out wrapper around `neo4rs` (the pure-Rust Bolt driver).
//! neo4rs is async, so this cdylib owns one multi-thread tokio runtime and
//! presents a **blocking facade**: every handler `block_on`s the async call.
//! A `Graph` (a connection pool) is cached per `(uri, user, db)` for the life
//! of the stryke process; a pool whose call errors is evicted so the next call
//! reconnects.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

use anyhow::{anyhow, bail, Result};
use neo4rs::{query, ConfigBuilder, Graph, Query};
use once_cell::sync::OnceCell;
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

// ── runtime + graph cache ───────────────────────────────────────────────────

static RT: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("build tokio runtime"))
}

static GRAPHS: OnceCell<std::sync::Mutex<HashMap<ConnKey, Graph>>> = OnceCell::new();

fn graphs() -> &'static std::sync::Mutex<HashMap<ConnKey, Graph>> {
    GRAPHS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ConnKey {
    uri: String,
    user: String,
    password: String,
    db: String,
}

fn conn_key(opts: &Value) -> ConnKey {
    ConnKey {
        uri: opts
            .get("uri")
            .and_then(|v| v.as_str())
            .unwrap_or("127.0.0.1:7687")
            .to_string(),
        user: opts
            .get("user")
            .and_then(|v| v.as_str())
            .unwrap_or("neo4j")
            .to_string(),
        password: opts
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        db: opts
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    }
}

async fn connect(key: &ConnKey) -> Result<Graph> {
    let mut builder = ConfigBuilder::default()
        .uri(&key.uri)
        .user(&key.user)
        .password(&key.password);
    if !key.db.is_empty() {
        builder = builder.db(key.db.clone());
    }
    let config = builder.build().map_err(|e| anyhow!("config: {e}"))?;
    Graph::connect(config)
        .await
        .map_err(|e| anyhow!("connect {}: {e}", key.uri))
}

/// Get (or open) the cached graph for these opts and run `f`. On error the pool
/// is evicted so a later call reconnects.
fn with_graph<T, F, Fut>(opts: &Value, f: F) -> Result<T>
where
    F: FnOnce(Graph) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let key = conn_key(opts);
    rt().block_on(async {
        let graph = {
            let existing = graphs().lock().unwrap().get(&key).cloned();
            match existing {
                Some(g) => g,
                None => {
                    let g = connect(&key).await?;
                    graphs().lock().unwrap().insert(key.clone(), g.clone());
                    g
                }
            }
        };
        let out = f(graph).await;
        if out.is_err() {
            graphs().lock().unwrap().remove(&key);
        }
        out
    })
}

// ── query building + row conversion ─────────────────────────────────────────

/// Build a parametrized Cypher query. `$name` placeholders are bound from a
/// JSON `params` object — values never enter the query text.
fn build_query(cypher: &str, params: &Value) -> Query {
    let mut q = query(cypher);
    if let Some(obj) = params.as_object() {
        for (k, val) in obj {
            q = match val {
                Value::Bool(b) => q.param(k, *b),
                Value::Number(n) if n.is_i64() => q.param(k, n.as_i64().unwrap()),
                Value::Number(n) if n.is_u64() => q.param(k, n.as_u64().unwrap() as i64),
                Value::Number(n) => q.param(k, n.as_f64().unwrap()),
                Value::String(s) => q.param(k, s.clone()),
                Value::Null => q,
                other => q.param(k, other.to_string()),
            };
        }
    }
    q
}

fn params_of(v: &Value) -> Value {
    v.get("params").cloned().unwrap_or(Value::Null)
}

/// Convert a Bolt row to a JSON object keyed by the RETURN aliases. neo4rs
/// deserializes the row (including nodes/relationships → their properties)
/// straight into `serde_json::Value`.
fn row_to_json(row: &neo4rs::Row) -> Value {
    row.to::<Value>().unwrap_or(Value::Null)
}

async fn run_query(graph: &Graph, cypher: &str, params: &Value) -> Result<Vec<Value>> {
    let mut stream = graph
        .execute(build_query(cypher, params))
        .await
        .map_err(|e| anyhow!("execute: {e}"))?;
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await.map_err(|e| anyhow!("read rows: {e}"))? {
        rows.push(row_to_json(&row));
    }
    Ok(rows)
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-neo4j handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
/// `p` must be a pointer previously returned by an export, or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── version + liveness ──────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn neo4j__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn neo4j__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let rows = with_graph(&v, |g| async move {
            run_query(&g, "RETURN 1 AS ok", &Value::Null).await
        })?;
        Ok(json!({ "value": !rows.is_empty() }))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__server_info(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let rows = with_graph(&v, |g| async move {
            run_query(
                &g,
                "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition",
                &Value::Null,
            )
            .await
        })?;
        Ok(json!({ "rows": rows }))
    })
}

// ── query / run ─────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn neo4j__query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cypher = v["cypher"]
            .as_str()
            .ok_or_else(|| anyhow!("missing cypher"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_graph(&v, |g| async move { run_query(&g, &cypher, &params).await })?;
        Ok(json!({ "rows": rows }))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__query_one(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cypher = v["cypher"]
            .as_str()
            .ok_or_else(|| anyhow!("missing cypher"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_graph(&v, |g| async move { run_query(&g, &cypher, &params).await })?;
        Ok(json!({ "row": rows.into_iter().next() }))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__scalar(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cypher = v["cypher"]
            .as_str()
            .ok_or_else(|| anyhow!("missing cypher"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_graph(&v, |g| async move { run_query(&g, &cypher, &params).await })?;
        let value = rows
            .first()
            .and_then(|r| r.as_object())
            .and_then(|o| o.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({ "value": value }))
    })
}

/// Run a write/DDL statement that returns no rows. Returns `{ ok: true }`.
#[no_mangle]
pub extern "C" fn neo4j__run(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cypher = v["cypher"]
            .as_str()
            .ok_or_else(|| anyhow!("missing cypher"))?
            .to_string();
        let params = params_of(&v);
        with_graph(&v, |g| async move {
            g.run(build_query(&cypher, &params))
                .await
                .map_err(|e| anyhow!("run: {e}"))
        })?;
        Ok(json!({ "ok": true }))
    })
}

// ── schema introspection ────────────────────────────────────────────────────

fn introspect(v: &Value, cypher: &'static str) -> Result<Value> {
    let rows = with_graph(
        v,
        |g| async move { run_query(&g, cypher, &Value::Null).await },
    )?;
    Ok(json!({ "rows": rows }))
}

#[no_mangle]
pub extern "C" fn neo4j__labels(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        introspect(
            &v,
            "CALL db.labels() YIELD label RETURN label ORDER BY label",
        )
    })
}

#[no_mangle]
pub extern "C" fn neo4j__relationship_types(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        introspect(
            &v,
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType",
        )
    })
}

#[no_mangle]
pub extern "C" fn neo4j__property_keys(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        introspect(
            &v,
            "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey",
        )
    })
}

#[no_mangle]
pub extern "C" fn neo4j__indexes(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| introspect(&v, "SHOW INDEXES"))
}

#[no_mangle]
pub extern "C" fn neo4j__constraints(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| introspect(&v, "SHOW CONSTRAINTS"))
}

// ── transactions ────────────────────────────────────────────────────────────

/// Run a list of `{ cypher, params? }` steps inside one transaction. Commits on
/// success, rolls back if any step errors. Returns `{ ok, steps }`.
#[no_mangle]
pub extern "C" fn neo4j__batch(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let steps: Vec<(String, Value)> = v
            .get("steps")
            .and_then(|s| s.as_array())
            .ok_or_else(|| anyhow!("missing steps array"))?
            .iter()
            .map(|s| {
                let c = s
                    .get("cypher")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let p = s.get("params").cloned().unwrap_or(Value::Null);
                (c, p)
            })
            .collect();
        let n = with_graph(&v, |g| async move {
            let mut txn = g.start_txn().await.map_err(|e| anyhow!("begin txn: {e}"))?;
            for (cypher, params) in &steps {
                if let Err(e) = txn.run(build_query(cypher, params)).await {
                    let _ = txn.rollback().await;
                    return Err(anyhow!("step failed (rolled back): {e}"));
                }
            }
            txn.commit().await.map_err(|e| anyhow!("commit: {e}"))?;
            Ok(steps.len())
        })?;
        Ok(json!({ "ok": true, "steps": n }))
    })
}

/// Like `query`, but return a flat list of the first column's values across all
/// rows (handy for collecting ids / names).
#[no_mangle]
pub extern "C" fn neo4j__query_values(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cypher = v["cypher"]
            .as_str()
            .ok_or_else(|| anyhow!("missing cypher"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_graph(&v, |g| async move { run_query(&g, &cypher, &params).await })?;
        let values: Vec<Value> = rows
            .into_iter()
            .map(|r| {
                r.as_object()
                    .and_then(|o| o.values().next().cloned())
                    .unwrap_or(Value::Null)
            })
            .collect();
        Ok(json!({ "values": values }))
    })
}

// ── node / relationship convenience (scalar properties) ─────────────────────

/// CREATE a node with a label and a scalar-property map. Returns the node.
#[no_mangle]
pub extern "C" fn neo4j__create_node(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let label = quote_ident(
            v["label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing label"))?,
        );
        let (clause, params) = flatten_props(v.get("props").unwrap_or(&Value::Null), "np")?;
        let cypher = format!("CREATE (n:{label} {clause}) RETURN n");
        let rows = with_graph(&v, |g| async move {
            run_query(&g, &cypher, &Value::Object(params)).await
        })?;
        Ok(json!({ "node": rows.into_iter().next() }))
    })
}

/// MERGE a node on a single scalar `key` = `value`, then `SET n += props`.
/// Returns the node.
#[no_mangle]
pub extern "C" fn neo4j__merge_node(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let label = quote_ident(
            v["label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing label"))?,
        );
        let key = quote_ident(v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?);
        let value = v
            .get("value")
            .cloned()
            .ok_or_else(|| anyhow!("missing value"))?;
        scalar_or_err(&value, "value")?;
        let mut params = Map::new();
        params.insert("k".into(), value);
        let mut cypher = format!("MERGE (n:{label} {{{key}: $k}})");
        if let Some(props) = v.get("props").filter(|p| !p.is_null()) {
            let (clause, p) = flatten_props(props, "np")?;
            params.extend(p);
            cypher.push_str(&format!(" SET n += {clause}"));
        }
        cypher.push_str(" RETURN n");
        let rows = with_graph(&v, |g| async move {
            run_query(&g, &cypher, &Value::Object(params)).await
        })?;
        Ok(json!({ "node": rows.into_iter().next() }))
    })
}

/// MATCH a node by `(label, key, value)` and SET its scalar properties
/// (`n += props`) WITHOUT creating it if absent — the update-only counterpart to
/// `merge_node`. Returns the updated node, or null if nothing matched.
#[no_mangle]
pub extern "C" fn neo4j__set_props(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let label = quote_ident(
            v["label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing label"))?,
        );
        let key = quote_ident(v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?);
        let value = v
            .get("value")
            .cloned()
            .ok_or_else(|| anyhow!("missing value"))?;
        scalar_or_err(&value, "value")?;
        let props = v
            .get("props")
            .filter(|p| !p.is_null())
            .ok_or_else(|| anyhow!("missing props"))?;
        let (clause, p) = flatten_props(props, "np")?;
        let mut params = Map::new();
        params.insert("k".into(), value);
        params.extend(p);
        let cypher = format!("MATCH (n:{label} {{{key}: $k}}) SET n += {clause} RETURN n");
        let rows = with_graph(&v, |g| async move {
            run_query(&g, &cypher, &Value::Object(params)).await
        })?;
        Ok(json!({ "node": rows.into_iter().next() }))
    })
}

/// MATCH two nodes by `(label, key, value)` each and CREATE a typed relationship
/// between them with optional scalar properties. Returns the relationship.
#[no_mangle]
pub extern "C" fn neo4j__create_rel(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let from_label = quote_ident(
            v["from_label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing from_label"))?,
        );
        let from_key = quote_ident(
            v["from_key"]
                .as_str()
                .ok_or_else(|| anyhow!("missing from_key"))?,
        );
        let to_label = quote_ident(
            v["to_label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing to_label"))?,
        );
        let to_key = quote_ident(
            v["to_key"]
                .as_str()
                .ok_or_else(|| anyhow!("missing to_key"))?,
        );
        let rel_type = quote_ident(v["type"].as_str().ok_or_else(|| anyhow!("missing type"))?);
        let from_val = v
            .get("from_value")
            .cloned()
            .ok_or_else(|| anyhow!("missing from_value"))?;
        let to_val = v
            .get("to_value")
            .cloned()
            .ok_or_else(|| anyhow!("missing to_value"))?;
        scalar_or_err(&from_val, "from_value")?;
        scalar_or_err(&to_val, "to_value")?;
        let mut params = Map::new();
        params.insert("fv".into(), from_val);
        params.insert("tv".into(), to_val);
        let mut props_clause = String::new();
        if let Some(props) = v.get("props").filter(|p| !p.is_null()) {
            let (clause, p) = flatten_props(props, "rp")?;
            params.extend(p);
            props_clause = format!(" {clause}");
        }
        let cypher = format!(
            "MATCH (a:{from_label} {{{from_key}: $fv}}), (b:{to_label} {{{to_key}: $tv}}) \
             CREATE (a)-[r:{rel_type}{props_clause}]->(b) RETURN r"
        );
        let rows = with_graph(&v, |g| async move {
            run_query(&g, &cypher, &Value::Object(params)).await
        })?;
        Ok(json!({ "relationship": rows.into_iter().next() }))
    })
}

/// MATCH two nodes by `(label, key, value)` each and MERGE a typed relationship
/// between them (idempotent — created once, reused after), optionally SETting
/// scalar properties. Returns the relationship.
#[no_mangle]
pub extern "C" fn neo4j__merge_rel(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let from_label = quote_ident(
            v["from_label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing from_label"))?,
        );
        let from_key = quote_ident(
            v["from_key"]
                .as_str()
                .ok_or_else(|| anyhow!("missing from_key"))?,
        );
        let to_label = quote_ident(
            v["to_label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing to_label"))?,
        );
        let to_key = quote_ident(
            v["to_key"]
                .as_str()
                .ok_or_else(|| anyhow!("missing to_key"))?,
        );
        let rel_type = quote_ident(v["type"].as_str().ok_or_else(|| anyhow!("missing type"))?);
        let from_val = v
            .get("from_value")
            .cloned()
            .ok_or_else(|| anyhow!("missing from_value"))?;
        let to_val = v
            .get("to_value")
            .cloned()
            .ok_or_else(|| anyhow!("missing to_value"))?;
        scalar_or_err(&from_val, "from_value")?;
        scalar_or_err(&to_val, "to_value")?;
        let mut params = Map::new();
        params.insert("fv".into(), from_val);
        params.insert("tv".into(), to_val);
        let mut set_clause = String::new();
        if let Some(props) = v.get("props").filter(|p| !p.is_null()) {
            let (clause, p) = flatten_props(props, "rp")?;
            params.extend(p);
            set_clause = format!(" SET r += {clause}");
        }
        let cypher = format!(
            "MATCH (a:{from_label} {{{from_key}: $fv}}), (b:{to_label} {{{to_key}: $tv}}) \
             MERGE (a)-[r:{rel_type}]->(b){set_clause} RETURN r"
        );
        let rows = with_graph(&v, |g| async move {
            run_query(&g, &cypher, &Value::Object(params)).await
        })?;
        Ok(json!({ "relationship": rows.into_iter().next() }))
    })
}

/// MATCH a typed relationship between two nodes (by `(label, key, value)` each)
/// and DELETE it. Returns `{ ok, deleted }` with the count removed.
#[no_mangle]
pub extern "C" fn neo4j__delete_rel(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let from_label = quote_ident(
            v["from_label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing from_label"))?,
        );
        let from_key = quote_ident(
            v["from_key"]
                .as_str()
                .ok_or_else(|| anyhow!("missing from_key"))?,
        );
        let to_label = quote_ident(
            v["to_label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing to_label"))?,
        );
        let to_key = quote_ident(
            v["to_key"]
                .as_str()
                .ok_or_else(|| anyhow!("missing to_key"))?,
        );
        let rel_type = quote_ident(v["type"].as_str().ok_or_else(|| anyhow!("missing type"))?);
        let from_val = v
            .get("from_value")
            .cloned()
            .ok_or_else(|| anyhow!("missing from_value"))?;
        let to_val = v
            .get("to_value")
            .cloned()
            .ok_or_else(|| anyhow!("missing to_value"))?;
        scalar_or_err(&from_val, "from_value")?;
        scalar_or_err(&to_val, "to_value")?;
        let mut params = Map::new();
        params.insert("fv".into(), from_val);
        params.insert("tv".into(), to_val);
        let cypher = format!(
            "MATCH (a:{from_label} {{{from_key}: $fv}})-[r:{rel_type}]->(b:{to_label} {{{to_key}: $tv}}) \
             DELETE r RETURN count(r) AS deleted"
        );
        let rows = with_graph(&v, |g| async move {
            run_query(&g, &cypher, &Value::Object(params)).await
        })?;
        let deleted = rows
            .first()
            .and_then(|r| r.get("deleted").cloned())
            .unwrap_or(json!(0));
        Ok(json!({ "ok": true, "deleted": deleted }))
    })
}

/// DETACH DELETE nodes of a label, optionally narrowed by a scalar-property
/// match. Returns `{ ok }`.
#[no_mangle]
pub extern "C" fn neo4j__delete_nodes(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let label = quote_ident(
            v["label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing label"))?,
        );
        let (clause, params) = match v.get("match").filter(|m| !m.is_null()) {
            Some(m) => flatten_props(m, "mp")?,
            None => (String::new(), Map::new()),
        };
        let cypher = format!("MATCH (n:{label} {clause}) DETACH DELETE n");
        with_graph(&v, |g| async move {
            g.run(build_query(&cypher, &Value::Object(params)))
                .await
                .map_err(|e| anyhow!("delete: {e}"))
        })?;
        Ok(json!({ "ok": true }))
    })
}

/// Count nodes, optionally of a single label.
#[no_mangle]
pub extern "C" fn neo4j__node_count(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let pattern = match v.get("label").and_then(|x| x.as_str()) {
            Some(l) => format!("(n:{})", quote_ident(l)),
            None => "(n)".to_string(),
        };
        let cypher = format!("MATCH {pattern} RETURN count(n) AS count");
        let rows = with_graph(
            &v,
            |g| async move { run_query(&g, &cypher, &Value::Null).await },
        )?;
        let count = rows
            .first()
            .and_then(|r| r.get("count"))
            .cloned()
            .unwrap_or(json!(0));
        Ok(json!({ "value": count }))
    })
}

// ── index / constraint management ───────────────────────────────────────────

#[no_mangle]
pub extern "C" fn neo4j__create_index(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let label = quote_ident(
            v["label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing label"))?,
        );
        let property = quote_ident(
            v["property"]
                .as_str()
                .ok_or_else(|| anyhow!("missing property"))?,
        );
        let name = v
            .get("name")
            .and_then(|x| x.as_str())
            .map(quote_ident)
            .unwrap_or_default();
        let cypher = format!("CREATE INDEX {name} IF NOT EXISTS FOR (n:{label}) ON (n.{property})");
        with_graph(&v, |g| async move {
            g.run(query(&cypher))
                .await
                .map_err(|e| anyhow!("create index: {e}"))
        })?;
        Ok(json!({ "ok": true }))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__create_constraint(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let label = quote_ident(
            v["label"]
                .as_str()
                .ok_or_else(|| anyhow!("missing label"))?,
        );
        let property = quote_ident(
            v["property"]
                .as_str()
                .ok_or_else(|| anyhow!("missing property"))?,
        );
        let name = v
            .get("name")
            .and_then(|x| x.as_str())
            .map(quote_ident)
            .unwrap_or_default();
        // default is a uniqueness constraint
        let cypher = format!(
            "CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property} IS UNIQUE"
        );
        with_graph(&v, |g| async move {
            g.run(query(&cypher))
                .await
                .map_err(|e| anyhow!("create constraint: {e}"))
        })?;
        Ok(json!({ "ok": true }))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__drop_index(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = quote_ident(v["name"].as_str().ok_or_else(|| anyhow!("missing name"))?);
        let cypher = format!("DROP INDEX {name} IF EXISTS");
        with_graph(&v, |g| async move {
            g.run(query(&cypher))
                .await
                .map_err(|e| anyhow!("drop index: {e}"))
        })?;
        Ok(json!({ "ok": true }))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__drop_constraint(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = quote_ident(v["name"].as_str().ok_or_else(|| anyhow!("missing name"))?);
        let cypher = format!("DROP CONSTRAINT {name} IF EXISTS");
        with_graph(&v, |g| async move {
            g.run(query(&cypher))
                .await
                .map_err(|e| anyhow!("drop constraint: {e}"))
        })?;
        Ok(json!({ "ok": true }))
    })
}

// ── query planning ──────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn neo4j__explain(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cypher = format!(
            "EXPLAIN {}",
            v["cypher"]
                .as_str()
                .ok_or_else(|| anyhow!("missing cypher"))?
        );
        let params = params_of(&v);
        let rows = with_graph(&v, |g| async move { run_query(&g, &cypher, &params).await })?;
        Ok(json!({ "rows": rows }))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__profile(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cypher = format!(
            "PROFILE {}",
            v["cypher"]
                .as_str()
                .ok_or_else(|| anyhow!("missing cypher"))?
        );
        let params = params_of(&v);
        let rows = with_graph(&v, |g| async move { run_query(&g, &cypher, &params).await })?;
        Ok(json!({ "rows": rows }))
    })
}

// ── pure URL helpers ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn neo4j__parse_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = v["url"].as_str().ok_or_else(|| anyhow!("missing url"))?;
        Ok(parse_bolt(url))
    })
}

#[no_mangle]
pub extern "C" fn neo4j__redact_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = v["url"].as_str().ok_or_else(|| anyhow!("missing url"))?;
        Ok(json!({ "value": redact_bolt(url) }))
    })
}

/// Backtick-quote a Cypher identifier (label / type / property / index name).
#[no_mangle]
pub extern "C" fn neo4j__quote_identifier(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = v["value"]
            .as_str()
            .ok_or_else(|| anyhow!("missing value"))?;
        Ok(json!({ "value": quote_ident(s) }))
    })
}

/// Escape a string for a single-quoted Cypher string literal.
#[no_mangle]
pub extern "C" fn neo4j__escape_string(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = v["value"]
            .as_str()
            .ok_or_else(|| anyhow!("missing value"))?;
        Ok(json!({ "value": escape_string(s) }))
    })
}

/// Wrap a string as a single-quoted Cypher string literal.
#[no_mangle]
pub extern "C" fn neo4j__quote_literal(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = v["value"]
            .as_str()
            .ok_or_else(|| anyhow!("missing value"))?;
        Ok(json!({ "value": quote_literal(s) }))
    })
}

/// Format a JSON value as a Cypher literal (string/number/bool/null/list/map).
#[no_mangle]
pub extern "C" fn neo4j__format_value(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let val = v.get("value").ok_or_else(|| anyhow!("missing value"))?;
        Ok(json!({ "value": format_value(val) }))
    })
}

/// True when a string is a valid unquoted Cypher identifier.
#[no_mangle]
pub extern "C" fn neo4j__valid_identifier(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = v["value"]
            .as_str()
            .ok_or_else(|| anyhow!("missing value"))?;
        Ok(json!({ "valid": valid_identifier(s) }))
    })
}

/// Build a Bolt URI from a components map (inverse of parse_url).
#[no_mangle]
pub extern "C" fn neo4j__build_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let parts = v
            .get("parts")
            .and_then(|x| x.as_object())
            .ok_or_else(|| anyhow!("missing parts object"))?;
        Ok(json!({ "value": build_bolt(parts) }))
    })
}

// ── pure logic (unit-tested) ────────────────────────────────────────────────

/// Decompose a Bolt URI (`neo4j[+s]://user:pass@host:7687`) into parts.
fn parse_bolt(url: &str) -> Value {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (s.to_string(), r),
        None => ("neo4j".to_string(), url),
    };
    let (authority, _path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, String::new()),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (username, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, _)) => (Some(u.to_string()), Some("***".to_string())),
            None => (Some(ui.to_string()), None),
        },
        None => (None, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<i64>().ok()),
        None => (hostport.to_string(), None),
    };
    json!({
        "scheme": scheme,
        "host": host,
        "port": port.unwrap_or(7687),
        "username": username,
        "password": password,
        "tls": scheme.ends_with("+s") || scheme.ends_with("+ssc"),
    })
}

fn redact_bolt(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((userinfo, host)) => {
                let user = userinfo.split_once(':').map(|(u, _)| u).unwrap_or(userinfo);
                format!("{scheme}://{user}:***@{host}")
            }
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

/// Backtick-quote a Cypher identifier (label / relationship type / property /
/// index name), escaping internal backticks. Labels and types cannot be
/// parametrized in Cypher, so the convenience helpers interpolate them — this
/// keeps that safe against injection.
fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Escape a string for a single-quoted Cypher string literal (`\` and `'`).
fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Wrap a string as a single-quoted Cypher string literal.
fn quote_literal(s: &str) -> String {
    format!("'{}'", escape_string(s))
}

/// Format a JSON value as a Cypher literal: string→`'...'`, number→as-is,
/// bool→`true`/`false`, null→`null`, array→`[...]`, object→`{`k`: v}`.
fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_literal(s),
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(format_value).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(o) => {
            let inner = o
                .iter()
                .map(|(k, val)| format!("{}: {}", quote_ident(k), format_value(val)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{inner}}}")
        }
    }
}

/// A Cypher identifier is safe unquoted when it matches `[A-Za-z_][A-Za-z0-9_]*`.
fn valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Build a Bolt URI from a components map (inverse of `parse_bolt`): keys
/// `scheme` (default `neo4j`), `host` (default `localhost`), `port`, `username`,
/// `password`.
fn build_bolt(v: &Map<String, Value>) -> String {
    let scheme = v.get("scheme").and_then(|x| x.as_str()).unwrap_or("neo4j");
    let host = v
        .get("host")
        .and_then(|x| x.as_str())
        .unwrap_or("localhost");
    let mut out = format!("{scheme}://");
    if let Some(user) = v.get("username").and_then(|x| x.as_str()) {
        out.push_str(user);
        if let Some(pass) = v.get("password").and_then(|x| x.as_str()) {
            out.push(':');
            out.push_str(pass);
        }
        out.push('@');
    }
    out.push_str(host);
    if let Some(port) = v.get("port").and_then(|x| x.as_u64()) {
        out.push_str(&format!(":{port}"));
    }
    out
}

fn scalar_or_err(v: &Value, what: &str) -> Result<()> {
    if v.is_object() || v.is_array() {
        bail!("{what} must be a scalar (string / number / bool / null)");
    }
    Ok(())
}

/// Flatten a JSON object of scalar properties into a Cypher property-map clause
/// `{`k`: $p0, …}` plus the named scalar params (`<prefix>0`, `<prefix>1`, …).
/// Errors on a non-scalar value — Neo4j property values are scalars or lists of
/// scalars; for lists/maps use `run` with hand-written Cypher.
fn flatten_props(props: &Value, prefix: &str) -> Result<(String, Map<String, Value>)> {
    let obj = props
        .as_object()
        .ok_or_else(|| anyhow!("props must be an object"))?;
    let mut clauses = Vec::with_capacity(obj.len());
    let mut params = Map::new();
    for (i, (k, val)) in obj.iter().enumerate() {
        scalar_or_err(val, &format!("property {k:?}"))?;
        let pname = format!("{prefix}{i}");
        clauses.push(format!("{}: ${}", quote_ident(k), pname));
        params.insert(pname, val.clone());
    }
    Ok((format!("{{{}}}", clauses.join(", ")), params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_key_defaults() {
        let k = conn_key(&json!({}));
        assert_eq!(k.uri, "127.0.0.1:7687");
        assert_eq!(k.user, "neo4j");
        assert!(k.db.is_empty());
    }

    #[test]
    fn parse_bolt_full() {
        let v = parse_bolt("neo4j+s://alice:secret@graph.example.com:7687");
        assert_eq!(v["scheme"], "neo4j+s");
        assert_eq!(v["host"], "graph.example.com");
        assert_eq!(v["port"], 7687);
        assert_eq!(v["username"], "alice");
        assert_eq!(v["password"], "***");
        assert_eq!(v["tls"], true);
    }

    #[test]
    fn parse_bolt_bare_host() {
        let v = parse_bolt("localhost:7687");
        assert_eq!(v["host"], "localhost");
        assert_eq!(v["port"], 7687);
        assert_eq!(v["username"], Value::Null);
        assert_eq!(v["tls"], false);
    }

    #[test]
    fn redact_bolt_hides_password() {
        assert_eq!(
            redact_bolt("neo4j://alice:secret@host:7687"),
            "neo4j://alice:***@host:7687"
        );
        assert_eq!(redact_bolt("bolt://host:7687"), "bolt://host:7687");
    }

    #[test]
    fn params_passthrough() {
        let v = json!({"cypher": "X", "params": {"id": 5}});
        assert_eq!(params_of(&v), json!({"id": 5}));
        assert_eq!(params_of(&json!({"cypher": "X"})), Value::Null);
    }

    #[test]
    fn quote_ident_escapes_backticks() {
        assert_eq!(quote_ident("Person"), "`Person`");
        assert_eq!(quote_ident("weird`label"), "`weird``label`");
        // an injection attempt stays inside the backtick quoting
        assert_eq!(
            quote_ident("P) DETACH DELETE n //"),
            "`P) DETACH DELETE n //`"
        );
    }

    #[test]
    fn flatten_props_builds_clause_and_params() {
        let (clause, params) = flatten_props(&json!({"name": "Ada", "age": 36}), "np").unwrap();
        assert!(clause.starts_with('{') && clause.ends_with('}'));
        assert!(clause.contains("`name`: $np0"));
        assert!(clause.contains("`age`: $np1"));
        assert_eq!(params["np0"], json!("Ada"));
        assert_eq!(params["np1"], json!(36));
    }

    #[test]
    fn flatten_props_rejects_nested() {
        assert!(flatten_props(&json!({"k": {"nested": 1}}), "np").is_err());
        assert!(flatten_props(&json!({"k": [1, 2]}), "np").is_err());
        assert!(flatten_props(&json!([]), "np").is_err());
    }

    #[test]
    fn scalar_or_err_accepts_scalars_only() {
        assert!(scalar_or_err(&json!("x"), "v").is_ok());
        assert!(scalar_or_err(&json!(1), "v").is_ok());
        assert!(scalar_or_err(&json!(null), "v").is_ok());
        assert!(scalar_or_err(&json!({"a": 1}), "v").is_err());
        assert!(scalar_or_err(&json!([1]), "v").is_err());
    }

    #[test]
    fn escape_and_quote_literal() {
        assert_eq!(escape_string("a'b"), "a\\'b");
        assert_eq!(escape_string("c\\d"), "c\\\\d");
        assert_eq!(quote_literal("O'Brien"), "'O\\'Brien'");
    }

    #[test]
    fn format_value_cypher() {
        assert_eq!(format_value(&json!(7)), "7");
        assert_eq!(format_value(&json!(true)), "true");
        assert_eq!(format_value(&Value::Null), "null");
        assert_eq!(format_value(&json!("x")), "'x'");
        assert_eq!(format_value(&json!([1, 2])), "[1, 2]");
        assert_eq!(format_value(&json!({"a": 1})), "{`a`: 1}");
    }

    #[test]
    fn valid_identifier_rules() {
        assert!(valid_identifier("n1"));
        assert!(valid_identifier("_x"));
        assert!(!valid_identifier("1n"));
        assert!(!valid_identifier("a-b"));
    }

    #[test]
    fn build_bolt_roundtrips() {
        let parts = json!({"scheme": "neo4j", "host": "db", "port": 7687, "username": "neo4j", "password": "pw"});
        assert_eq!(
            build_bolt(parts.as_object().unwrap()),
            "neo4j://neo4j:pw@db:7687"
        );
        assert_eq!(
            build_bolt(json!({"host": "h"}).as_object().unwrap()),
            "neo4j://h"
        );
    }
}
