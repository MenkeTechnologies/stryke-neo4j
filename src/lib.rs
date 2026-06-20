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

use anyhow::{anyhow, Result};
use neo4rs::{query, ConfigBuilder, Graph, Query};
use once_cell::sync::OnceCell;
use serde_json::{json, Value};
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
}
