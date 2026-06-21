```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ n e o 4 j ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-neo4j/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-neo4j/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[NEO4J GRAPH CLIENT FOR STRYKE // CYPHER QUERY + PARAMS + RUN + SCHEMA INTROSPECTION // BOLT]`

> *"Graphs, one stryke pipe at a time."*

Neo4j graph database client for stryke. Parametrized Cypher query and run,
scalar/row helpers, and schema introspection (labels, relationship types,
property keys, indexes, constraints) against any Neo4j 4.x/5.x over the Bolt
protocol — via `neo4rs`, the pure-Rust driver. Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-mssql`](https://github.com/MenkeTechnologies/stryke-mssql)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] Connecting](#0x02-connecting)
- [\[0x03\] Architecture](#0x03-architecture)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Build & test](#0x05-build--test)
- [\[0x06\] License](#0x06-license)

---

## \[0x00\] Install

```sh
s add github.com/MenkeTechnologies/stryke-neo4j
```

On first `use Neo4j`, stryke dlopens the cdylib in-process and registers every
`neo4j__*` export.

---

## \[0x01\] Quick start

```perl
use Neo4j

var %conn = ( uri => "neo4j://localhost:7687", user => "neo4j", password => $ENV{NEO4J_PASS} )

# write with named params ($name placeholders)
Neo4j::run(
    "CREATE (p:Person {id: $id, name: $name})",
    params => { id => 1, name => "Ada" },
    %conn,
)

# query — each row is a hashref keyed by RETURN aliases
val @people = Neo4j::query(
    "MATCH (p:Person) WHERE p.id = $id RETURN p.name AS name",
    params => { id => 1 },
    %conn,
)
p $people[0]{name}        # Ada

# scalar
p Neo4j::scalar("MATCH (p:Person) RETURN count(p) AS n", %conn)

# schema
p Neo4j::labels(%conn)    # [ { label => "Person" }, ... ]
```

---

## \[0x02\] Connecting

`%conn` (or `$NEO4J_URL` as a Bolt-URI fallback):

| Key        | Default            | Notes                                                |
| ---------- | ------------------ | ---------------------------------------------------- |
| `uri`      | `127.0.0.1:7687`   | Bolt URI: `neo4j://`, `neo4j+s://`, `bolt://`, …      |
| `user`     | `neo4j`            |                                                      |
| `password` | —                  |                                                      |
| `database` | server default     | For multi-database servers                           |

A `neo4rs` `Graph` (connection pool) is cached per `(uri, user, db)`; a pool
whose call errors is evicted and reopened on the next call.

---

## \[0x03\] Architecture

- **Driver** — [`neo4rs`](https://docs.rs/neo4rs), the pure-Rust Bolt protocol
  client. No JVM, no official-driver FFI.
- **Blocking facade** — neo4rs is async, so the cdylib owns one tokio runtime
  and `block_on`s each call, matching the sync model the other stryke data
  packages use.
- **Rows as JSON** — neo4rs deserializes each Bolt row straight into JSON, so a
  returned node or relationship becomes its property map and arbitrary results
  round-trip without a per-query schema.
- **Parametrized Cypher** — `$name` placeholders are bound from `params`, so
  values never enter the query text — no string interpolation, no injection.

---

## \[0x04\] API reference

| Group         | Functions                                                              |
| ------------- | ---------------------------------------------------------------------- |
| Liveness      | `version`, `ping`, `server_info`                                       |
| Query         | `query`, `query_one`, `scalar`, `query_values`                         |
| Write         | `run`, `batch` (transaction)                                           |
| Graph helpers | `create_node`, `merge_node`, `create_rel`, `merge_rel`, `delete_rel`, `delete_nodes`, `node_count` |
| Schema        | `create_index`, `create_constraint`, `drop_index`, `drop_constraint`   |
| Introspection | `labels`, `relationship_types`, `property_keys`, `indexes`, `constraints` |
| Planning      | `explain`, `profile`                                                   |
| Cypher helpers| `escape`, `quote_literal`, `quote_ident`, `valid_identifier`, `format_value` |
| URL helpers   | `parse_url`, `redact_url`, `build_url`                                 |

The graph/schema convenience helpers take scalar properties and safely
backtick-quote labels, relationship types, and property names (which Cypher
can't parametrize); use `run` with hand-written Cypher for list/map properties
or anything more elaborate.

---

## \[0x05\] Build & test

```sh
make debug       # cargo build
make test        # cargo test, then `s test t/` (live needs $NEO4J_URL)
make install     # s pkg install -g .
```

`cargo test` runs the in-crate unit tests (Bolt-URI parse/redact, defaults,
params) with no database. Point `$NEO4J_URL` (+ user/pass) at a throwaway Neo4j
container to exercise the query path.

---

## \[0x06\] License

MIT &middot; MenkeTechnologies
