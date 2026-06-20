//! Integration-test placeholder.
//!
//! `stryke-neo4j` is a `cdylib`-only crate (no `rlib`), so an integration test
//! cannot link its `extern "C"` exports. The real coverage is:
//!
//!   * `src/lib.rs` `#[cfg(test)] mod tests` — unit tests for the pure logic
//!     (Bolt-URI parse/redact, connection defaults, param passthrough), which
//!     run on `cargo test`.
//!   * `t/test_stryke_neo4j_surface.stk` — pins every `Neo4j::*` wrapper and the
//!     URL helpers, with no database.
//!   * `t/test_neo4j.stk` — Cypher query/run against a live Neo4j (`$NEO4J_URL`),
//!     short-circuited when none is set.

#[test]
fn cdylib_crate_compiles() {
    // Reaching here means every `extern "C"` `neo4j__*` export type-checked and
    // linked into the test harness — the minimum contract for a cdylib crate.
}
