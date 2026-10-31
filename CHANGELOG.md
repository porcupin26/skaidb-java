# Changelog

All notable changes to the skaidb Java driver. The driver has its own
version series, independent of the skaidb server; see the README's
compatibility section for which server versions each release speaks to.

## 1.4.1 (2026-10-31)

One slow round trip at the start no longer gets a healthy pool member
ejected.

- Fixed: a member's first round trip set its latency average outright, so
  a statement that stalled in the client — a CPU-bound application holding
  the thread while it starts — put seconds into it; ten round trips later
  the member was compared with the others, and the average had barely
  decayed: ejected (`latency 4153.7 ms vs median 0.2 ms`) while it answered
  in milliseconds. The fastest of a member's first three round trips now
  seeds its average (a round trip is only ever lengthened), and later ones
  move it as before. Until then the member counts as not measured:
  `Pool.Member.latencyMs` is `null` and `LEAST_LATENCY` tries it.
- A latency ejection's warning names the slowest of the member's last 10
  round trips alongside its average and the median (`latency 516.3 ms vs
  median 0.2 ms; slowest of the last 10 round trips 900.0 ms`), which tells
  a slow member from one slow statement.

## 1.4.0 (2026-10-29)

A streamed `CALL` whose procedure body `EMIT`s result sets returns them.

- Fixed: the server answers such a `CALL` with all of its result sets in
  one `ResultSets` frame (tag 8) even to a stream request, and
  `conn.stream(...)`, with or without parameters, refused that frame
  (`unexpected response tag 8 to stream request`) and marked the connection
  broken. The frame is now decoded by the code the buffered path uses: the
  stream starts on the first set and the new `RowStream.nextResultSet()`
  walks the rest, as `ResultSet.nextResultSet()` does. The exchange is
  already over, so the stream owns nothing and the connection stays usable;
  the new `RowStream.isStreamed()` is `false` for such a one-frame reply.
- Fixed (JDBC): with a fetch size above zero a `Statement`, or a
  `PreparedStatement` without parameters, streams every statement, so a
  `CALL` whose procedure EMITs failed the same way. Its result sets now come
  back through `getResultSet()` / `getMoreResults()`, materialised, exactly
  as without a fetch size.

## 1.3.0 (2026-10-29)

Streaming with bound parameters.

- **Parameterised streaming**: `conn.stream(sql, params...)` binds `?`
  placeholders, typed, exactly as a prepared `Query` does, over
  `OP_QUERY_STREAM_PARAMS` (server ≥ 0.330) — a range-bounded export no
  longer needs its literals interpolated into the SQL. Without parameters
  the request is the original `OP_QUERY_STREAM`, so a parameterless stream
  works against every server; an older server refuses a parameterised one
  with `server does not support parameterised streaming (skaidb >= 0.330):
  ...` and the connection stays usable. `stream(String)` keeps its
  signature, so code compiled against 1.2.0 links unchanged; an `Object[]`
  passed alone is the parameter list. There is deliberately no
  `stream(String, int)` overload: it would read `stream(sql, 7)` as a
  consistency level, so a stream keeps running at the connection's level.
- JDBC: with a fetch size above zero, a `PreparedStatement` query (`SELECT`
  or `WITH`) with parameters is streamed too, its parameters travelling with
  it. Against a server older than 0.330 (remembered per connection until it
  re-dials), or where the server's grammar takes no placeholder, it runs
  prepared as before; other statements with parameters keep the prepared
  path, which reuses the statement id.
- The vendored conformance suite gains `stream_params`; `ConformanceTest`
  passes `call.params` to `Connection.stream`.

## 1.2.0 (2026-10-25)

Connection-pool load balancing across the cluster's members.

- **Pool load balancing across members** (the rules every skaidb driver
  shares, PROTOCOL.md §6.1), set through the new `Skaidb.PoolOptions`
  (`new Pool(dsn, options)`, `new Pool(connectOptions, options)`; the
  existing constructors keep working): `LoadBalancing.ROUND_ROBIN` /
  `LEAST_LATENCY` (power of two choices on the round-trip average ×
  checked-out connections, re-measuring a member not used in 2 s; `RANDOM`
  stays the default), `IdleOrder.FIFO`, health ejection (consecutive
  transport failures, or a latency average far above the other members')
  with a background probe that re-admits the member, `discover(true)`
  (members from `SHOW ENDPOINTS`, server ≥ 0.327: joiners added, leavers
  dropped, resyncing/unreachable members skipped and re-checked every 5 s),
  and `readYourWrites(Duration)` with `acquire(affinity)` /
  `withConnection(affinity, work)` — reads follow the member that took the
  key's last write. `pool.members()` reports each member's state, latency
  and connections. The probe and refresh run on one daemon thread per pool,
  stopped by `close()`.
- A pooled connection that re-dials does so in the pool's order (usable
  members first, the one it lost last).
- Changed: a seed that closes the connection during the handshake (a
  capacity refusal, a node going down) no longer ends a connect — the next
  seed is tried — and `authentication denied: too many connections: ...`
  counts as unreachable (`no reachable endpoint`, JDBC SQLState `08001`, not
  `28000`). A login refused for its credentials still fails at once.
- Changed: `new Pool(dsn)` parses the DSN in the constructor, so a malformed
  DSN throws there rather than at the first `acquire()`.
- Fixed: a transport failure while preparing a statement left the connection
  looking usable; it is now marked broken and re-dials on its next
  statement, as after a failure mid-query.
- JDBC: `SkaidbDataSource` leaves pooling to HikariCP and the like, whose
  connections land on random members; docs/JDBC.md says so and points to
  `Skaidb.Pool` for balancing.

## 1.1.0 (2026-09-26)

JDBC driver, certificate login, and the shared wire-protocol conformance suite.

- **JDBC 4.3 driver** (`com.skaidb.jdbc`), in the same dependency-free jar:
  `SkaidbDriver` is registered through `META-INF/services/java.sql.Driver`
  (and by loading the class) and accepts
  `jdbc:skaidb://host[:port][,host2[:port]...][/database][?user=..&password=..&tls=..&consistency=..]`
  (Properties override the URL). `Connection`, `Statement`,
  `PreparedStatement` (server-side prepared, typed parameters;
  `executeBatch` is one `OP_EXECUTE_BATCH` round trip), forward-only
  read-only `ResultSet` with converting getters and `getObject(i, Class)`,
  `ResultSetMetaData` (types inferred from the values), `DatabaseMetaData`
  (`getTables`/`getColumns`/`getPrimaryKeys`/`getIndexInfo`/`getCatalogs`
  from `SHOW TABLES`/`DESCRIBE`/`SHOW INDEXES`/`SHOW DATABASES`),
  `setAutoCommit(false)` transactions (`BEGIN`, or `BEGIN ATOMIC` with
  `transaction=atomic`), streaming with a fetch size, SQLException
  subclasses with SQLStates, and `SkaidbDataSource` for HikariCP and other
  pools. See `docs/JDBC.md`.
- **Certificate login** (wire mechanism EXTERNAL): `tls_client_cert` /
  `tls_client_key` present a client certificate (PEM; PKCS#8 or PKCS#1 RSA
  key) and `auth_mechanism=certificate` makes it the login. With no user
  given, AuthStart carries an empty name and the server takes the
  certificate's CN.
- `Skaidb.ConnectOptions` and `Skaidb.connect(ConnectOptions)`: every
  connection setting as a builder, values taken verbatim.
- `Query.execute()` and `Connection.executeRaw(sql)` return a
  `Skaidb.Result` telling rows, an affected count and DDL apart;
  `Query.getParameterCount()`.
- `Connection.setReadTimeout(millis)` bounds every read;
  `Connection.reconnects()` counts re-dials.
- The shared conformance suite (`conformance/vectors.json`, from the
  server's reference encoders) runs in `ConformanceTest`: every value
  vector, every SCRAM vector, the auth outcomes and every case through the
  public API against a fake server that checks the request bytes. CI checks
  the vendored vectors against <https://skaidb.org/conformance/vectors.json>.
- Fixed (found by the conformance suite): `Connection.stream()` of a
  statement that returns no rows dropped the affected-row count;
  `RowStream.getAffected()` now reports it.
- Fixed: a login the server refused left its socket open until garbage
  collection; it is now closed at once.
- Fixed: `Query.executeBatch` on a statement the server will not prepare
  (DDL, `USE`) threw an internal exception type; it now runs the rows with
  client-side binding.

## 1.0.2

Release automation: published from GitHub Actions. No driver changes.

- Pushing a tag `vX.Y.Z` now runs the release workflow
  (`.github/workflows/release.yml`) on GitHub-hosted runners: it checks the
  tag against `pom.xml`, builds and tests on Temurin 17, creates the GitHub
  Release with the jar, `-sources.jar`, `-javadoc.jar`, pom and checksums
  attached and this section as the notes, then has JitPack build the tag
  and verifies that `com.github.porcupin26:skaidb-java:vX.Y.Z` resolves.

## 1.0.1

Republished under a new tag because JitPack cached a failed v1.0.0 build;
no code changes.

- The first commit under tag `v1.0.0` built with JitPack's stock Maven,
  which is too old for `maven-compiler-plugin` 3.13.0; JitPack cached that
  failure and never re-resolves a tag, so
  `com.github.porcupin26:skaidb-java:v1.0.0` does not resolve. Use
  `v1.0.1` or newer.

## 1.0.0

First standalone release. The driver previously lived in the skaidb
monorepo under `drivers/java`; its history is carried over unchanged.

- Published as `com.github.porcupin26:skaidb-java` (via JitPack); a Maven
  Wrapper (`./mvnw`, Maven 3.9.9) is checked in.
- The version the driver reports to the server in its Hello frame is now
  derived from the package metadata (`Skaidb.VERSION`), never a literal.
- `SkaidbException` and the internal `Unpreparable` carry a `serialVersionUID`.
- DSN parsing is factored into a package-private `parseDsn`, so it is
  covered by tests without a socket.
- JUnit 5 test suite: parameter counting and client-side binding, value
  encode/decode against the wire spec, DSN parsing, and an in-process fake
  server covering the SCRAM handshake, Hello, prepared statements,
  streaming and the abandon/drain rule.
- Sources and Javadoc jars are attached to every build.
- `examples/` holds the runnable examples.
