# Changelog

All notable changes to the skaidb Java driver. The driver has its own
version series, independent of the skaidb server; see the README's
compatibility section for which server versions each release speaks to.

## 1.5.2 (2026-11-16)

A connection the server closed while it sat idle costs no statement, a
re-dialled connection is the session the lost one was — settings
included — a pool hands every borrower its own session, and a transaction
written by hand stays on its connection as the helper's does.

**Upgrade notes.**
- On a pool without a database option a borrower's `USE` no longer carries
  over to later borrowers: every borrower starts in `default`. Choose the
  pool's database in its DSN (or `ConnectOptions`). The same goes for a
  borrower's `SET CONSISTENCY` and `SET SCAN BUDGET`, and for a
  transaction it left open, with or without a database option.
- A thread that is interrupted while it waits for the server now stops
  waiting: its statement throws a transport error and the connection is
  closed (it re-dials on its next statement). The interrupt used to be
  ignored until the server answered. A thread that merely arrives with its
  interrupt status set runs its statements as before.
- The first statement after the server restarted is no longer the one
  that fails: code that retried it once "to get past the dead connection"
  still works, and no longer has anything to retry.

**Fixed**
- A connection the server had closed while it sat idle — a restart, a
  failover, an idle timeout — failed the next statement with a transport
  error (`query failed: Broken pipe`, or over a network a failed read
  after a write that still succeeded), once for every idle connection, and
  a pool handed such a connection out as live. Before it writes a request
  the driver now looks at the socket: one read that does not wait, no
  round trip. Anything readable on an idle connection — the end of the
  stream, a reset, stray bytes — means it is gone, and it is replaced
  before anything was sent on it: the statement, a read or a write alike,
  runs on the new connection. Under TLS the look reads the TCP connection
  beneath the session; a connection that is streaming is not looked at;
  inside a transaction a connection found gone is the transaction lost,
  not a re-dial. `Pool.acquire` takes the same look and drops a dead idle
  connection. JDBC: the same for statements, `isValid()` and metadata.
- `SET CONSISTENCY ...` and `SET SCAN BUDGET ...` were lost when a
  connection re-dialled: after a restart a scan the application's budget
  had refused simply ran, and an application that had asked for `SET
  CONSISTENCY ALL` read at each request's own level. The connection notes
  the last statement of each kind the server accepted, as it notes a
  `USE`, and sends them again on the new connection — after the session
  database, the consistency override first — before the statement; `...
  DEFAULT` clears the note. A setting the new connection refuses fails
  the statement with that error: it never runs under other settings than
  the ones the application set.
- A pool handed a connection on in the session its last borrower left:
  in the database of that borrower's `USE` (a pool without a database
  option), under its `SET SCAN BUDGET`, at its `SET CONSISTENCY` level, or
  inside a transaction it had left open. `Pool.release` now puts the
  connection back into the pool's own session before it is kept —
  `ROLLBACK`, `USE` of the pool's database (`default` when it has none),
  `SET SCAN BUDGET DEFAULT`, `SET CONSISTENCY DEFAULT`, only the ones that
  are needed — and closes a connection it cannot put back (a server older
  than 0.349 refuses `SET CONSISTENCY DEFAULT`). A pool with a database
  option used to close a connection a `USE` had moved; it now moves it
  back. For a pool of your own: `Connection.session()` returns the noted
  session (`Skaidb.Session`: database, consistency override, scan budget)
  and `Connection.restore(session)` puts a connection back to it. JDBC: the
  connection does that at the request boundaries of JDBC 4.3 —
  `beginRequest()` takes the session as the request finds it,
  `endRequest()` closes an open streamed result set, rolls an open
  transaction back and puts the session back, or closes the connection
  when it cannot. A pool has to mark those boundaries for it: HikariCP
  does from version 6 on when the JVM runs with
  `-Dcom.zaxxer.hikari.enableRequestBoundaries=true`; see docs/JDBC.md.
- After a connection loss, a `USE` could not leave a database that had
  been dropped meanwhile: the re-dial entered the old database first, was
  refused, and `USE other` failed like every other statement — the
  application could only reconnect by hand. A re-dial caused by a `USE`
  statement itself (or JDBC's `setCatalog`) no longer enters the old
  database first; the session's settings are sent, then the `USE`, whose
  answer decides. Any other statement still fails rather than run in
  another database.
- A transaction written by hand — `BEGIN` sent as a statement — did not
  stay on its connection: after a lost connection the driver re-dialled,
  and the rest of the transaction, and its `COMMIT`, ran on a new session,
  each statement committed on its own. The connection now notes a `BEGIN`
  the server accepted (any statement whose first word is `BEGIN`) and the
  `COMMIT [TRANSACTION]` or `ROLLBACK [TRANSACTION]` that ends it, and in
  between nothing re-dials, exactly as inside `transaction(body)`: every
  statement after the loss throws `the connection was lost during the
  transaction` without being sent. The application ends the transaction
  as it would have — `ROLLBACK` succeeds without being sent, `COMMIT`
  throws that error (`... nothing was committed`) — and the next statement
  re-dials. A `COMMIT` under which the connection is lost says that
  whether the transaction was committed is unknown. JDBC: a `BEGIN` sent
  through a `Statement` is held to the same rule (SQLState `40003`).
- A body of `transaction(body)` that handed a statement on its connection
  to another thread and waited for that thread never returned: the thread
  waited for the transaction to end, the transaction for the thread. A
  statement of another thread still waits for the transaction — as long
  as the transaction is at work. One that has waited 30 seconds without
  the transaction's thread using the connection throws `connection is
  busy in another thread's transaction` and is not sent, which ends the
  body's wait too.
- A server that hung up was reported as `query failed: null` (`no
  reachable endpoint in h:p: null` at the login). The error says `the
  connection was closed by the server`; no transport error reads `null`
  any more.
- `close()` may be called from another thread at any moment of a
  statement, under TLS too: it ends a statement that waits for the server
  and neither throws nor waits for it.
- `subscribe()` returns when its thread is interrupted while a poll waits
  for the server, as it does for an interrupt between two polls.
- JDBC: inside a transaction whose connection was lost, `DatabaseMetaData`
  calls, `getCatalog()` and `setCatalog()` re-dialled — a new session
  behind the transaction's back. They throw
  `SQLTransactionRollbackException` (`40003`) like a statement.
- JDBC: `isValid()` answered `false` for a healthy connection while a
  streamed `ResultSet` was open on it. It answers `true`, without a round
  trip.
- JDBC: `getCatalog()` went on answering the URL's database after a `USE`
  sent through a statement had entered another, and a `setCatalog()` back
  to the URL's database sent nothing. Both follow the database the
  connection is in (`Skaidb.Connection.database()`, new).
- JDBC: a statement refused for the scan budget carried SQLState `HY000`
  against a real server, whose message starts with `resource limit: scan
  budget exceeded`. It is `SQLNonTransientException`, `54000`, as
  documented.
- JDBC: on a connection with a fetch size, `SET CONSISTENCY ...` through a
  `Statement` was refused by the server (it takes that statement only
  un-streamed). `stream(sql)` sends a `SET CONSISTENCY` as the ordinary
  statement it is.

## 1.5.1 (2026-11-16)

A transaction stays on the connection it began on, a connection stays in
the database it was in, a prepared statement stays the statement it was,
and a broken connection lets go of its socket at once.

- Fixed: inside `conn.transaction(body)`, a body that caught a transport
  failure and carried on had its next statement sent on a re-dialled
  connection, where no transaction is open — so that statement was
  committed on its own, while the helper reported the loss. Nothing
  re-dials any more between the answer to `BEGIN` and the end of the
  helper: each further statement of the body throws `the connection was
  lost during the transaction` without being sent, and so does the helper
  instead of sending `COMMIT`; both carry the I/O failure as their cause.
  `BEGIN` itself still re-dials a broken connection as any statement does,
  a failure under `COMMIT` is still thrown as it is, and the connection
  re-dials on its first statement after the helper. `pool.transaction` is
  the same helper.
- Fixed: a body that returned or threw with a `RowStream` still open left
  the transaction open — neither `COMMIT` nor `ROLLBACK` can be sent behind
  a stream — and once the stream was closed, later statements on that
  connection ran inside the transaction and were lost with it. The helper
  now closes such a stream and rolls the transaction back; if the rest of
  the stream cannot be drained the connection is marked broken, and the
  server drops the transaction with the socket. The body's exception is
  thrown as before; a body that returned gets `connection is busy
  streaming: the transaction's body returned with a RowStream open ...`.
- Fixed: a connection marked broken — a transport failure, a read that
  exceeded `setReadTimeout`, a stream that lost its place on the wire —
  kept its dead socket open until the next statement re-dialled or
  `close()` ran, so the server kept the session, and a transaction open on
  it, until then. The socket is now closed the moment the connection is
  marked broken.
- Fixed (JDBC): with `setAutoCommit(false)`, a streamed `ResultSet` that
  was still open made the driver take the connection for lost. Another
  statement and `commit()` threw `SQLTransactionRollbackException`
  (`40003`), `rollback()` returned without rolling back, and the
  transaction stayed open on the server: statements run in autocommit
  afterwards joined it and were never committed. A statement or `commit()`
  behind an open streamed result set now fails with SQLState `HY010`, as
  it does outside a transaction, and the transaction stays open;
  `rollback()` closes the result set and rolls back.
- Fixed: a database entered with a `USE` statement was lost when the
  connection re-dialled after a transport failure: the new session was in
  no database, and the next statements ran against the default database's
  tables of the same names — refused there at best, read or written
  silently otherwise. Only the database of the DSN (or `ConnectOptions`)
  was entered again. The connection now notes a `USE` the server accepted
  (`USE [DATABASE] name [;]`, the name bare or in double quotes; a `--`
  comment after it, or on a line before it, is skipped as the server skips
  it) and every re-dial enters the database the connection was last in
  before anything else is sent; if the server refuses it there, the
  statement fails with that error instead of running elsewhere. JDBC:
  `setCatalog` and a `USE` sent through a statement survive a reconnect the
  same way.
- Fixed: a pool with a session database handed a connection on in whatever
  database its last borrower's `USE` had left it. `release()` now closes
  such a connection, so the next borrower gets one in the pool's database.
  A pool without a session database keeps the connection, as before.
- Fixed: a connection that ran more than 256 distinct parameterised
  statements filled the server's table of prepared statements (256 for a
  connection), and from then on the driver sent each further one as text
  with its values written into it — silently, and typed differently: a
  timestamp was stored as a number, bytes as a hex string, and an array or
  document parameter failed (`cannot bind a collection into a statement
  the server will not prepare`). The connection now keeps the 240
  statements used most recently and closes the least recently used one on
  the server (`OP_CLOSE`) to make room, so any number of distinct
  statements run with typed parameters. A prepare the server refuses with
  `too many prepared statements` all the same is thrown as a statement
  error. JDBC: any number of `PreparedStatement`s can be created and
  dropped, closed or not — the statement on the server belongs to the
  connection, shared by its text.
- Fixed: with threads sharing one connection, a statement with parameters
  could be executed under a prepared-statement id of the connection's
  previous socket, when another thread's statement re-dialled between the
  first thread's prepare and its execute. On the new session that id is
  unknown — or names another statement, which then ran with the first
  one's values. The prepare and the execute now happen under one hold of
  the connection.
- Fixed: a connect whose login succeeded and whose session database was
  then refused (the `USE` failed) left its socket open until the garbage
  collector closed it. It is closed before the error is thrown.
- `examples/Transactions.java`: the transaction helper end to end — a
  commit, a conflict that is retried, a conflict that is thrown, a
  rollback.

## 1.5.0 (2026-11-15)

A retrying transaction helper and a typed error for a transaction conflict.

- **`TransactionConflictException`**: a standalone server (0.346 or newer)
  refuses a `COMMIT` when another writer changed a row the transaction
  writes after the transaction began, with an error whose message starts
  with `transaction conflict:`. The driver throws
  `Skaidb.TransactionConflictException` for it wherever it throws a
  server's error — `execute`, `query`, `executeRaw`, a `Query`, a batch, a
  stream — with the server's message unchanged. It is a subclass of
  `SkaidbException` (which is no longer `final`), so existing `catch`
  blocks catch it as before. It is a statement error: the connection stays
  usable and is not re-dialled, and a pool does not count it against the
  member.
- **`conn.transaction(body)`**: sends `BEGIN`, runs the body with the
  connection, sends `COMMIT` and returns the body's value. A commit refused
  for a conflict starts over from `BEGIN` and runs the body again, with no
  pause, up to three runs; `transaction(body, attempts)` sets the number of
  runs and `transaction(body, attempts, begin)` the opening statement
  (`BEGIN ATOMIC` on a cluster), sent verbatim. A refused `BEGIN` is thrown
  and nothing else is sent; a body that throws is rolled back and its
  exception rethrown as it is (whatever the `ROLLBACK` answers is ignored),
  never retried; any other `COMMIT` error is thrown with no retry and no
  `ROLLBACK`. A transport failure is never retried, and `COMMIT` and
  `ROLLBACK` are never sent on a re-dialled socket. The connection's
  monitor is held for the whole transaction, so other threads' statements
  on a shared connection wait for it.
- **`pool.transaction(body)`** (same overloads): the helper on one
  connection checked out for the whole transaction and returned after.
- JDBC: a `commit()` refused for a conflict throws
  `SQLTransactionRollbackException` with SQLState `40001` (it was a plain
  `SQLException`, `HY000`), the `TransactionConflictException` as its
  cause; the next statement opens a new transaction on the same connection.

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
