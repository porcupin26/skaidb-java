# Changelog

All notable changes to the skaidb Java driver. The driver has its own
version series, independent of the skaidb server; see the README's
compatibility section for which server versions each release speaks to.

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
