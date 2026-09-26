# skaidb JDBC driver

`com.skaidb.jdbc` is a JDBC 4.3 driver layered on the core client
(`com.skaidb.Skaidb`). It ships in the same jar and has no dependencies.
Everything the core client does — seed-list failover, TLS, certificate
login, the session database, reconnection after a transport failure — the
JDBC connection does too.

## Registering

Nothing to do. The jar lists `com.skaidb.jdbc.SkaidbDriver` in
`META-INF/services/java.sql.Driver`, so `DriverManager` finds it on the
classpath; loading the class (`Class.forName("com.skaidb.jdbc.SkaidbDriver")`)
also registers it, for containers that want a driver class name.

## URL

```
jdbc:skaidb://host[:port][,host2[:port]...][/database][?key=value&...]
```

The port defaults to 7000. The path is the session database (entered with
`USE` after every connect and reconnect). Values are percent-decoded (`%40`
is `@`; a `+` stays a `+`). Keys given as `Properties` (including the
`user`/`password` of `DriverManager.getConnection(url, user, password)`)
override the URL's. An unknown key in the URL is an error; unknown
`Properties` are ignored, since pools pass their own through.

| Key | Meaning | Default |
|-----|---------|---------|
| `user`, `password` | SCRAM-SHA-256 credentials. | `anonymous`, empty |
| `database` | Session database (same as the URL path). | none |
| `consistency` | `one`, `quorum` or `all` for every statement. | `quorum` |
| `tls` | `true`: TLS, verifying the server against the JVM trust store. | `false` |
| `tls_ca` | PEM file of the CA(s) to trust. Implies TLS. | — |
| `tls_insecure` | `true`: TLS that verifies nothing. Development only. | `false` |
| `tls_server_name` | SNI name the server certificate must carry. | `skaidb` |
| `tls_client_cert`, `tls_client_key` | Client certificate (PEM) and its unencrypted key (PKCS#8, or PKCS#1 RSA). Implies TLS. | — |
| `auth_mechanism` | `scram`, or `certificate` to log in with the client certificate (its CN is the user; `user` may be omitted). | `scram` |
| `transaction` | What `setAutoCommit(false)` opens: `begin` (`BEGIN`, standalone server) or `atomic` (`BEGIN ATOMIC`, cluster). | `begin` |
| `read_timeout` | Milliseconds any read may block (0 = no limit). | `0` |
| `fetch_size` | Default `Statement` fetch size; above 0 streams result sets. | `0` |

Errors in the URL throw `SQLNonTransientConnectionException` with SQLState
`08001`.

```java
Connection c = DriverManager.getConnection(
    "jdbc:skaidb://db1:7000,db2:7000,db3:7000/app?consistency=quorum", "app", "s3cret");

Connection mtls = DriverManager.getConnection(
    "jdbc:skaidb://db1/app?tls_ca=/etc/skaidb/ca.pem"
    + "&tls_client_cert=/etc/app/app.crt&tls_client_key=/etc/app/app.key&auth_mechanism=certificate");
```

## Connection pools

`com.skaidb.jdbc.SkaidbDataSource` is a `javax.sql.DataSource` with bean
setters for every key (`setUrl`, `setUser`, `setPassword`, `setDatabase`,
`setConsistency`, `setTls`, `setTlsCa`, `setTlsClientCert`,
`setTlsClientKey`, `setAuthMechanism`, `setTransaction`, `setReadTimeout`,
`setFetchSize`, ...). Each `getConnection()` opens a new physical
connection; pooling is the pool's job.

**HikariCP**, by JDBC URL:

```java
HikariConfig cfg = new HikariConfig();
cfg.setJdbcUrl("jdbc:skaidb://db1:7000,db2:7000,db3:7000/app");
cfg.setUsername("app");
cfg.setPassword("s3cret");
cfg.setMaximumPoolSize(16);
HikariDataSource ds = new HikariDataSource(cfg);

try (Connection c = ds.getConnection();
     PreparedStatement ps = c.prepareStatement("SELECT name FROM users WHERE id = ?")) {
    ps.setLong(1, 42);
    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) System.out.println(rs.getString("name"));
    }
}
```

or through the DataSource:

```java
HikariConfig cfg = new HikariConfig();
cfg.setDataSourceClassName("com.skaidb.jdbc.SkaidbDataSource");
cfg.addDataSourceProperty("url", "jdbc:skaidb://db1:7000,db2:7000/app");
cfg.addDataSourceProperty("user", "app");
cfg.addDataSourceProperty("password", "s3cret");
HikariDataSource ds = new HikariDataSource(cfg);
```

`Connection.isValid(timeout)` is a `SELECT 1` round trip bounded by the
timeout, so no `connectionTestQuery` is needed. `setNetworkTimeout` bounds
every read on the connection.

## Statements

- `Statement` sends the SQL verbatim; `?` is not a placeholder there.
- `PreparedStatement` prepares on the server and sends parameters as typed
  values, so arrays, documents (`java.util.Map`), `UUID` and `BigDecimal`
  bind losslessly. Statements the server will not prepare (DDL, `USE`) fall
  back to client-side literal binding. Every parameter must be set
  (SQLState `07001` otherwise).
- `PreparedStatement.executeBatch()` sends every row in **one** round trip
  (`OP_EXECUTE_BATCH`). The server reports the total only, so each element
  of the returned array is `Statement.SUCCESS_NO_INFO` and
  `getLargeUpdateCount()` holds the total. Rows autocommit individually
  unless a transaction is open: if one fails, the rows before it stay
  applied and a `BatchUpdateException` is thrown.
- `Statement.executeBatch()` runs its statements one after another.
- DDL reports an update count of 0.
- A `CALL` whose procedure `EMIT`s several result sets returns them through
  `getMoreResults()`.
- `setQueryTimeout(s)`: a statement that runs longer fails with
  `SQLTimeoutException`; the connection then re-dials, because the server's
  late answer would otherwise be read as the next statement's.
- `setMaxRows(n)` truncates client-side.
- Generated keys are not reported (`getGeneratedKeys()` is always empty);
  use `INSERT ... RETURNING`.

## Result sets

Forward-only and read-only (`TYPE_FORWARD_ONLY`, `CONCUR_READ_ONLY`,
`HOLD_CURSORS_OVER_COMMIT`). Column labels match case-insensitively, first
match wins.

By default the whole result arrives in one frame. With a fetch size above
zero (`Statement.setFetchSize`, or `fetch_size` in the URL) a `Statement` —
or a `PreparedStatement` without parameters — is **streamed**: rows arrive a
chunk at a time, so a table of any size can be read without materialising
it and without the server's one-shot scan budgets. While a streamed result
set is open it owns the connection: other statements on that connection fail
with SQLState `HY010` until it is read to the end or closed (closing reads
and discards the rest).

`getObject(i)` returns:

| skaidb | Java |
|--------|------|
| Null | `null` |
| Bool | `Boolean` |
| Int | `Long` |
| Float | `Double` |
| Decimal | `BigDecimal` |
| String | `String` |
| Bytes | `byte[]` |
| Uuid | `java.util.UUID` |
| Timestamp | `java.sql.Timestamp` |
| Array | `java.sql.Array` (elements as `getObject` returns them) |
| Document | `java.util.LinkedHashMap<String, Object>` |

The typed getters convert where it makes sense: numbers between each other
(`SQLDataException` `22003` when out of range), numeric text to numbers,
anything to `String` (arrays and documents as JSON), text to `UUID`,
timestamps to `Timestamp`/`Date`/`Time`. `getObject(i, Class)` also accepts
`Instant`, `OffsetDateTime`, `ZonedDateTime`, `LocalDateTime`, `LocalDate`,
`List`, `Map`, `BigInteger` and the primitive wrappers. The `java.time`
local types are read and bound in UTC; the legacy `java.sql.Date`/`Time`
use the given `Calendar`'s zone, else the JVM's default. skaidb has no
time-of-day type, so `setTime` is not supported.

`ResultSetMetaData` has the server's column names. skaidb tables are
schema-less and the wire carries a type per value, not per column, so
column types are inferred from a materialised result's values (the one type
all non-NULL values share, else `OTHER`); a streamed result reports `OTHER`.
Nullability is always `columnNullableUnknown`.

## Transactions

Autocommit is on by default. With `setAutoCommit(false)` the first
statement opens a transaction — `BEGIN`, or `BEGIN ATOMIC` with
`transaction=atomic` — and `commit()`/`rollback()` end it; switching
autocommit back on commits, and closing the connection rolls back. Plain
`BEGIN` needs a standalone server: a cluster refuses it and wants
`transaction=atomic`. DDL is not transactional. The isolation level is
`TRANSACTION_READ_COMMITTED` (the only one accepted). Savepoints are not
supported.

If the connection breaks inside a transaction the server has discarded it:
the next statement, or `commit()`, throws `SQLTransactionRollbackException`
(SQLState `40003`) instead of running outside the transaction.

## Metadata

`DatabaseMetaData` reports product `skaidb`, the driver version and JDBC
4.3. The server version is not available over the protocol
(`getDatabaseProductVersion()` is `unknown`). Catalogs are skaidb databases;
there are no schemas.

| Method | Source |
|--------|--------|
| `getCatalogs` | `SHOW DATABASES` |
| `getTables` | `SHOW TABLES` of the current database (types `TABLE`, `VIEW`, `MATERIALIZED VIEW`, `TIMESERIES`, `ROLLUP`) |
| `getColumns` | `DESCRIBE` per table: only the columns the catalog knows (primary-key, indexed, generated), typed `OTHER` |
| `getPrimaryKeys` | `DESCRIBE` (key order) |
| `getIndexInfo` | `SHOW INDEXES` |
| `getTypeInfo` | the fixed list of skaidb value types |

These cover the connection's current database; asking for another catalog
answers empty. Metadata skaidb does not keep — procedures, functions,
privileges, foreign keys, UDTs — comes back as an empty result set with the
columns JDBC specifies.

## Errors

Every failure is an `SQLException` subclass with an SQLState:

| Situation | Exception | SQLState |
|-----------|-----------|----------|
| Bad URL or options; no seed reachable; TLS setup | `SQLNonTransientConnectionException` | `08001` |
| Wrong credentials, certificate refused, server signature mismatch | `SQLInvalidAuthorizationSpecException` | `28000` |
| Transport failure mid-statement (the connection re-dials next time) | `SQLRecoverableException` | `08006` |
| Read or query timeout | `SQLTimeoutException` | `HYT00` |
| Used after close | `SQLNonTransientConnectionException` | `08003` |
| Unique / not-null / foreign-key / check / exclusion violation | `SQLIntegrityConstraintViolationException` | `23505` / `23502` / `23503` / `23514` / `23P01` |
| Parse error; unknown table, column or function; permission denied; already exists | `SQLSyntaxErrorException` | `42601`; `42P01`, `42703`, `42883`; `42501`; `42P07` |
| Scan budget exceeded | `SQLNonTransientException` | `54000` |
| Connection busy with an open streamed result set | `SQLException` | `HY010` |
| A feature the driver does not implement | `SQLFeatureNotSupportedException` | `0A000` |
| Anything else the server refused | `SQLException` | `HY000` |

The message is always the server's (or the core driver's) own text.

## Not supported

Scrollable or updatable result sets, `CallableStatement` (run `CALL`
through a `Statement` or `PreparedStatement`), savepoints, generated keys,
`Statement.cancel()` (use `setQueryTimeout`), named cursors, `Blob`/`Clob`/
`NClob`/`SQLXML`/`Ref`/`RowId`/`Struct` objects (bind `byte[]`/`String`/
`Map` instead; the stream setters read their input fully), type maps and
JDBC escape syntax. Each throws `SQLFeatureNotSupportedException`.
`Driver.jdbcCompliant()` is `false`.

The core client's objects stay reachable: `connection.unwrap(Skaidb.Connection.class)`
returns the underlying connection, and `SkaidbConnection.setConsistency("one")`
changes the level of later statements.
