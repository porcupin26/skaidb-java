# skaidb-java

[![CI](https://github.com/porcupin26/skaidb-java/actions/workflows/ci.yml/badge.svg)](https://github.com/porcupin26/skaidb-java/actions/workflows/ci.yml)
[![JitPack](https://jitpack.io/v/porcupin26/skaidb-java.svg)](https://jitpack.io/#porcupin26/skaidb-java)

The official [skaidb](https://skaidb.org) driver for Java. A JDBC-flavoured
client — `connect`, `prepare`, `setInt`/`setString`, `executeQuery`/
`executeUpdate`, and a `ResultSet` with `next()`/`getInt`/`getString` — so a
JDBC user has essentially nothing new to learn. It speaks skaidb's binary
protocol directly: SCRAM-SHA-256 authentication, server-side prepared
statements with typed parameters, streamed result sets, TLS, seed-list
failover and connection pooling.

**Pure JDK, no dependencies, one source file** (`com.skaidb.Skaidb`).
Requires Java 11 or newer.

- Server documentation: <https://skaidb.org/docs/>
- Wire protocol the driver implements: <https://skaidb.org/docs/PROTOCOL.html>
- Full API reference in this repository: [docs/API.md](docs/API.md)
- Changelog: [CHANGELOG.md](CHANGELOG.md)

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Connecting](#connecting)
- [Statements and parameters](#statements-and-parameters)
- [Result sets](#result-sets)
- [Streaming large results](#streaming-large-results)
- [Batches, transactions and prepared statements](#batches-transactions-and-prepared-statements)
- [Connection pooling](#connection-pooling)
- [Change streams (`subscribe`)](#change-streams-subscribe)
- [Type mapping](#type-mapping)
- [Errors and reconnection](#errors-and-reconnection)
- [Threading](#threading)
- [Versions and compatibility](#versions-and-compatibility)
- [Building and testing](#building-and-testing)
- [License](#license)

## Install

The artifact is published through [JitPack](https://jitpack.io/#porcupin26/skaidb-java),
which builds it from the git tag on first request. Coordinates:

```
com.github.porcupin26:skaidb-java:v1.0.2
```

> **Note:** `v1.0.0` is not resolvable through JitPack — its first build
> failed on JitPack's stock Maven and JitPack caches a tag's first result
> for good — so use `v1.0.1` or newer. The driver code is identical.

The JitPack repository has to be declared once — that is the only step
Maven Central would spare you (publishing there needs a Sonatype account and
GPG-signed artifacts, which this project does not have yet).

**Maven** — `pom.xml`:

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.github.porcupin26</groupId>
    <artifactId>skaidb-java</artifactId>
    <version>v1.0.2</version>
  </dependency>
</dependencies>
```

**Gradle (Kotlin DSL)** — `settings.gradle.kts` or `build.gradle.kts`:

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.porcupin26:skaidb-java:v1.0.2")
}
```

**Gradle (Groovy)** — `build.gradle`:

```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.porcupin26:skaidb-java:v1.0.2'
}
```

Sources and Javadoc jars are attached (`-sources.jar`, `-javadoc.jar`), so
IDEs resolve documentation automatically.

**Without a build tool.** The driver is one file. Either copy
`src/main/java/com/skaidb/Skaidb.java` into your tree, or:

```sh
javac -d out src/main/java/com/skaidb/Skaidb.java
jar cf skaidb.jar -C out .
```

A jar built this way has no manifest version, so the driver identifies
itself to the server with its built-in fallback version.

## Quick start

```java
import com.skaidb.Skaidb;

try (Skaidb.Connection conn = Skaidb.connect("skaidb://user:pass@localhost:7000/app")) {
    conn.execute("CREATE TABLE users (PRIMARY KEY (id))");

    try (Skaidb.Query q = conn.prepare("INSERT INTO users (id, name) VALUES (?, ?)")) {
        q.setInt(1, 1).setString(2, "Ada").executeUpdate();
    }

    Skaidb.ResultSet rs = conn.prepare("SELECT id, name FROM users WHERE id = ?")
                              .setInt(1, 1).executeQuery();
    while (rs.next()) {
        System.out.println(rs.getInt("id") + " " + rs.getString("name"));
    }
}
```

Two runnable programs live in [`examples/`](examples/): `Example.java` is
this tour; `Advanced.java` covers typed parameters, batches, per-statement
consistency, streaming, pooling, multi-result-set `CALL`s and change streams.

```sh
javac -d out src/main/java/com/skaidb/Skaidb.java examples/Example.java
java -cp out Example localhost 7000 skaidb secret
```

## Connecting

### `Skaidb.connect(String dsn)`

```
skaidb://[user[:password]@]host[:port][,host2[:port2]...][/database][?option=value&...]
```

| Part | Meaning | Default |
|------|---------|---------|
| `user`, `password` | SCRAM-SHA-256 credentials. Omit both for an anonymous connection (only works when the server allows it). | `anonymous`, empty |
| `host[:port]`, comma-separated | Seed list. The driver shuffles it and dials seeds in turn until one **connects and authenticates**; a node that accepts TCP while unhealthy is skipped. skaidb is leaderless, so any node serves every statement. | port `7000` |
| `/database` | Session database: the driver runs `USE "database"` after every connect and reconnect. | none |
| `consistency=one\|quorum\|all` | Default consistency level for statements on this connection. | `quorum` |
| `tls=true` | Encrypt with TLS, verifying the server certificate against the JVM's default trust store. | off |
| `tls_ca=/path/to/ca.pem` | Encrypt with TLS, trusting only the CA certificate(s) in that PEM file. Implies `tls=true`. | — |
| `tls_insecure=true` | Encrypt but **verify nothing** — a man in the middle can present any certificate. Development only. Implies `tls=true`. | off |
| `tls_server_name=name` | The SNI name sent and the name the server certificate must carry. skaidb's own certificates carry `DNS:skaidb`, which is usually not the address you dial, hence the separate knob. | `skaidb` |

Examples:

```java
Skaidb.connect("skaidb://localhost");                                     // anonymous, port 7000
Skaidb.connect("skaidb://app:s3cret@db1:7000,db2:7000,db3:7000/orders"); // seeds + session database
Skaidb.connect("skaidb://app:s3cret@db1/orders?consistency=one");
Skaidb.connect("skaidb://app:s3cret@db1/?tls_ca=/etc/skaidb/ca.pem&tls_server_name=skaidb");
Skaidb.connect("skaidb://app:s3cret@db1/?tls_insecure=true");            // dev only
```

A cluster configured with `client_tls = required` refuses plaintext
outright: without one of the `tls*` options such a cluster is simply
unreachable.

### `Skaidb.connect(String host, int port, String user, String password)`

One host, QUORUM consistency, no TLS, no session database. Everything else
goes through the DSN form.

### Consistency

`Skaidb.CONSISTENCY_ONE` (0), `Skaidb.CONSISTENCY_QUORUM` (1, the default) and
`Skaidb.CONSISTENCY_ALL` (2) select how many replicas must acknowledge a
write or be consulted for a read.

```java
conn.setConsistency(Skaidb.CONSISTENCY_ONE);           // every later statement on this connection

conn.prepare("SELECT ... WHERE id = ?")                // this statement only
    .setInt(1, 7)
    .setConsistency(Skaidb.CONSISTENCY_ALL)
    .executeQuery();
```

The per-statement form is the one to use from a connection shared by
several threads: the connection-level field is a plain shared setting.

### Timeouts

Connecting to a seed times out after 10 seconds. There is **no read
timeout**: a statement blocks for as long as the server takes, which is the
JDBC default too. Put long-running work on its own connection (or pool) so
it cannot delay unrelated statements.

## Statements and parameters

Every statement auto-commits; there is no `BEGIN`/`COMMIT`.

| Method | Use for | Returns |
|--------|---------|---------|
| `conn.execute(sql)` | A statement with no parameters that returns no rows. | affected row count, or `-1` for DDL |
| `conn.query(sql)` | A `SELECT` (or `CALL`) with no parameters. | `ResultSet` |
| `conn.prepare(sql)` | A statement with `?` placeholders. | `Query` |
| `conn.stream(sql)` | A large `SELECT`, read a chunk at a time. Takes no parameters. | `RowStream` |

`Query` binds parameters JDBC-style — `?` placeholders, **1-based**
setters, chainable:

```java
Skaidb.Query q = conn.prepare("INSERT INTO t (id, name, score, ok, tags) VALUES (?, ?, ?, ?, ?)");
q.setLong(1, 1L)
 .setString(2, "Ada")
 .setDouble(3, 9.5)
 .setBoolean(4, true)
 .setObject(5, java.util.List.of("web", "trial"))
 .executeUpdate();
```

`setInt`, `setLong`, `setDouble`, `setBoolean`, `setString`, `setNull` and
the general `setObject` all accept any of the Java types in [Type mapping](#type-mapping).
Placeholders inside string literals (`'why?'`) are not placeholders. An
index outside `1..n` throws immediately; a missing parameter is sent as
`NULL`.

`executeQuery()` returns a `ResultSet` (empty for a statement that produced
none); `executeUpdate()` returns the affected count (`-1` for DDL, `0` for a
statement that returned rows). `Query` is `AutoCloseable` for symmetry with
JDBC but holds no resources.

### How parameters travel

With parameters present the driver prepares the statement **on the server**
and sends the values as typed binary values — the only way to bind an
array, a document, a `UUID` or a `BigDecimal` losslessly. The server-side
prepared id is cached per connection (up to 240 distinct statements) and the
cache is cleared on reconnect, so a stale id can never execute the wrong
statement.

Statement kinds the server refuses to prepare — DDL and session statements
such as `USE` — fall back to **client-side text binding**: the values are
SQL-quoted and interpolated (`'` doubled, `NULL`/`TRUE`/`FALSE`, numbers as
literals, bytes as hex, timestamps as epoch milliseconds). On that path a
collection, map or Java array is refused with an exception rather than
silently stringified, because skaidb has no literal form for them.

## Result sets

`Skaidb.ResultSet` is fully materialised: the whole answer arrived in one
frame. Cursor position starts before the first row.

```java
Skaidb.ResultSet rs = conn.query("SELECT id, name, score FROM t");
String[] cols = rs.getColumnNames();
int n = rs.getRowCount();
while (rs.next()) {
    long id       = rs.getLong("id");     // by name
    String name   = rs.getString(2);      // by 1-based index
    double score  = rs.getDouble("score");
    Object raw    = rs.getObject("score");
    boolean empty = rs.isNull("name");
}
```

- Column indexes are **1-based**, as in JDBC.
- `getInt`/`getLong`/`getDouble` coerce any numeric value; `getString`
  returns `String.valueOf` of any non-null value; `getBoolean` requires a
  boolean. A typed getter on a `NULL` throws (`column x is NULL`); use
  `isNull` or `getObject` first.
- An unknown column name throws `no such column`.

A `CALL` whose procedure body `EMIT`s several result sets comes back as one
`ResultSet` with more behind it:

```java
Skaidb.ResultSet rs = conn.query("CALL report()");
do {
    while (rs.next()) { /* ... */ }
} while (rs.nextResultSet());
```

## Streaming large results

`conn.stream(sql)` returns a `Skaidb.RowStream`: the server sends the result
in chunks (about 256 KB each) and the driver holds one chunk at a time, so
a table of any size can be exported without materialising it — and without
hitting the server's scan budgets for one-shot queries.

```java
try (Skaidb.RowStream s = conn.stream("SELECT id, name FROM events")) {
    String[] cols = s.getColumnNames();
    while (s.next()) {
        Object id   = s.getObject(0);        // by 0-based index
        Object name = s.getObject("name");   // by name
    }
}
```

`RowStream.getObject(int)` is **0-based** (unlike `ResultSet`), and values
come back as the raw mapped objects — there are no typed getters. A stream
takes no parameters; build the SQL text yourself or use a prepared `Query`
for a non-streamed result.

### The abandon/drain rule

The protocol allows one exchange per connection at a time, and a stream is
one long exchange. While a `RowStream` is open **it owns the connection**:

- any other statement on that connection throws
  `connection is busy streaming: ...` instead of silently desynchronising;
- `conn.isUsable()` reports `false`, so a `Pool` will not lend the
  connection out;
- reading to the end (`next()` returned `false`) releases the connection
  immediately, whether or not you close the stream.

Abandoning a stream early — breaking out of the loop, or an exception — is
fine **as long as you close it**, which is what try-with-resources does.
`close()` reads and discards the rest of the stream so the socket is left
at a request boundary. If the remainder is large (more than 8 MB) or the
drain fails, the connection is marked broken instead: the pool drops it,
and a directly held connection transparently re-dials on its next statement.
Either way a desynchronised connection is never handed on. Java cannot drain
a socket for you when the stream is garbage collected, so an unclosed,
unfinished stream leaves that connection unusable until it is closed.

A stream that fails part-way throws from `next()`; the rows read so far
are valid.

## Batches, transactions and prepared statements

**Batches.** `Query.executeBatch(List<Object[]> rows)` executes one prepared
statement once per row in a **single round-trip** and returns the total
affected count:

```java
List<Object[]> rows = new ArrayList<>();
for (int i = 0; i < 10_000; i++) rows.add(new Object[] { (long) i, "user" + i });
long n = conn.prepare("INSERT INTO users (id, name) VALUES (?, ?)").executeBatch(rows);
```

Each row autocommits on its own: if one fails the server names it, earlier
rows stay applied, and the batch throws. Make the statement idempotent so
a retry is safe. Every row must carry exactly the statement's parameter count.

**Transactions.** skaidb has no multi-statement transactions from the
driver: every statement is atomic and auto-committed. Use conditional
updates (compare-and-set style `UPDATE ... WHERE`) and batches for
multi-row work; see the server documentation for the guarantees each
consistency level gives.

**Prepared statements** are implicit: `conn.prepare(sql)` with parameters
prepares server-side and caches the id on that connection, so reusing the
same SQL text costs one round-trip per execution. There is no explicit
`PreparedStatement` object to manage or close.

## Connection pooling

A `Connection` serialises every statement through one socket, so threads
sharing a connection queue behind each other. `Skaidb.Pool` gives each
worker its own:

```java
try (Skaidb.Pool pool = new Skaidb.Pool("skaidb://u:p@h1:7000,h2:7000/app", 8)) {
    long n = pool.withConnection(c -> {
        Skaidb.ResultSet rs = c.query("SELECT count(*) AS n FROM t");
        rs.next();
        return rs.getLong("n");
    });

    Skaidb.Connection c = pool.acquire();     // manual form
    try { c.execute("..."); } finally { pool.release(c); }
}
```

- `new Pool(dsn)` keeps up to 10 idle connections; `new Pool(dsn, maxsize)`
  sets that bound. `maxsize` bounds the connections kept **idle**, not the
  number checked out: a burst opens extras and the surplus is closed on
  return.
- Pooled connections come from `Skaidb.connect(dsn)`, so they inherit seed
  failover, TLS, consistency and the session database.
- `acquire()` returns only connections that pass `isUsable()`; `release()`
  closes a broken or still-streaming connection instead of parking it.
- `close()` closes the pool and every idle connection; a later `acquire()`
  throws `pool is closed`.

## Change streams (`subscribe`)

After `CREATE STREAM s ON t` on the server, `Connection.subscribe` follows
the stream's log and hands every change to a handler, blocking until the
handler returns `false`:

```java
conn.subscribe("s", null, ev -> {
    System.out.println(ev.id + " " + ev.op + " key=" + ev.key + " ts=" + ev.ts + " doc=" + ev.doc);
    return true;                      // false stops
});
```

`Event.id` is the log position: persist the last one and pass it as `after`
to resume exactly where you stopped, across restarts. `op` is the change
kind, `key` the row key, `ts` the change timestamp and `doc` the row image,
all as the mapped Java objects. This helper **polls** (pages of 500, 500 ms
idle sleep); for push delivery subscribe to `$stream/<db>/<name>` with any
MQTT client — the events are identical. Interrupting the thread returns.

## Type mapping

Values you bind (`setObject`, `setX`, batch rows) and values you read back
map as follows.

| skaidb | Bind from Java | Read as Java |
|--------|----------------|--------------|
| Null | `null` | `null` |
| Bool | `Boolean` | `Boolean` |
| Int | `Byte`, `Short`, `Integer`, `Long` | `Long` |
| Float | `Float`, `Double` (NaN/Infinity refused) | `Double` |
| Decimal | `java.math.BigDecimal` (mantissa up to 128 bits) | `java.math.BigDecimal` |
| String | `String`, any `CharSequence` | `String` |
| Bytes | `byte[]` | `byte[]` |
| Uuid | `java.util.UUID` | `java.util.UUID` |
| Timestamp | `java.time.Instant` (millisecond precision) | `java.time.Instant` |
| Array | `java.util.Collection`, any Java array | `java.util.List<Object>` |
| Document | `java.util.Map<String, ?>` (string keys) | `java.util.LinkedHashMap<String, Object>` (insertion order kept) |

Arrays and documents nest freely. Any other Java type throws
`cannot bind value of type ...`. The encoding is specified in §4 of the
[protocol document](https://skaidb.org/docs/PROTOCOL.html).

## Errors and reconnection

Everything throws `Skaidb.SkaidbException`, an **unchecked**
`RuntimeException` with a message and, for transport failures, a cause.
It is one class; branch on the message if you must:

| Situation | Message starts with | Connection afterwards |
|-----------|---------------------|-----------------------|
| DSN malformed | `bad DSN:`, `DSN scheme must be skaidb://`, `DSN has no host`, `bad consistency` | never opened |
| No seed reachable | `no reachable endpoint in ...` | never opened |
| Wrong credentials | `authentication denied:` | closed |
| Server signature wrong (MITM) | `server signature mismatch` | closed |
| TLS setup problem | `TLS setup failed:` | never opened |
| The server rejected a statement | the server's own text, e.g. a syntax or constraint error | **usable** |
| Parameter misuse | `parameter index ... out of range`, `statement expects N parameters`, `cannot bind ...` | usable |
| A `NULL` read through a typed getter | `column x is NULL` | usable |
| Another statement while streaming | `connection is busy streaming` | usable once the stream ends |
| Transport failure mid-statement | `query failed:`, `stream read failed:`, `prepare failed:` | **broken**: re-dials on the next statement |
| Used after `close()` | `connection is closed` | closed |

A statement error never damages the connection. A transport error marks it
broken: the next statement re-dials through the seed list, re-authenticates,
re-enters the session database and clears the prepared-statement cache —
a recovered connection is indistinguishable from a fresh one. The failed
statement itself is **not retried**: it may already have executed, and an
ambiguous write must never be repeated silently. Retry at the call site
where you know whether that is safe. `isUsable()` tells a pool (or you)
whether a connection is closed, broken or mid-stream.

## Threading

- A `Connection` may be shared by threads: statements are serialised on
  its monitor, one at a time. For parallelism use a `Pool`.
- `setConsistency` on the connection is a shared field; prefer
  `Query.setConsistency` from shared connections.
- A `RowStream` is not thread-safe: one stream, one reader. It may be
  closed from another thread.
- `Pool` is thread-safe.
- `subscribe` blocks its calling thread.

## Versions and compatibility

- The driver has its own version series, starting at **1.0.0**, independent
  of the server's. `Skaidb.VERSION` (or `Skaidb.version()`) reports it; the
  same string is announced to the server in the Hello frame and shows up in
  the server's `drivers` table. It is derived from the jar manifest's
  `Implementation-Version`, falling back to a build-time
  `version.properties`, so it always equals the artifact's version.
- Works with any current skaidb server. Server-side prepared statements
  need server ≥ 0.17.0; older servers fall back to client-side text
  binding automatically (typed values then cannot be bound). Streaming
  (`stream`) and Hello need a server that knows those opcodes; on an older
  one `stream` throws `server does not support streaming` and the Hello
  reply is ignored.
- Java 11 or newer (`maven.compiler.release` is 11). CI builds and tests on
  Temurin 11, 17 and 21.

## Building and testing

```sh
./mvnw -B verify     # compiles with -Xlint:all (warnings fail the build), runs the tests,
                     # packages the jar plus -sources.jar and -javadoc.jar in target/
                     # (the wrapper fetches Maven 3.9.9; a local mvn >= 3.6.3 works too)
```

The tests need no server: `SkaidbTest` covers the pure functions
(placeholder counting, client-side binding, value encode/decode against the
wire spec, DSN parsing, `ResultSet`), and `FakeServerTest` runs an
in-process fake speaking just enough of the protocol to exercise the SCRAM
handshake, the Hello frame, prepared statements, batches, multiple result
sets, streaming, the abandon/drain rule, reconnection and the pool.

Releases: bump `<version>` in `pom.xml`, add a CHANGELOG entry, tag `vX.Y.Z`
and push the tag. GitHub Actions then runs the tests, publishes the GitHub
Release with the jars attached and has JitPack build the tag; see
[docs/RELEASING.md](docs/RELEASING.md).

## License

[SSPL-1.0](LICENSE), the same license as skaidb itself.
