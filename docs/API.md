# skaidb-java API reference

Every type is a nested class of `com.skaidb.Skaidb`. Signatures are
verbatim from `Skaidb.java`; the README explains how to use them. The JDBC
driver (`com.skaidb.jdbc`) implements the standard `java.sql` interfaces and
is described in [JDBC.md](JDBC.md).

## `Skaidb`

```java
public static final String VERSION;                  // the driver version, e.g. "1.3.0"
public static String version();                      // same, as a method

public static final int CONSISTENCY_ONE    = 0;
public static final int CONSISTENCY_QUORUM = 1;      // default
public static final int CONSISTENCY_ALL    = 2;

public static final String AUTH_SCRAM       = "scram";        // default
public static final String AUTH_CERTIFICATE = "certificate";  // TLS client certificate (EXTERNAL)

public static Connection connect(String dsn);
public static Connection connect(ConnectOptions options);
public static Connection connect(String host, int port, String user, String password);
```

`connect(dsn)` accepts
`skaidb://[user[:password]@]host[:port][,host2[:port]...][/database][?options]`
with options `consistency=one|quorum|all`, `tls=true`, `tls_ca=<pem path>`,
`tls_insecure=true`, `tls_server_name=<name>`, `tls_client_cert=<pem path>`,
`tls_client_key=<pem path>`, `auth_mechanism=scram|certificate`. It dials
immediately and throws `SkaidbException` if no seed connects and
authenticates.

`connect(host, port, user, password)` is the single-host form: QUORUM, no
TLS, no session database.

## `Skaidb.ConnectOptions`

```java
public ConnectOptions();
public ConnectOptions host(String host, int port);      // add a seed
public ConnectOptions host(String hostPort);            // "h" (port 7000) or "h:port"
public ConnectOptions user(String user);                // unset: "anonymous" (SCRAM) or empty (certificate)
public ConnectOptions password(String password);
public ConnectOptions database(String database);
public ConnectOptions consistency(int level);
public ConnectOptions consistency(String level);        // "one" | "quorum" | "all"
public ConnectOptions tls(boolean tls);
public ConnectOptions tlsCa(String pemPath);            // implies TLS
public ConnectOptions tlsInsecure(boolean insecure);    // implies TLS
public ConnectOptions tlsServerName(String name);       // default "skaidb"
public ConnectOptions tlsClientCert(String pemPath);    // implies TLS; needs tlsClientKey
public ConnectOptions tlsClientKey(String pemPath);     // PKCS#8 or PKCS#1 RSA, unencrypted
public ConnectOptions authMechanism(String mechanism);  // AUTH_SCRAM | AUTH_CERTIFICATE
```

Every setter returns `this`. `connect(options)` copies the options, so the
builder can be reused. Certificate login without a client certificate, or
a certificate without its key, throws before dialling. In certificate mode
an unset user is sent as an empty name (the server takes the certificate's
CN); a set one must equal the CN.

## `Skaidb.SkaidbException extends RuntimeException`

```java
public SkaidbException(String message);
public SkaidbException(String message, Throwable cause);
```

The one exception type. Unchecked. Transport failures carry the underlying
`IOException` as cause.

## `Skaidb.Connection implements AutoCloseable`

```java
public Connection setConsistency(int level);          // ONE|QUORUM|ALL for later statements; returns this
public Query      prepare(String sql);                // '?' placeholders, 1-based setters
public long       execute(String sql);                // affected rows, or -1 for DDL
public ResultSet  query(String sql);                  // a SELECT/CALL with no parameters
public Result     executeRaw(String sql);             // SQL verbatim ('?' is not a placeholder); any answer
public RowStream  stream(String sql);                 // chunked result; owns the connection until done
public RowStream  stream(String sql, Object... params); // the same, '?' bound to params (server >= 0.330)
public void       subscribe(String stream, String after, EventHandler handler);
public boolean    isUsable();                         // !closed && !broken && !streaming
public long       reconnects();                       // re-dials so far (session state does not survive one)
public void       setReadTimeout(int millis);         // bound every read; 0 = none (default)
public int        getReadTimeout();
public void       close();                            // idempotent
```

Semantics:

- Statements are serialised on the connection's monitor.
- `execute` and `query` are shorthands for `prepare(sql).executeUpdate()` /
  `.executeQuery()` with zero parameters (they go through `OP_QUERY`, never
  server-side prepare).
- `stream` throws `connection is busy streaming` if a previous `RowStream`
  is still open, `connection is closed` after `close()`, and
  `server does not support streaming` on a server without the opcode. A
  non-row statement passed to `stream` executes normally and yields a
  stream with no columns whose `getAffected()` is the affected count.
- `stream(sql, params...)` binds the `?` placeholders in order, typed (any
  value `Query.setObject` takes), and sends them with the SQL in
  `OP_QUERY_STREAM_PARAMS`, server ≥ 0.330; an older server answers
  `server does not support parameterised streaming (skaidb >= 0.330): ...`
  and the connection stays usable. Without parameters (`stream(sql)`, an
  empty or `null` array) the request is `OP_QUERY_STREAM`. The statement
  must be a kind the server prepares (not DDL or a session statement). An
  `Object[]` passed alone is the parameter list; a `List` binds as one
  array value. A value that cannot be bound throws before anything is
  sent. A stream runs at the connection's consistency level.
- A read that exceeds `setReadTimeout` throws and marks the connection
  broken, like any transport error.
- A transport error marks the connection broken; the next statement
  re-dials (seed failover, handshake, Hello, `USE`), clears the prepared
  cache, and only then runs. The failed statement is not retried.
- `subscribe` blocks until the handler returns `false` or the thread is
  interrupted. It polls `_stream_<name>` in pages of 500 with a 500 ms idle
  sleep.

### `Skaidb.Connection.Event`

```java
public final String id;    // log position; pass as `after` to resume
public final String op;    // change kind
public final Object key;   // row key
public final Object ts;    // change timestamp
public final Object doc;   // row image
```

### `Skaidb.Connection.EventHandler`

```java
public interface EventHandler { boolean handle(Event ev); }   // false = stop
```

## `Skaidb.Query implements AutoCloseable`

```java
public Query setConsistency(int level);   // this statement only; the connection's level is untouched
public Query setInt(int i, int v);        // all setters: 1-based index, return this
public Query setLong(int i, long v);
public Query setDouble(int i, double v);
public Query setBoolean(int i, boolean v);
public Query setString(int i, String v);
public Query setNull(int i);
public Query setObject(int i, Object v);  // any type in the mapping table; null allowed

public ResultSet executeQuery();          // empty ResultSet for a non-row statement
public long      executeUpdate();         // affected rows; -1 DDL; 0 if rows came back
public long      executeBatch(java.util.List<Object[]> rows);   // one round-trip; total affected
public Result    execute();               // rows, affected count or DDL, whichever it was
public int       getParameterCount();     // '?' placeholders outside string literals
public void      close();                 // no-op, for try-with-resources symmetry
```

- The placeholder count is fixed when the `Query` is created; an index
  outside `1..n` throws at `setX` time.
- With ≥ 1 placeholder the statement is prepared server-side (cached per
  connection, ≤ 240 entries) and parameters travel typed. If the server
  refuses to prepare it, the driver interpolates SQL-quoted text instead,
  where collections/maps/arrays are refused.
- `executeBatch` requires every row to have exactly the statement's
  parameter count; an empty list returns 0 without a round-trip. A
  statement the server refuses to prepare (DDL, `USE`) runs row by row with
  client-side binding.

## `Skaidb.Result`

```java
public boolean   hasRows();        // the statement returned a result set
public ResultSet getResultSet();   // null when it did not
public long      getAffected();    // a mutation's count; -1 for rows and DDL
public boolean   isDdl();          // neither rows nor a count
```

## `Skaidb.ResultSet`

```java
public boolean  next();                  // advance; false past the last row
public boolean  nextResultSet();         // switch to the next EMITted set of a CALL; false if none
public int      getRowCount();
public String[] getColumnNames();        // a copy

public Object   getObject(int col);      // 1-BASED
public Object   getObject(String name);
public String   getString(int col);      public String  getString(String name);   // null-safe
public int      getInt(int col);         public int     getInt(String name);      // numeric coercion
public long     getLong(int col);        public long    getLong(String name);
public double   getDouble(int col);      public double  getDouble(String name);
public boolean  getBoolean(int col);     public boolean getBoolean(String name);
public boolean  isNull(String name);
```

By-name typed getters throw `column <name> is NULL` on a null cell;
by-index typed getters throw a `NullPointerException`/`ClassCastException`
from the unboxing instead — prefer the by-name form or check `isNull`
first. Unknown names throw `no such column`.

## `Skaidb.RowStream implements AutoCloseable`

```java
public String[] getColumnNames();
public boolean  next();                  // pulls the next chunk when needed; false at RowsEnd
public Object   getObject(int i);        // 0-BASED
public Object   getObject(String name);
public long     getAffected();           // a non-row statement's affected count; -1 otherwise
public void     close();                 // drains the rest (≤ 8 MB) or marks the connection broken; idempotent
```

Not thread-safe. Reading to the end releases the connection before
`close()`; a server error mid-stream throws from `next()` after releasing.

## `Skaidb.Pool implements AutoCloseable`

```java
public interface Work<T> { T run(Connection conn); }

public Pool(String dsn);                                  // up to 10 idle, default balancing
public Pool(String dsn, int maxsize);                     // maxsize >= 1 bounds the IDLE set
public Pool(String dsn, PoolOptions options);
public Pool(ConnectOptions connect);
public Pool(ConnectOptions connect, PoolOptions options);

public Connection acquire();                              // under the pool-wide affinity key
public Connection acquire(Object affinity);               // idle to the picked member, else a fresh dial
public void       release(Connection c);                  // parks it if usable, its member usable and room remains; else closes it
public <T> T      withConnection(Work<T> work);
public <T> T      withConnection(Object affinity, Work<T> work);
public List<Member> members();                            // snapshot per member, ordered by endpoint
public void       close();                                // closes every idle connection, stops the background thread
```

Thread-safe. A malformed DSN throws from the constructor. The probe and
discovery refresh run on one daemon thread per pool, started on first need.
Balancing follows
[PROTOCOL.md §6.1](https://skaidb.org/docs/PROTOCOL.html#61-balancing-a-pool-across-members);
the README's [Balancing across members](../README.md#balancing-across-members)
explains each option.

### `Skaidb.Pool.Member`

```java
public final String  endpoint;   // what the pool dials, host:port
public final String  canonical;  // the member's own SHOW ENDPOINTS name; null until discovered
public final String  state;      // ready | ejected | resyncing | unreachable | unlisted
public final Double  latencyMs;  // round-trip average; null until measured
public final int     inUse;      // checked out now
public final int     idle;       // idle in the pool
public final boolean seed;       // a configured endpoint (never dropped by discovery)
```

### `Skaidb.PoolOptions`

A mutable builder; every setter returns `this` and refuses an invalid value
with `SkaidbException`.

```java
public PoolOptions maxSize(int maxSize);                   // 10; >= 1
public PoolOptions loadBalancing(LoadBalancing policy);    // RANDOM
public PoolOptions loadBalancing(String policy);           // "random" | "round_robin" | "least_latency"
public PoolOptions idleOrder(IdleOrder order);             // LIFO
public PoolOptions idleOrder(String order);                // "lifo" | "fifo"
public PoolOptions discover(boolean discover);             // false
public PoolOptions refreshInterval(Duration interval);     // 60 s (5 s while a member is not ready)
public PoolOptions readYourWrites(Duration window);        // zero = off
public PoolOptions ejectErrors(int n);                     // 3; >= 1
public PoolOptions ejectLatencyFactor(double factor);      // 3.0; 0 disables
public PoolOptions ejectLatencyFloor(Duration floor);      // 50 ms
public PoolOptions ejectBackoff(Duration backoff);         // 5 s, doubling per repeat
public PoolOptions ejectBackoffMax(Duration max);          // 60 s
```

### `Skaidb.LoadBalancing`, `Skaidb.IdleOrder`

```java
public enum LoadBalancing { RANDOM, ROUND_ROBIN, LEAST_LATENCY;
    public static LoadBalancing parse(String name); }      // case-insensitive
public enum IdleOrder { LIFO, FIFO;
    public static IdleOrder parse(String name); }
```

## Wire protocol

The driver implements the skaidb binary protocol on port 7000: framing,
SCRAM-SHA-256 handshake (with mutual verification when a password is set),
the EXTERNAL (TLS client certificate) handshake,
`OP_QUERY` (1), prepare/execute (2–4), streaming (5–7), batch (7), Hello
(8), streaming with parameters (9) and the `ResultSets` reply (8). The
specification is published at <https://skaidb.org/docs/PROTOCOL.html>.
