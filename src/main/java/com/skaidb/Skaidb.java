package com.skaidb;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;

/**
 * Official skaidb driver for Java. The API is modeled on JDBC — {@code connect},
 * {@code prepare}, {@code setInt}/{@code setString}, {@code executeQuery}/
 * {@code executeUpdate}, and a {@code ResultSet} with {@code next()}/{@code getX}
 * — so a JDBC user has essentially nothing new to learn. Pure JDK: no
 * third-party dependencies.
 *
 * <pre>{@code
 * try (Skaidb.Connection conn = Skaidb.connect("skaidb://user:pass@localhost:7000")) {
 *     conn.execute("CREATE TABLE users (PRIMARY KEY (id))");
 *     try (Skaidb.Query q = conn.prepare("INSERT INTO users (id, name) VALUES (?, ?)")) {
 *         q.setInt(1, 1).setString(2, "Ada").executeUpdate();
 *     }
 *     Skaidb.ResultSet rs = conn.prepare("SELECT id, name FROM users WHERE id = ?")
 *                               .setInt(1, 1).executeQuery();
 *     while (rs.next()) System.out.println(rs.getInt("id") + " " + rs.getString("name"));
 * }
 * }</pre>
 */
public final class Skaidb {

    private Skaidb() {}

    /**
     * The driver's own version, as reported to the server in the Hello frame
     * (the {@code drivers} table's client_version). Derived from the package
     * metadata so it always equals the published artifact version: the jar
     * manifest's Implementation-Version first, then the build-time
     * {@code version.properties} resource, and only then the literal below.
     */
    public static final String VERSION = detectVersion();

    private static final String FALLBACK_VERSION = "1.1.0";

    private static String detectVersion() {
        try {
            Package pkg = Skaidb.class.getPackage();
            String v = pkg == null ? null : pkg.getImplementationVersion();
            if (v != null && !v.isEmpty()) return v;
            try (java.io.InputStream in = Skaidb.class.getResourceAsStream("version.properties")) {
                if (in != null) {
                    java.util.Properties props = new java.util.Properties();
                    props.load(in);
                    v = props.getProperty("version");
                    if (v != null && !v.isEmpty() && !v.startsWith("$")) return v;
                }
            }
        } catch (IOException | RuntimeException e) {
            // fall through: a build without metadata still identifies itself
        }
        return FALLBACK_VERSION;
    }

    /** {@link #VERSION}, as a method for callers that prefer one. */
    public static String version() { return VERSION; }

    public static final int CONSISTENCY_ONE = 0;
    public static final int CONSISTENCY_QUORUM = 1;
    public static final int CONSISTENCY_ALL = 2;

    /**
     * Connect using a {@code skaidb://user:pass@host:port/db?consistency=quorum}
     * URL. See {@link #parseDsn} for every accepted component.
     */
    public static Connection connect(String dsn) {
        return new Connection(parseDsn(dsn));
    }

    /** Connect with explicit {@link ConnectOptions} (no URL quoting involved). */
    public static Connection connect(ConnectOptions options) {
        return new Connection(options.copy().validate());
    }

    /** Authenticate with SCRAM-SHA-256 (user name + password). The default. */
    public static final String AUTH_SCRAM = "scram";
    /**
     * Authenticate with the TLS client certificate (wire mechanism EXTERNAL):
     * the certificate's Common Name is the user, no password is sent.
     */
    public static final String AUTH_CERTIFICATE = "certificate";

    /**
     * Everything a connection needs, as a mutable builder. {@link #parseDsn}
     * produces one from a URL; {@link Skaidb#connect(ConnectOptions)} dials it.
     * Values are taken verbatim, so passwords need no URL escaping here.
     *
     * <pre>{@code
     * Skaidb.connect(new Skaidb.ConnectOptions()
     *         .host("db1", 7000).host("db2", 7000)
     *         .user("app").password("s3cret").database("orders")
     *         .tlsCa("/etc/skaidb/ca.pem"));
     * }</pre>
     */
    public static final class ConnectOptions {
        List<String> seeds = new ArrayList<>();
        /** Null = not given: "anonymous" for SCRAM, empty for certificate login. */
        String user;
        String password = "";
        String database = "";
        int consistency = CONSISTENCY_QUORUM;
        boolean tls = false;
        String tlsCa = "";
        boolean tlsInsecure = false;
        String tlsServerName = "skaidb";
        String tlsClientCert = "";
        String tlsClientKey = "";
        String authMechanism = AUTH_SCRAM;

        public ConnectOptions() {}

        /** Add a seed endpoint. Seeds are dialled in shuffled order until one authenticates. */
        public ConnectOptions host(String host, int port) {
            if (host == null || host.isEmpty()) throw new SkaidbException("empty host");
            seeds.add(host + ":" + port);
            return this;
        }
        /** Add a seed as {@code host} or {@code host:port} (port 7000 when omitted). */
        public ConnectOptions host(String hostPort) {
            String h = hostPort == null ? "" : hostPort.trim();
            if (h.isEmpty()) throw new SkaidbException("empty host");
            seeds.add(hasPort(h) ? h : h + ":7000");
            return this;
        }
        public ConnectOptions user(String user) { this.user = user; return this; }
        public ConnectOptions password(String password) { this.password = password == null ? "" : password; return this; }
        /** Session database, entered with {@code USE} after every connect and reconnect. */
        public ConnectOptions database(String database) { this.database = database == null ? "" : database; return this; }
        /** Default consistency: {@link #CONSISTENCY_ONE}, {@link #CONSISTENCY_QUORUM} or {@link #CONSISTENCY_ALL}. */
        public ConnectOptions consistency(int level) {
            if (level < 0 || level > 2) throw new SkaidbException("bad consistency " + level);
            this.consistency = level;
            return this;
        }
        /** {@code one}, {@code quorum} or {@code all}. */
        public ConnectOptions consistency(String level) { this.consistency = parseConsistency(level); return this; }
        /** Encrypt with TLS, verifying the server against the JVM trust store (or {@link #tlsCa}). */
        public ConnectOptions tls(boolean tls) { this.tls = tls; return this; }
        /** Trust only the CA certificate(s) in this PEM file. Implies TLS. */
        public ConnectOptions tlsCa(String pemPath) { this.tlsCa = pemPath == null ? "" : pemPath; return this; }
        /** Encrypt but verify nothing. Development only. Implies TLS. */
        public ConnectOptions tlsInsecure(boolean insecure) { this.tlsInsecure = insecure; return this; }
        /** SNI name and the name the server certificate must carry (default {@code skaidb}). */
        public ConnectOptions tlsServerName(String name) { this.tlsServerName = name; return this; }
        /**
         * Present this client certificate (PEM, leaf first, optionally followed
         * by its chain) in the TLS handshake. Implies TLS; needs {@link #tlsClientKey}.
         */
        public ConnectOptions tlsClientCert(String pemPath) { this.tlsClientCert = pemPath == null ? "" : pemPath; return this; }
        /**
         * The client certificate's private key: an unencrypted PEM file,
         * PKCS#8 ({@code BEGIN PRIVATE KEY}, RSA / EC / Ed25519) or PKCS#1
         * ({@code BEGIN RSA PRIVATE KEY}).
         */
        public ConnectOptions tlsClientKey(String pemPath) { this.tlsClientKey = pemPath == null ? "" : pemPath; return this; }
        /** {@link #AUTH_SCRAM} (default) or {@link #AUTH_CERTIFICATE}. */
        public ConnectOptions authMechanism(String mechanism) {
            String m = mechanism == null ? "" : mechanism.toLowerCase(java.util.Locale.ROOT);
            if (m.isEmpty()) m = AUTH_SCRAM;
            if (!m.equals(AUTH_SCRAM) && !m.equals(AUTH_CERTIFICATE))
                throw new SkaidbException("unknown auth_mechanism " + mechanism + " (use scram or certificate)");
            this.authMechanism = m;
            return this;
        }

        ConnectOptions copy() {
            ConnectOptions c = new ConnectOptions();
            c.seeds = new ArrayList<>(seeds);
            c.user = user; c.password = password; c.database = database;
            c.consistency = consistency; c.tls = tls; c.tlsCa = tlsCa;
            c.tlsInsecure = tlsInsecure; c.tlsServerName = tlsServerName;
            c.tlsClientCert = tlsClientCert; c.tlsClientKey = tlsClientKey;
            c.authMechanism = authMechanism;
            return c;
        }

        /** Derive the implied settings and refuse contradictory ones. */
        ConnectOptions validate() {
            if (seeds.isEmpty()) throw new SkaidbException("no host given");
            if (tlsServerName == null || tlsServerName.isEmpty()) tlsServerName = "skaidb";
            if (tlsClientCert.isEmpty() != tlsClientKey.isEmpty())
                throw new SkaidbException("tls_client_cert and tls_client_key go together");
            tls = tls || !tlsCa.isEmpty() || tlsInsecure || !tlsClientCert.isEmpty();
            if (authMechanism.equals(AUTH_CERTIFICATE) && tlsClientCert.isEmpty())
                throw new SkaidbException(
                    "certificate authentication needs TLS with a client certificate "
                    + "(tls_client_cert and tls_client_key)");
            if (user == null) user = authMechanism.equals(AUTH_CERTIFICATE) ? "" : "anonymous";
            return this;
        }
    }

    private static boolean hasPort(String h) {
        // [v6]:port, host:port; a bare IPv6 literal has several colons
        if (h.startsWith("[")) return h.contains("]:");
        return h.indexOf(':') >= 0 && h.indexOf(':') == h.lastIndexOf(':');
    }

    /**
     * Decode a DSN without dialling. {@code skaidb://[user[:pass]@]host[:port][,host2[:port]...][/db][?options]}
     * with options {@code consistency=one|quorum|all}, {@code tls=true},
     * {@code tls_ca=/path/ca.pem}, {@code tls_insecure=true},
     * {@code tls_server_name=name}, {@code tls_client_cert=/path/client.pem},
     * {@code tls_client_key=/path/client.key},
     * {@code auth_mechanism=scram|certificate}.
     */
    static ConnectOptions parseDsn(String dsn) {
        try {
            URI u = URI.create(dsn);
            if (!"skaidb".equals(u.getScheme()))
                throw new SkaidbException("DSN scheme must be skaidb://");
            // A comma-separated seed list is not a legal URI host, so
            // java.net.URI cannot decompose the authority — getUserInfo() and
            // getHost() both come back null and credentials would silently
            // degrade to anonymous. Parse the authority ourselves.
            String authority = u.getAuthority() != null ? u.getAuthority() : u.getSchemeSpecificPart();
            authority = authority.replaceFirst("^//", "");
            int slash = authority.indexOf('/');
            if (slash >= 0) authority = authority.substring(0, slash);
            ConnectOptions o = new ConnectOptions();
            int at = authority.lastIndexOf('@');
            if (at >= 0) {
                String ui = authority.substring(0, at);
                authority = authority.substring(at + 1);
                String[] up = ui.split(":", 2);
                o.user = up[0];
                if (up.length > 1) o.password = up[1];
            }
            // Session database from the URL path: skaidb://host:7000/app
            o.database = u.getPath() == null ? "" : u.getPath().replaceFirst("^/", "");
            String q = u.getQuery();
            if (q != null) {
                for (String part : q.split("&")) {
                    if (part.startsWith("consistency=")) {
                        o.consistency = parseConsistency(part.substring("consistency=".length()));
                    } else if (part.startsWith("tls_ca=")) {
                        o.tlsCa = part.substring("tls_ca=".length());
                    } else if (part.startsWith("tls_server_name=")) {
                        o.tlsServerName = part.substring("tls_server_name=".length());
                    } else if (part.startsWith("tls_client_cert=")) {
                        o.tlsClientCert = part.substring("tls_client_cert=".length());
                    } else if (part.startsWith("tls_client_key=")) {
                        o.tlsClientKey = part.substring("tls_client_key=".length());
                    } else if (part.startsWith("auth_mechanism=")) {
                        o.authMechanism(part.substring("auth_mechanism=".length()));
                    } else if (part.equals("tls_insecure=true") || part.equals("tls_insecure=1")) {
                        o.tlsInsecure = true;
                    } else if (part.equals("tls=true") || part.equals("tls=1")) {
                        o.tls = true;
                    }
                }
            }
            // Seeds: skaidb://user:pass@h1:7000,h2:7000,h3/db
            for (String h : authority.split(",")) {
                h = h.trim();
                if (h.isEmpty()) continue;
                o.seeds.add(h.contains(":") ? h : h + ":7000");
            }
            if (o.seeds.isEmpty()) throw new SkaidbException("DSN has no host");
            return o.validate();
        } catch (IllegalArgumentException e) {
            throw new SkaidbException("bad DSN: " + e.getMessage());
        }
    }

    /** Connect to one host explicitly: QUORUM consistency, no TLS, no session database. */
    public static Connection connect(String host, int port, String user, String password) {
        return connect(new ConnectOptions().host(host, port).user(user).password(password));
    }

    static int parseConsistency(String s) {
        switch (s.toLowerCase(java.util.Locale.ROOT)) {
            case "one": return CONSISTENCY_ONE;
            case "all": return CONSISTENCY_ALL;
            case "":
            case "quorum": return CONSISTENCY_QUORUM;
            default: throw new SkaidbException("bad consistency " + s);
        }
    }

    /** Thrown on connection, protocol, or statement errors. */
    public static final class SkaidbException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public SkaidbException(String message) { super(message); }
        public SkaidbException(String message, Throwable cause) { super(message, cause); }
    }

    // ---- Connection --------------------------------------------------------

    public static final class Connection implements AutoCloseable {
        private static int nonceCounter = 0;
        private Socket socket;
        private DataInputStream in;
        private OutputStream out;
        private int consistency;
        // isUsable() is a pool's health check and must answer while another
        // thread holds this connection's monitor for a running statement, so
        // the three state flags are volatile rather than monitor-guarded.
        // Writes still happen under the monitor wherever check-then-set has
        // to be atomic (see stream()).
        private volatile boolean closed = false;
        /**
         * Transport died, or a stream left the wire at an unknown position;
         * the next statement re-dials (see ensureLive).
         */
        private volatile boolean broken = false;
        /** A RowStream owns the wire until it ends; see requireIdle(). */
        private volatile boolean streaming = false;
        private final java.util.Map<String, long[]> prepared = new java.util.HashMap<>();
        // Retained so a reconnect can repeat the original connect exactly.
        private final ConnectOptions opts;
        /** SO_TIMEOUT for every read, 0 = none; re-applied on each re-dial. */
        private volatile int readTimeoutMs = 0;
        /** How many times ensureLive re-dialled; see {@link #reconnects()}. */
        private volatile long reconnects = 0;

        Connection(ConnectOptions opts) {
            this.opts = opts;
            this.consistency = opts.consistency;
            dial();
        }

        /**
         * Connect, authenticate and enter the session database. Used for the
         * first connect and for every reconnect, so a recovered connection is
         * indistinguishable from a fresh one.
         */
        private void dial() {
            // Try each seed until one connects AND authenticates — a node that
            // accepts TCP while unhealthy must not swallow the attempt. skaidb
            // is leaderless, so any node serves; shuffled so many clients
            // spread instead of stampeding the first entry.
            java.util.List<String> order = new java.util.ArrayList<>(opts.seeds);
            java.util.Collections.shuffle(order);
            Socket connected = null;
            Exception last = null;
            for (String ep : order) {
                int c = ep.lastIndexOf(':');
                String h = c > 0 ? ep.substring(0, c) : ep;
                int p = c > 0 ? Integer.parseInt(ep.substring(c + 1)) : 7000;
                try {
                    Socket s = new Socket();
                    s.connect(new InetSocketAddress(h, p), 10_000);
                    s.setTcpNoDelay(true);
                    if (opts.tls) s = tlsWrap(s, p, opts);
                    s.setSoTimeout(readTimeoutMs);
                    connected = s;
                    break;
                } catch (Exception e) {
                    last = e;
                }
            }
            if (connected == null) {
                throw new SkaidbException("no reachable endpoint in " + String.join(", ", order)
                        + (last == null ? "" : ": " + last.getMessage()), last);
            }
            try {
                socket = connected;
                in = new DataInputStream(socket.getInputStream());
                out = socket.getOutputStream();
                if (opts.authMechanism.equals(AUTH_CERTIFICATE)) handshakeCertificate(opts.user);
                else handshake(opts.user, opts.password);
            } catch (IOException | RuntimeException e) {
                // A refused login must not leave the socket open behind it.
                try { socket.close(); } catch (IOException ignored) {}
                if (e instanceof SkaidbException) throw (SkaidbException) e;
                throw new SkaidbException("connect failed: " + e.getMessage(), e);
            }
            sendHello();
            // USE is per-connection session state, so it runs on every dial.
            if (opts.database != null && !opts.database.isEmpty()) {
                execute("USE \"" + opts.database.replace("\"", "\"\"") + "\"");
            }
        }

        /**
         * Best-effort self-identification: fills the server's {@code drivers}
         * table client_name/client_version. An old server answers the unknown
         * opcode with an error frame, which is ignored — identity is
         * telemetry, never load-bearing.
         */
        private void sendHello() {
            try {
                byte[] name = "java".getBytes(StandardCharsets.UTF_8);
                byte[] ver = VERSION.getBytes(StandardCharsets.UTF_8);
                Buf req = new Buf();
                req.u8(8).u32(name.length).raw(name).u32(ver.length).raw(ver);
                writeFrame(req.toBytes());
                readFrame();
            } catch (IOException | SkaidbException e) {
                // telemetry only
            }
        }

        /**
         * Upgrade a connected socket to TLS. A server with client_tls =
         * required refuses plaintext outright, so without this such a cluster
         * is simply unreachable. The SNI name must match a SAN on the server
         * certificate — skaidb's own certs carry DNS:skaidb, which is usually
         * NOT the address you dialled, hence the separate knob.
         */
        private static Socket tlsWrap(Socket raw, int port, ConnectOptions o) throws IOException {
            String caFile = o.tlsCa;
            boolean insecure = o.tlsInsecure;
            String serverName = o.tlsServerName;
            try {
                javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
                javax.net.ssl.TrustManager[] tm = null;
                if (insecure) {
                    // Encrypts, but authenticates nothing: a man in the middle
                    // can present any certificate. Development only.
                    tm = new javax.net.ssl.TrustManager[] { new javax.net.ssl.X509TrustManager() {
                        public void checkClientTrusted(java.security.cert.X509Certificate[] c, String t) {}
                        public void checkServerTrusted(java.security.cert.X509Certificate[] c, String t) {}
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[0];
                        }
                    } };
                } else if (caFile != null && !caFile.isEmpty()) {
                    java.security.KeyStore ks = java.security.KeyStore.getInstance(
                            java.security.KeyStore.getDefaultType());
                    ks.load(null, null);
                    java.security.cert.CertificateFactory cf =
                            java.security.cert.CertificateFactory.getInstance("X.509");
                    int i = 0;
                    try (java.io.InputStream fin = java.nio.file.Files.newInputStream(
                            java.nio.file.Paths.get(caFile))) {
                        for (java.security.cert.Certificate c : cf.generateCertificates(fin)) {
                            ks.setCertificateEntry("ca" + (i++), c);
                        }
                    }
                    if (i == 0) throw new SkaidbException("no certificates found in tls_ca " + caFile);
                    javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory
                            .getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(ks);
                    tm = tmf.getTrustManagers();
                }
                javax.net.ssl.KeyManager[] km = null;
                if (!o.tlsClientCert.isEmpty()) km = clientKeyManagers(o.tlsClientCert, o.tlsClientKey);
                ctx.init(km, tm, null);
                javax.net.ssl.SSLSocket ss = (javax.net.ssl.SSLSocket) ctx.getSocketFactory()
                        .createSocket(raw, serverName, port, true);
                ss.setUseClientMode(true);
                javax.net.ssl.SSLParameters params = ss.getSSLParameters();
                params.setServerNames(java.util.Collections.singletonList(
                        new javax.net.ssl.SNIHostName(serverName)));
                if (!insecure) params.setEndpointIdentificationAlgorithm("HTTPS");
                ss.setSSLParameters(params);
                ss.startHandshake();
                return ss;
            } catch (java.security.GeneralSecurityException e) {
                throw new SkaidbException("TLS setup failed: " + e.getMessage(), e);
            }
        }

        /** Override the consistency level for subsequent statements. */
        public Connection setConsistency(int level) {
            if (level < 0 || level > 2) throw new SkaidbException("bad consistency " + level);
            this.consistency = level;
            return this;
        }

        /** Prepare a parameterized statement (use {@code ?} placeholders). */
        public Query prepare(String sql) { return new Query(this, sql); }

        /** Run a statement with no parameters; returns affected rows, or -1 for DDL. */
        public long execute(String sql) { return new Query(this, sql).executeUpdate(); }

        /** Run a SELECT with no parameters. */
        public ResultSet query(String sql) { return new Query(this, sql).executeQuery(); }

        /**
         * Send {@code sql} verbatim as one statement ({@code ?} is not a
         * placeholder here) and report whichever kind of answer it produced.
         */
        public Result executeRaw(String sql) {
            Object res = run(sql, true);
            if (res instanceof ResultSet) return new Result((ResultSet) res, -1L);
            return new Result(null, (Long) res);
        }

        /** One change captured by a stream. */
        public static final class Event {
            /** Log position: keep the last one to resume. */
            public final String id;
            public final String op;
            public final Object key;
            public final Object ts;
            public final Object doc;

            Event(String id, String op, Object key, Object ts, Object doc) {
                this.id = id; this.op = op; this.key = key; this.ts = ts; this.doc = doc;
            }
        }

        /** What to do with each event; return false to stop subscribing. */
        public interface EventHandler { boolean handle(Event ev); }

        /**
         * Deliver a stream's events to {@code handler} as they arrive,
         * blocking until the handler returns false.
         *
         * <p>A dependency-free helper over the stream's log: it pages the log
         * with the keyset cursor. {@code Event.id} is the position — keep the
         * last one and pass it as {@code after} to resume exactly where you
         * stopped, across restarts.
         *
         * <p>This polls; for push delivery subscribe to
         * {@code $stream/<db>/<name>} with any MQTT client instead. The
         * events are identical.
         */
        public void subscribe(String stream, String after, EventHandler handler) {
            String log = "_stream_" + stream;
            String cur = after;
            while (true) {
                ResultSet rs;
                if (cur == null) {
                    rs = prepare("SELECT id, op, k, ts, doc FROM " + log
                            + " ORDER BY id LIMIT 500").executeQuery();
                } else {
                    rs = prepare("SELECT id, op, k, ts, doc FROM " + log
                            + " WHERE id > ? ORDER BY id LIMIT 500").setString(1, cur).executeQuery();
                }
                int n = 0;
                while (rs.next()) {
                    cur = rs.getString("id");
                    n++;
                    if (!handler.handle(new Event(cur, rs.getString("op"),
                            rs.getObject("k"), rs.getObject("ts"), rs.getObject("doc")))) {
                        return;
                    }
                }
                if (n == 0) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        /**
         * False once closed, once a transport error broke the socket, and
         * while a {@link RowStream} still owns the wire: a {@link Pool} must
         * never hand out a connection whose next frame belongs to somebody
         * else's result set.
         */
        public boolean isUsable() { return !closed && !broken && !streaming; }

        /**
         * How many times this connection has re-dialled after a transport
         * failure. Session state (a {@code BEGIN} transaction, {@code SET}
         * variables other than the session database) does not survive a
         * re-dial, so a caller holding such state compares this before and
         * after its statements.
         */
        public long reconnects() { return reconnects; }

        /**
         * Bound every read on this connection to {@code millis} (0 = wait
         * forever, the default). A read that times out throws and marks the
         * connection broken — the server may still be running the statement
         * and its answer would arrive out of turn — so the next statement
         * re-dials. Applies to later re-dials too.
         */
        public synchronized void setReadTimeout(int millis) {
            if (millis < 0) throw new SkaidbException("read timeout must be >= 0");
            readTimeoutMs = millis;
            try {
                if (socket != null) socket.setSoTimeout(millis);
            } catch (IOException e) {
                broken = true;
            }
        }

        /** The read timeout in milliseconds; 0 = none. */
        public int getReadTimeout() { return readTimeoutMs; }

        @Override public void close() {
            if (closed) return;
            closed = true;
            try { socket.close(); } catch (IOException ignored) {}
        }

        // -- framing --
        private void writeFrame(byte[] payload) throws IOException {
            out.write(new byte[]{
                (byte) (payload.length >>> 24), (byte) (payload.length >>> 16),
                (byte) (payload.length >>> 8), (byte) payload.length});
            out.write(payload);
            out.flush();
        }

        private byte[] readFrame() throws IOException {
            int len = in.readInt(); // big-endian
            byte[] buf = new byte[len];
            in.readFully(buf);
            return buf;
        }

        // -- handshake --
        private void handshake(String user, String password) throws IOException {
            String clientNonce;
            synchronized (Connection.class) { clientNonce = "jv" + (++nonceCounter) + "." + System.nanoTime(); }

            Buf start = new Buf();
            start.u8(10).str(user).str(clientNonce);
            writeFrame(start.toBytes());

            Reader r = new Reader(readFrame());
            if (r.u8() != 11) throw new SkaidbException("bad handshake challenge");
            byte[] salt = r.blob();
            int iterations = r.u32();
            String serverNonce = r.text();

            byte[] authMessage = scramAuthMessage(user, clientNonce, serverNonce, salt, iterations);
            byte[] salted = scramSaltedPassword(password, salt, iterations);
            byte[][] proofAndSig = scramProof(salted, authMessage);

            Buf finish = new Buf();
            finish.u8(12).raw(proofAndSig[0]);
            writeFrame(finish.toBytes());

            Reader r2 = new Reader(readFrame());
            if (r2.u8() != 13) throw new SkaidbException("bad handshake outcome");
            if (r2.u8() == 1) {
                byte[] serverSig = r2.take(32);
                if (!password.isEmpty() && !MessageDigest.isEqual(serverSig, proofAndSig[1]))
                    throw new SkaidbException("server signature mismatch (mutual auth failed)");
            } else {
                throw new SkaidbException("authentication denied: " + r2.text());
            }
        }

        /**
         * EXTERNAL (PROTOCOL.md §2.4): the TLS client certificate is the
         * credential and its Common Name the user; {@code user} is empty or
         * must equal it. No exchange follows AuthStart, and the outcome's
         * 32 zero bytes are NOT verified — TLS already authenticated the server.
         */
        private void handshakeCertificate(String user) throws IOException {
            writeFrame(externalAuthStart(user));
            Reader r = new Reader(readFrame());
            if (r.u8() != 13) throw new SkaidbException("bad handshake outcome");
            if (r.u8() != 1) throw new SkaidbException("authentication denied: " + r.text());
        }

        /**
         * Refuse a statement while a {@link RowStream} still owns the wire.
         *
         * <p>The protocol allows one exchange at a time, so a statement sent
         * mid-stream would read that stream's chunks as its own answer and
         * desync both. The obvious guard — holding the connection's monitor
         * for the stream's lifetime — is not expressible in Java: {@code
         * synchronized} cannot outlive the method that hands the stream back,
         * and a stream is often closed on a different thread than opened it.
         * So a flag marks the wire busy and the loser gets a clear error
         * instead of corruption. It is also deliberately NOT enforced by
         * blocking: the caller of stream() may sit on the stream for minutes,
         * and a silent multi-minute stall reads as a hung database.
         */
        private void requireIdle() {
            if (streaming) throw new SkaidbException(
                "connection is busy streaming: finish or close() the RowStream from "
                + "Connection.stream() before running another statement on this connection");
        }

        // -- query: returns the raw response Reader positioned after the tag --
        synchronized Object run(String sql, boolean wantRows) {
            return run(sql, wantRows, consistency);
        }

        /** {@link #run(String, boolean)} at an explicit consistency level. */
        synchronized Object run(String sql, boolean wantRows, int level) {
            ensureLive();
            byte[] body = sql.getBytes(StandardCharsets.UTF_8);
            Buf req = new Buf();
            req.u8(1).u8(level).u32(body.length).raw(body);
            return roundtrip(req.toBytes());
        }

        /**
         * Stream a result set: rows arrive a chunk at a time instead of the
         * whole set being materialised. For exports and large scans.
         *
         * <pre>try (Skaidb.RowStream s = conn.stream("SELECT ...")) {
         *     while (s.next()) System.out.println(s.getObject(0));
         * }</pre>
         *
         * The connection is busy for the whole stream: any other statement on
         * it throws until the stream ends or is closed, and {@link #isUsable}
         * reports false so a {@link Pool} will not lend it out meanwhile.
         * Always close the stream (try-with-resources above) — Java collects
         * the object but cannot drain the socket for you. Takes no parameters
         * — the opcode carries SQL text.
         */
        public synchronized RowStream stream(String sql) {
            if (closed) throw new SkaidbException("connection is closed");
            requireIdle();
            ensureLive();
            byte[] body = sql.getBytes(StandardCharsets.UTF_8);
            Buf req = new Buf();
            req.u8(5).u8(consistency).u32(body.length).raw(body);
            try {
                writeFrame(req.toBytes());
                Reader r = new Reader(readFrame());
                int tag = r.u8();
                if (tag == 3) {
                    String msg = r.text();
                    throw new SkaidbException(msg.contains("unknown opcode")
                        ? "server does not support streaming: " + msg : msg);
                }
                // Mutation/Ddl: one frame and the exchange is over, so the
                // wire stays free and the stream owns nothing.
                if (tag == 1) return new RowStream(this, new String[0], false, r.u64());
                if (tag == 2) return new RowStream(this, new String[0], false, -1L);
                if (tag != 5) {
                    // The server answered a stream request with something a
                    // stream cannot start with; whatever follows is not ours
                    // to interpret, so do not reuse this socket.
                    broken = true;
                    throw new SkaidbException("unexpected response tag " + tag + " to stream request");
                }
                int ncols = r.u32();
                String[] cols = new String[ncols];
                for (int i = 0; i < ncols; i++) cols[i] = r.text();
                streaming = true;      // released by RowStream: RowsEnd, Error or close()
                return new RowStream(this, cols, true, -1L);
            } catch (IOException e) {
                // Half a request may be on the wire, or half a header off it.
                broken = true;
                throw new SkaidbException("stream failed: " + e.getMessage(), e);
            }
        }

        /** Read one frame; RowStream uses this to pull chunks. */
        Reader nextFrame() { return new Reader(nextFramePayload()); }

        /**
         * Raw frame bytes, so a drain can budget by size without decoding.
         * Not synchronized: the streaming thread blocks here for as long as
         * the server takes, and holding the monitor across that would turn
         * requireIdle()'s clear error into an invisible wait.
         */
        byte[] nextFramePayload() {
            try {
                return readFrame();
            } catch (IOException e) {
                broken = true;   // socket gone; the next statement re-dials
                throw new SkaidbException("stream read failed: " + e.getMessage(), e);
            }
        }

        /**
         * Called by RowStream when it can no longer say where the next frame
         * starts. The connection then fails isUsable() — so a Pool drops it
         * rather than handing the desync to the next borrower — and ensureLive
         * re-dials before the next statement instead of reading leftover
         * stream frames as that statement's answer.
         */
        void markBroken() { broken = true; }

        /** Give the wire back; the connection accepts statements again. */
        void endStream() { streaming = false; }

        /**
         * Prepare `sql` on the SERVER and return {statementId, paramCount}.
         * Cached per connection, because a prepared id is only meaningful on
         * the connection that created it.
         */
        synchronized long[] prepareServer(String sql) {
            requireIdle();
            // Before the cache is consulted: a reconnect empties it, so a
            // stale id from the dead socket can never be handed out.
            ensureLive();
            long[] hit = prepared.get(sql);
            if (hit != null) return hit;
            byte[] body = sql.getBytes(StandardCharsets.UTF_8);
            Buf req = new Buf();
            req.u8(2).u32(body.length).raw(body);
            if (closed) throw new SkaidbException("connection is closed");
            try {
                writeFrame(req.toBytes());
                Reader r = new Reader(readFrame());
                int tag = r.u8();
                if (tag == 4) {                      // Prepared
                    long id = r.u32() & 0xffffffffL;
                    int nparams = r.u16();
                    long[] v = new long[] { id, nparams };
                    if (prepared.size() < 240) prepared.put(sql, v);
                    return v;
                }
                if (tag == 3) throw new Unpreparable(r.text());
                throw new SkaidbException("unexpected prepare response tag " + tag);
            } catch (IOException e) {
                throw new SkaidbException("prepare failed: " + e.getMessage(), e);
            }
        }

        /** Execute a prepared statement with TYPED parameters. */
        synchronized Object execPrepared(long stmtId, Object[] params) {
            return execPrepared(stmtId, params, consistency);
        }

        /** {@link #execPrepared(long, Object[])} at an explicit level. */
        synchronized Object execPrepared(long stmtId, Object[] params, int level) {
            Buf req = new Buf();
            req.u8(3).u8(level).u32((int) stmtId).u16(params.length);
            for (Object p : params) {
                byte[] v = encodeValue(p);
                req.u32(v.length).raw(v);
            }
            return roundtrip(req.toBytes());
        }

        /** Execute a prepared statement once per row, in ONE round-trip. */
        synchronized Object execBatch(long stmtId, java.util.List<Object[]> rows) {
            return execBatch(stmtId, rows, consistency);
        }

        /** {@link #execBatch(long, java.util.List)} at an explicit level. */
        synchronized Object execBatch(long stmtId, java.util.List<Object[]> rows, int level) {
            Buf req = new Buf();
            req.u8(7).u8(level).u32((int) stmtId).u32(rows.size());
            for (Object[] params : rows) {
                req.u16(params.length);
                for (Object p : params) {
                    byte[] v = encodeValue(p);
                    req.u32(v.length).raw(v);
                }
            }
            return roundtrip(req.toBytes());
        }

        private Object roundtrip(byte[] request) {
            if (closed) throw new SkaidbException("connection is closed");
            // Every non-streaming statement funnels through here, so this is
            // the one place the busy-wire check has to hold.
            requireIdle();
            try {
                writeFrame(request);

                Reader r = new Reader(readFrame());
                int tag = r.u8();
                if (tag == 0) {            // Rows
                    int ncols = r.u32();
                    String[] cols = new String[ncols];
                    for (int i = 0; i < ncols; i++) cols[i] = r.text();
                    int nrows = r.u32();
                    List<Object[]> data = new ArrayList<>(nrows);
                    for (int i = 0; i < nrows; i++) {
                        int ncells = r.u32();
                        Object[] row = new Object[ncells];
                        for (int j = 0; j < ncells; j++) row[j] = decodeValue(new Reader(r.blob()));
                        data.add(row);
                    }
                    return new ResultSet(cols, data);
                } else if (tag == 8) {     // ResultSets: a CALL whose body EMITted
                    int nsets = r.u32();
                    List<ResultSet> sets = new ArrayList<>(nsets);
                    for (int s = 0; s < nsets; s++) {
                        int ncols = r.u32();
                        String[] cols = new String[ncols];
                        for (int i = 0; i < ncols; i++) cols[i] = r.text();
                        int nrows = r.u32();
                        List<Object[]> data = new ArrayList<>(nrows);
                        for (int i = 0; i < nrows; i++) {
                            int ncells = r.u32();
                            Object[] row = new Object[ncells];
                            for (int j = 0; j < ncells; j++) row[j] = decodeValue(new Reader(r.blob()));
                            data.add(row);
                        }
                        sets.add(new ResultSet(cols, data));
                    }
                    if (sets.isEmpty()) return new ResultSet(new String[0], new ArrayList<>());
                    ResultSet first = sets.get(0);
                    first.more = new ArrayList<>(sets.subList(1, sets.size()));
                    return first;
                } else if (tag == 1) {     // Mutation
                    return r.u64();
                } else if (tag == 2) {     // Ddl
                    return -1L;
                } else if (tag == 3) {     // Error
                    throw new SkaidbException(r.text());
                }
                throw new SkaidbException("unknown response tag " + tag);
            } catch (IOException e) {
                // The statement may already have executed, so it is NOT
                // retried here — an ambiguous write must never be repeated
                // silently. The connection is marked broken; the next
                // statement re-dials through ensureLive.
                broken = true;
                throw new SkaidbException("query failed: " + e.getMessage(), e);
            }
        }

        /**
         * Re-dial if the transport died since the last statement, BEFORE
         * anything is prepared on it.
         *
         * The prepared-statement cache MUST be cleared: an id is only valid on
         * the connection that created it, so carrying one across a reconnect
         * would execute a different statement (or fail obscurely).
         */
        private synchronized void ensureLive() {
            if (closed) throw new SkaidbException("connection is closed");
            if (!broken) return;
            prepared.clear();
            try {
                if (socket != null) socket.close();
            } catch (IOException ignored) {
                // already gone
            }
            // Cleared BEFORE dialling: dial() issues USE, which runs a
            // statement, which would otherwise re-enter this method forever.
            broken = false;
            reconnects++;
            try {
                dial();      // seed failover + handshake + USE, as at connect
            } catch (RuntimeException e) {
                broken = true;   // still down; the next statement retries
                throw e;
            }
        }
    }

    /**
     * A streamed result set: holds one chunk, not the whole result. Obtained
     * from {@link Connection#stream(String)}, which it owns until the last
     * frame is read or {@link #close} runs — no other statement may use that
     * connection meanwhile. Not thread-safe: one stream, one reader.
     */
    public static final class RowStream implements AutoCloseable {
        /**
         * How much of an abandoned stream is worth pulling off the socket to
         * keep the connection: about 32 of the server's ~256 KB chunks. Past
         * that, re-dialling is cheaper than reading a result set the caller
         * has already walked away from.
         */
        private static final long DRAIN_BYTE_BUDGET = 8L * 1024 * 1024;

        private final Connection conn;
        private final String[] columns;
        private java.util.List<Object[]> chunk = new ArrayList<>();
        private int pos = 0;
        private boolean live;            // more frames are still coming
        /** True while this stream, and not the connection, owns the wire. */
        private boolean owns;
        private Object[] current;
        private final long affected;

        RowStream(Connection conn, String[] columns, boolean live, long affected) {
            this.conn = conn;
            this.columns = columns;
            this.live = live;
            this.affected = affected;
            // A non-row statement answered in one frame owns nothing: the
            // connection was never marked busy for it.
            this.owns = live;
        }

        public String[] getColumnNames() { return columns.clone(); }

        /**
         * The affected-row count when the streamed statement was a mutation
         * (it then has no columns and no rows); -1 for rows and for DDL.
         */
        public long getAffected() { return affected; }

        /** Advance to the next row; false once the stream is exhausted. */
        public boolean next() {
            while (pos >= chunk.size()) {
                if (!live) return false;
                Reader r;
                try {
                    r = conn.nextFrame();
                } catch (RuntimeException e) {
                    // The transport died mid-stream: nothing more is coming
                    // and the wire position is unknowable.
                    release(true);
                    throw e;
                }
                int tag = r.u8();
                if (tag == 6) {                    // RowsChunk
                    int n = r.u32();
                    java.util.List<Object[]> rows = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        int ncells = r.u32();
                        Object[] row = new Object[ncells];
                        for (int j = 0; j < ncells; j++) row[j] = decodeValue(new Reader(r.blob()));
                        rows.add(row);
                    }
                    chunk = rows;
                    pos = 0;
                } else if (tag == 7) {             // RowsEnd
                    // The exchange is over and the wire sits at a request
                    // boundary, so release it now rather than at close():
                    // a caller that reads to the end and never closes still
                    // gets a connection it can go on using.
                    release(false);
                    return false;
                } else if (tag == 3) {             // failed partway; rows so far are valid
                    release(false);                // Error ends the stream too
                    throw new SkaidbException(r.text());
                } else {
                    release(true);
                    throw new SkaidbException("unexpected frame tag " + tag + " in stream");
                }
            }
            current = chunk.get(pos++);
            return true;
        }

        public Object getObject(int i) { return current[i]; }

        public Object getObject(String name) {
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equals(name)) return current[i];
            }
            throw new SkaidbException("no such column: " + name);
        }

        /**
         * Hand the connection back, draining whatever the server is still
         * sending so the socket is left at a request boundary. Abandoning a
         * stream early — breaking out of the loop, or throwing — is the usual
         * way to get here, since this is what try-with-resources calls.
         *
         * <p>If the remainder exceeds {@link #DRAIN_BYTE_BUDGET}, or the
         * drain itself fails, the connection is marked broken instead: it
         * then fails {@link Connection#isUsable}, so a {@link Pool} discards
         * it, and a directly held connection re-dials on its next statement
         * rather than reading this stream's leftovers as that statement's
         * answer. Either way no desynced connection is handed on. Idempotent.
         */
        @Override public void close() {
            if (!owns) return;
            release(!drain());
        }

        /**
         * Read and discard the rest of the stream. False when it could not be
         * finished, i.e. when the caller must not reuse the connection.
         */
        private boolean drain() {
            long budget = DRAIN_BYTE_BUDGET;
            while (live) {
                byte[] frame;
                try {
                    frame = conn.nextFramePayload();
                } catch (RuntimeException e) {
                    // close() must not throw over a stream the caller has
                    // already abandoned; the false answer poisons instead.
                    return false;
                }
                // Only the tag is needed, so the chunk is never decoded.
                int tag = frame.length == 0 ? -1 : frame[0] & 0xff;
                if (tag == 7 || tag == 3) return true;   // RowsEnd / Error: done
                if (tag != 6) return false;              // not a stream frame
                budget -= frame.length;
                if (budget <= 0) return false;
            }
            return true;
        }

        /**
         * Release the wire exactly once, whatever ended the stream. {@code
         * poison} says the position is no longer trustworthy, which must
         * outlive this stream — hence marking the connection, not just
         * dropping it.
         */
        private void release(boolean poison) {
            live = false;
            if (!owns) return;
            owns = false;
            if (poison) conn.markBroken();
            conn.endStream();
        }
    }

    // ---- Query (prepared statement) ---------------------------------------

    public static final class Query implements AutoCloseable {
        private final Connection conn;
        private final String sql;
        private final Object[] params;

        /** -1 = inherit the connection's level (see {@link #setConsistency}). */
        private int level = -1;

        Query(Connection conn, String sql) {
            this.conn = conn;
            this.sql = sql;
            this.params = new Object[countPlaceholders(sql)];
        }

        /**
         * Run THIS statement at `level` ({@code CONSISTENCY_ONE|QUORUM|ALL}),
         * leaving the connection's own level untouched — the per-statement
         * control {@link Connection#setConsistency} cannot give you, since
         * that field is shared by every thread using the connection.
         */
        public Query setConsistency(int level) {
            if (level < 0 || level > 2) throw new SkaidbException("bad consistency " + level);
            this.level = level;
            return this;
        }

        /** The level this statement should run at. */
        private int level() {
            return level < 0 ? conn.consistency : level;
        }

        // JDBC-style 1-based parameter setters (all funnel through setObject).
        public Query setInt(int i, int v)        { return setObject(i, (long) v); }
        public Query setLong(int i, long v)      { return setObject(i, v); }
        public Query setDouble(int i, double v)  { return setObject(i, v); }
        public Query setBoolean(int i, boolean v){ return setObject(i, v); }
        public Query setString(int i, String v)  { return setObject(i, v); }
        public Query setNull(int i)              { return setObject(i, null); }

        public Query setObject(int i, Object v) {
            if (i < 1 || i > params.length)
                throw new SkaidbException("parameter index " + i + " out of range 1.." + params.length);
            params[i - 1] = v;
            return this;
        }

        public ResultSet executeQuery() {
            Object res = exec();
            if (res instanceof ResultSet) return (ResultSet) res;
            return new ResultSet(new String[0], new ArrayList<>()); // mutation/ddl: empty set
        }

        /**
         * Run via a SERVER-side prepared statement, so parameters travel as
         * typed values — the only way to send an array or a document, which
         * have no SQL literal form. Statement kinds the server refuses to
         * prepare (DDL, session statements) fall back to client-side text
         * binding, which is why `bind` still exists.
         */
        private Object exec() {
            if (params.length == 0) return conn.run(sql, true, level());
            try {
                long[] p = conn.prepareServer(sql);
                if (p[1] != params.length)
                    throw new SkaidbException(
                        "statement expects " + p[1] + " parameters, got " + params.length);
                return conn.execPrepared(p[0], params, level());
            } catch (Unpreparable e) {
                return conn.run(bind(sql, params), true, level());
            }
        }

        /**
         * Execute this statement once per row in ONE round-trip. Each row
         * autocommits on its own: if one fails the server names it and
         * earlier rows stay applied, so make the statement idempotent.
         * Returns the total affected row count.
         */
        public long executeBatch(java.util.List<Object[]> rows) {
            if (rows.isEmpty()) return 0L;
            long[] p;
            try {
                p = conn.prepareServer(sql);
            } catch (Unpreparable e) {
                // A statement kind the server will not prepare (DDL, session
                // statements): run the rows one by one with client-side
                // binding, as a single execution would.
                long total = 0;
                for (Object[] r : rows) {
                    if (r.length != params.length)
                        throw new SkaidbException(
                            "batch row expects " + params.length + " parameters, got " + r.length);
                    Object res = conn.run(bind(sql, r), true, level());
                    if (res instanceof Long && (Long) res > 0) total += (Long) res;
                }
                return total;
            }
            for (Object[] r : rows) {
                if (r.length != p[1])
                    throw new SkaidbException(
                        "batch row expects " + p[1] + " parameters, got " + r.length);
            }
            Object res = conn.execBatch(p[0], rows, level());
            return res instanceof Long ? (Long) res : 0L;
        }

        public long executeUpdate() {
            Object res = exec();
            return (res instanceof Long) ? (Long) res : 0L;
        }

        /**
         * Run the statement and report whichever kind of answer it produced:
         * rows, an affected count, or neither (DDL). The general form of
         * {@link #executeQuery} / {@link #executeUpdate} for callers that do
         * not know the statement kind in advance.
         */
        public Result execute() {
            Object res = exec();
            if (res instanceof ResultSet) return new Result((ResultSet) res, -1L);
            return new Result(null, (Long) res);
        }

        /** The number of {@code ?} placeholders (outside string literals). */
        public int getParameterCount() { return params.length; }

        @Override public void close() {}
    }

    // ---- Result -----------------------------------------------------------

    /** What {@link Query#execute()} produced: rows, an affected count, or DDL. */
    public static final class Result {
        private final ResultSet rows;
        private final long affected;

        Result(ResultSet rows, long affected) {
            this.rows = rows;
            this.affected = affected;
        }

        /** True when the statement returned a result set. */
        public boolean hasRows() { return rows != null; }
        /** The result set, or null when the statement returned none. */
        public ResultSet getResultSet() { return rows; }
        /** The affected-row count of a mutation; -1 for rows and for DDL. */
        public long getAffected() { return affected; }
        /** True for a statement that returned neither rows nor a count (DDL, session statements). */
        public boolean isDdl() { return rows == null && affected < 0; }
    }

    // ---- ResultSet --------------------------------------------------------

    public static final class ResultSet {
        private String[] columns;
        private List<Object[]> rows;
        private int pos = -1;
        /** Further result sets of a multi-set reply (a CALL whose body EMITted). */
        List<ResultSet> more = new ArrayList<>();

        ResultSet(String[] columns, List<Object[]> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        /** Advance to the next result set of a multi-set reply; false when there is none. */
        public boolean nextResultSet() {
            if (more.isEmpty()) return false;
            ResultSet n = more.remove(0);
            this.columns = n.columns;
            this.rows = n.rows;
            this.pos = -1;
            return true;
        }

        public boolean next() { return ++pos < rows.size(); }
        public int getRowCount() { return rows.size(); }
        public String[] getColumnNames() { return columns.clone(); }

        public Object getObject(int col) { return rows.get(pos)[col - 1]; } // 1-based
        public Object getObject(String name) { return rows.get(pos)[colIndex(name)]; }

        public String getString(String name)  { Object v = getObject(name); return v == null ? null : String.valueOf(v); }
        public int getInt(String name)         { return ((Number) req(name)).intValue(); }
        public long getLong(String name)       { return ((Number) req(name)).longValue(); }
        public double getDouble(String name)   { return ((Number) req(name)).doubleValue(); }
        public boolean getBoolean(String name) { return (Boolean) req(name); }
        public boolean isNull(String name)     { return getObject(name) == null; }

        public String getString(int col)  { Object v = getObject(col); return v == null ? null : String.valueOf(v); }
        public int getInt(int col)         { return ((Number) getObject(col)).intValue(); }
        public long getLong(int col)       { return ((Number) getObject(col)).longValue(); }
        public double getDouble(int col)   { return ((Number) getObject(col)).doubleValue(); }
        public boolean getBoolean(int col) { return (Boolean) getObject(col); }

        private Object req(String name) {
            Object v = getObject(name);
            if (v == null) throw new SkaidbException("column " + name + " is NULL");
            return v;
        }

        private int colIndex(String name) {
            for (int i = 0; i < columns.length; i++) if (columns[i].equals(name)) return i;
            throw new SkaidbException("no such column: " + name);
        }
    }

    // ---- value decoding (§4) ----------------------------------------------

    static Object decodeValue(Reader r) {
        int tag = r.u8();
        switch (tag) {
            case 0:  return null;
            case 1:  return r.u8() != 0;
            case 2:  return r.i64();
            case 3:  return Double.longBitsToDouble(r.i64());
            case 4: {                                  // Decimal -> BigDecimal
                BigInteger mant = signedLE(r.take(16));
                int scale = r.u32();
                return new BigDecimal(mant, scale);
            }
            case 5:  return r.text();
            case 6:  return r.blob();                  // Bytes -> byte[]
            case 7: {                                  // Uuid
                byte[] b = r.take(16);
                long hi = 0, lo = 0;
                for (int i = 0; i < 8; i++) hi = (hi << 8) | (b[i] & 0xff);
                for (int i = 8; i < 16; i++) lo = (lo << 8) | (b[i] & 0xff);
                return new UUID(hi, lo);
            }
            case 8:  return Instant.ofEpochMilli(r.i64());   // Timestamp
            case 9: {                                  // Array -> List
                int n = r.u32();
                List<Object> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) list.add(decodeValue(r));
                return list;
            }
            case 10: {                                 // Document -> ordered Map
                int n = r.u32();
                Map<String, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) { String k = r.text(); m.put(k, decodeValue(r)); }
                return m;
            }
            default: throw new SkaidbException("unknown value tag " + tag);
        }
    }

    private static BigInteger signedLE(byte[] le) {
        byte[] be = new byte[le.length];
        for (int i = 0; i < le.length; i++) be[i] = le[le.length - 1 - i];
        return new BigInteger(be); // two's-complement big-endian
    }

    // ---- client-side parameter binding (§5) -------------------------------

    static int countPlaceholders(String sql) {
        int n = 0;
        boolean inStr = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inStr) {
                if (c == '\'') {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { i++; } else inStr = false;
                }
            } else if (c == '\'') {
                inStr = true;
            } else if (c == '?') {
                n++;
            }
        }
        return n;
    }

    static String bind(String sql, Object[] params) {
        if (params.length == 0) return sql;
        StringBuilder b = new StringBuilder(sql.length() + 16);
        boolean inStr = false;
        int idx = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inStr) {
                b.append(c);
                if (c == '\'') {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { b.append('\''); i++; }
                    else inStr = false;
                }
                continue;
            }
            if (c == '\'') { inStr = true; b.append(c); continue; }
            if (c == '?') {
                if (idx >= params.length) throw new SkaidbException("more placeholders than parameters");
                b.append(quote(params[idx++]));
                continue;
            }
            b.append(c);
        }
        return b.toString();
    }

    static String quote(Object v) {
        if (v == null) return "NULL";
        if (v instanceof Boolean) return ((Boolean) v) ? "TRUE" : "FALSE";
        if (v instanceof Float || v instanceof Double) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) throw new SkaidbException("cannot bind NaN/Infinity");
            return Double.toString(d);
        }
        if (v instanceof Number) return v.toString();
        if (v instanceof byte[]) return "'" + hex((byte[]) v) + "'";
        if (v instanceof Instant) return Long.toString(((Instant) v).toEpochMilli());
        // Collections and maps are REFUSED rather than stringified. The wire
        // has no client-side literal for an Array or a Document, so the old
        // catch-all turned setObject(i, List.of(1, 2)) into the string
        // '[1, 2]' and stored that — a silent wrong write, the worst kind of
        // failure. Build the value in SQL, or use the REST /insert endpoint,
        // which accepts arbitrary JSON rows.
        if (v instanceof java.util.Collection || v instanceof java.util.Map || v.getClass().isArray()) {
            // Reached only on the client-side fallback path (a statement the
            // server refuses to prepare). Typed binding carries these fine;
            // an interpolated literal cannot represent them, and stringifying
            // would silently store the wrong value.
            String kind = v instanceof java.util.Map ? "a map"
                        : v instanceof java.util.Collection ? "a collection" : "an array";
            throw new SkaidbException(
                "cannot bind " + kind + " into a statement the server will not prepare: "
                    + "skaidb has no literal form for arrays/documents.");
        }
        // strings, UUID, anything else -> quoted string with '' escaping
        return "'" + v.toString().replace("'", "''") + "'";
    }

    // ---- crypto + small helpers -------------------------------------------

    /** The SCRAM auth message (PROTOCOL.md §2.1): the five fields joined by NUL. */
    static byte[] scramAuthMessage(String user, String clientNonce, String serverNonce,
                                   byte[] salt, int iterations) {
        return String.join("\0", user, clientNonce, serverNonce, hex(salt), Integer.toString(iterations))
            .getBytes(StandardCharsets.UTF_8);
    }

    /** PBKDF2-HMAC-SHA-256 of the UTF-8 password, 32 bytes. */
    static byte[] scramSaltedPassword(String password, byte[] salt, int iterations) {
        return pbkdf2(password.getBytes(StandardCharsets.UTF_8), salt, iterations, 32);
    }

    /** {client proof, expected server signature} for a salted password and auth message. */
    static byte[][] scramProof(byte[] salted, byte[] authMessage) {
        byte[] clientKey = hmac(salted, "Client Key".getBytes(StandardCharsets.UTF_8));
        byte[] clientSig = hmac(sha256(clientKey), authMessage);
        byte[] proof = new byte[32];
        for (int i = 0; i < 32; i++) proof[i] = (byte) (clientKey[i] ^ clientSig[i]);
        byte[] serverKey = hmac(salted, "Server Key".getBytes(StandardCharsets.UTF_8));
        return new byte[][] { proof, hmac(serverKey, authMessage) };
    }

    /** AuthStart for mechanism EXTERNAL: the user (may be empty), an empty nonce, mechanism byte 2. */
    static byte[] externalAuthStart(String user) {
        return new Buf().u8(10).str(user == null ? "" : user).str("").u8(2).toBytes();
    }

    /** A KeyManager presenting the PEM certificate chain with its PEM private key. */
    static javax.net.ssl.KeyManager[] clientKeyManagers(String certPath, String keyPath)
            throws IOException, java.security.GeneralSecurityException {
        java.security.cert.Certificate[] chain;
        try (java.io.InputStream fin = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(certPath))) {
            chain = java.security.cert.CertificateFactory.getInstance("X.509")
                .generateCertificates(fin).toArray(new java.security.cert.Certificate[0]);
        }
        if (chain.length == 0) throw new SkaidbException("no certificate found in tls_client_cert " + certPath);
        java.security.PrivateKey key = loadPrivateKey(keyPath);
        java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        char[] pw = "skaidb-client".toCharArray();   // in-memory only; some JDKs refuse an empty one
        ks.setKeyEntry("client", key, pw, chain);
        javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory
            .getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pw);
        return kmf.getKeyManagers();
    }

    /**
     * An unencrypted PEM private key: PKCS#8 ({@code BEGIN PRIVATE KEY}; RSA,
     * EC or Ed25519/Ed448 where the JDK has it) or PKCS#1 RSA ({@code BEGIN
     * RSA PRIVATE KEY}, wrapped into PKCS#8 here).
     */
    static java.security.PrivateKey loadPrivateKey(String path)
            throws IOException, java.security.GeneralSecurityException {
        String pem = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)),
                                StandardCharsets.US_ASCII);
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("-----BEGIN ([A-Z0-9 ]+)-----([^-]*)-----END \\1-----").matcher(pem);
        while (m.find()) {
            String kind = m.group(1);
            if (!kind.endsWith("PRIVATE KEY")) continue;
            byte[] der = java.util.Base64.getMimeDecoder().decode(m.group(2));
            if (kind.equals("PRIVATE KEY")) {
                return pkcs8Key(der);
            } else if (kind.equals("RSA PRIVATE KEY")) {
                return java.security.KeyFactory.getInstance("RSA")
                    .generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(wrapPkcs1(der)));
            } else if (kind.equals("ENCRYPTED PRIVATE KEY")) {
                throw new SkaidbException("tls_client_key " + path + " is encrypted; decrypt it "
                    + "(openssl pkcs8 -topk8 -nocrypt -in key.pem -out key.pk8.pem)");
            } else {
                throw new SkaidbException("tls_client_key " + path + " holds a " + kind + "; convert it "
                    + "to PKCS#8 (openssl pkcs8 -topk8 -nocrypt -in key.pem -out key.pk8.pem)");
            }
        }
        throw new SkaidbException("no PEM private key found in tls_client_key " + path);
    }

    private static java.security.PrivateKey pkcs8Key(byte[] der) throws java.security.GeneralSecurityException {
        java.security.spec.PKCS8EncodedKeySpec spec = new java.security.spec.PKCS8EncodedKeySpec(der);
        java.security.GeneralSecurityException last = null;
        for (String alg : new String[] { "RSA", "EC", "Ed25519", "EdDSA", "Ed448", "RSASSA-PSS" }) {
            try {
                return java.security.KeyFactory.getInstance(alg).generatePrivate(spec);
            } catch (java.security.GeneralSecurityException e) {
                last = e;
            }
        }
        throw new java.security.spec.InvalidKeySpecException("unsupported private key algorithm", last);
    }

    /** PKCS#1 RSAPrivateKey -> PKCS#8 PrivateKeyInfo (version 0, rsaEncryption, NULL params). */
    private static byte[] wrapPkcs1(byte[] pkcs1) {
        byte[] algId = { 0x30, 0x0d, 0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7,
                         0x0d, 0x01, 0x01, 0x01, 0x05, 0x00 };
        byte[] version = { 0x02, 0x01, 0x00 };
        byte[] octet = derTlv(0x04, pkcs1);
        byte[] body = new byte[version.length + algId.length + octet.length];
        System.arraycopy(version, 0, body, 0, version.length);
        System.arraycopy(algId, 0, body, version.length, algId.length);
        System.arraycopy(octet, 0, body, version.length + algId.length, octet.length);
        return derTlv(0x30, body);
    }

    private static byte[] derTlv(int tag, byte[] value) {
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        o.write(tag);
        int n = value.length;
        if (n < 0x80) {
            o.write(n);
        } else {
            int bytes = n > 0xffffff ? 4 : n > 0xffff ? 3 : n > 0xff ? 2 : 1;
            o.write(0x80 | bytes);
            for (int i = bytes - 1; i >= 0; i--) o.write((n >>> (8 * i)) & 0xff);
        }
        o.write(value, 0, n);
        return o.toByteArray();
    }

    private static byte[] hmac(byte[] key, byte[] msg) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            // SecretKeySpec rejects a zero-length key; the empty-password
            // (anonymous) path only needs a well-defined value the server ignores.
            mac.init(new SecretKeySpec(key.length == 0 ? new byte[1] : key, "HmacSHA256"));
            return mac.doFinal(msg);
        } catch (Exception e) {
            throw new SkaidbException("hmac failed: " + e.getMessage(), e);
        }
    }

    private static byte[] sha256(byte[] data) {
        try { return MessageDigest.getInstance("SHA-256").digest(data); }
        catch (Exception e) { throw new SkaidbException("sha256 failed", e); }
    }

    // PBKDF2-HMAC-SHA256 (hand-rolled to avoid empty-password provider quirks).
    private static byte[] pbkdf2(byte[] password, byte[] salt, int iterations, int dkLen) {
        int hLen = 32;
        int blocks = (dkLen + hLen - 1) / hLen;
        byte[] out = new byte[blocks * hLen];
        byte[] key = password.length == 0 ? new byte[0] : password;
        for (int block = 1; block <= blocks; block++) {
            byte[] in = new byte[salt.length + 4];
            System.arraycopy(salt, 0, in, 0, salt.length);
            in[salt.length] = (byte) (block >>> 24);
            in[salt.length + 1] = (byte) (block >>> 16);
            in[salt.length + 2] = (byte) (block >>> 8);
            in[salt.length + 3] = (byte) block;
            byte[] u = hmac(key, in);
            byte[] t = u.clone();
            for (int i = 1; i < iterations; i++) {
                u = hmac(key, u);
                for (int j = 0; j < t.length; j++) t[j] ^= u[j];
            }
            System.arraycopy(t, 0, out, (block - 1) * hLen, hLen);
        }
        byte[] dk = new byte[dkLen];
        System.arraycopy(out, 0, dk, 0, dkLen);
        return dk;
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        return sb.toString();
    }

    // ---- little binary helpers --------------------------------------------

    /**
     * The server refuses to prepare some statement kinds (DDL, session
     * statements). That is not an error — the caller falls back to
     * client-side text binding, exactly as the Python driver does.
     */
    static final class Unpreparable extends RuntimeException {
        private static final long serialVersionUID = 1L;
        Unpreparable(String m) { super(m); }
    }

    /**
     * Encode a Java value as a TYPED skaidb value (tag + payload) — the
     * inverse of decodeValue. This is what prepared binding buys: arrays and
     * nested documents have no SQL literal form, so they can only travel as
     * typed values, never as interpolated text.
     */
    static byte[] encodeValue(Object v) {
        Buf o = new Buf();
        encodeInto(v, o);
        return o.toBytes();
    }

    private static void encodeInto(Object v, Buf o) {
        if (v == null) { o.u8(0); return; }
        if (v instanceof Boolean) { o.u8(1).u8(((Boolean) v) ? 1 : 0); return; }
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            o.u8(2).i64(((Number) v).longValue()); return;
        }
        if (v instanceof Float || v instanceof Double) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d))
                throw new SkaidbException("cannot bind NaN/Infinity");
            o.u8(3).i64(Double.doubleToLongBits(d)); return;
        }
        if (v instanceof java.math.BigDecimal) {
            java.math.BigDecimal bd = (java.math.BigDecimal) v;
            int scale = bd.scale();
            if (scale < 0) { bd = bd.setScale(0); scale = 0; }
            java.math.BigInteger mant = bd.unscaledValue();
            byte[] be = mant.toByteArray();               // big-endian, signed
            if (be.length > 16) throw new SkaidbException("decimal mantissa exceeds 128 bits");
            byte[] le = new byte[16];
            byte fill = (byte) (mant.signum() < 0 ? 0xff : 0x00);
            java.util.Arrays.fill(le, fill);
            for (int i = 0; i < be.length; i++) le[i] = be[be.length - 1 - i];
            o.u8(4).raw(le).u32(scale); return;
        }
        if (v instanceof CharSequence) {
            byte[] b = v.toString().getBytes(StandardCharsets.UTF_8);
            o.u8(5).u32(b.length).raw(b); return;
        }
        if (v instanceof byte[]) {
            byte[] b = (byte[]) v;
            o.u8(6).u32(b.length).raw(b); return;
        }
        if (v instanceof java.util.UUID) {
            java.util.UUID u = (java.util.UUID) v;
            Buf t = new Buf();
            long hi = u.getMostSignificantBits(), lo = u.getLeastSignificantBits();
            for (int i = 7; i >= 0; i--) t.u8((int) ((hi >>> (8 * i)) & 0xff));
            for (int i = 7; i >= 0; i--) t.u8((int) ((lo >>> (8 * i)) & 0xff));
            o.u8(7).raw(t.toBytes()); return;
        }
        if (v instanceof java.time.Instant) {
            o.u8(8).i64(((java.time.Instant) v).toEpochMilli()); return;
        }
        if (v instanceof java.util.Collection) {
            java.util.Collection<?> c = (java.util.Collection<?>) v;
            o.u8(9).u32(c.size());
            for (Object item : c) encodeInto(item, o);
            return;
        }
        if (v.getClass().isArray()) {
            int n = java.lang.reflect.Array.getLength(v);
            o.u8(9).u32(n);
            for (int i = 0; i < n; i++) encodeInto(java.lang.reflect.Array.get(v, i), o);
            return;
        }
        if (v instanceof java.util.Map) {
            java.util.Map<?, ?> m = (java.util.Map<?, ?>) v;
            o.u8(10).u32(m.size());
            for (java.util.Map.Entry<?, ?> e : m.entrySet()) {
                if (!(e.getKey() instanceof CharSequence))
                    throw new SkaidbException("document keys must be strings");
                byte[] k = e.getKey().toString().getBytes(StandardCharsets.UTF_8);
                o.u32(k.length).raw(k);
                encodeInto(e.getValue(), o);
            }
            return;
        }
        throw new SkaidbException("cannot bind value of type " + v.getClass().getName());
    }

    static final class Buf {
        private final java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        Buf u8(int v) { b.write(v & 0xff); return this; }
        Buf u32(int v) { b.write(v & 0xff); b.write((v >>> 8) & 0xff); b.write((v >>> 16) & 0xff); b.write((v >>> 24) & 0xff); return this; }
        Buf u16(int v) { b.write(v & 0xff); b.write((v >>> 8) & 0xff); return this; }
        Buf i64(long v) { for (int i = 0; i < 8; i++) b.write((int) ((v >>> (8 * i)) & 0xff)); return this; }
        Buf raw(byte[] x) { b.write(x, 0, x.length); return this; }
        Buf str(String s) { byte[] x = s.getBytes(StandardCharsets.UTF_8); u32(x.length); return raw(x); }
        byte[] toBytes() { return b.toByteArray(); }
    }

    static final class Reader {
        private final byte[] buf;
        private int pos = 0;
        Reader(byte[] buf) { this.buf = buf; }
        byte[] take(int n) {
            if (pos + n > buf.length) throw new SkaidbException("truncated server message");
            byte[] s = new byte[n];
            System.arraycopy(buf, pos, s, 0, n);
            pos += n;
            return s;
        }
        int u8() { return take(1)[0] & 0xff; }
        int u16() { byte[] b = take(2); return (b[0] & 0xff) | ((b[1] & 0xff) << 8); }
        int u32() {
            byte[] b = take(4);
            return (b[0] & 0xff) | ((b[1] & 0xff) << 8) | ((b[2] & 0xff) << 16) | ((b[3] & 0xff) << 24);
        }
        long i64() {
            byte[] b = take(8);
            long v = 0;
            for (int i = 7; i >= 0; i--) v = (v << 8) | (b[i] & 0xffL);
            return v;
        }
        long u64() { return i64(); } // affected counts fit in a signed long
        byte[] blob() { return take(u32()); }
        String text() { return new String(blob(), StandardCharsets.UTF_8); }
    }
    // ---- Pool --------------------------------------------------------------

    /**
     * A thread-safe pool of connections opened from one DSN.
     *
     * <p>{@code maxsize} bounds the connections kept IDLE, not the number
     * checked out: a burst creates extras and the surplus is closed on return.
     * Pooled connections come from {@link Skaidb#connect(String)}, so they
     * inherit seed failover, TLS and the session database.
     *
     * <p>This matters more in Java than elsewhere: a {@code Connection}
     * serializes every statement through one socket, so threads sharing one
     * connection queue behind each other. A pool gives each worker its own.
     *
     * <pre>{@code
     * try (Skaidb.Pool pool = new Skaidb.Pool("skaidb://u:p@h1:7000,h2:7000/app", 8)) {
     *     long n = pool.withConnection(c -> {
     *         Skaidb.ResultSet rs = c.query("SELECT count(*) AS n FROM t");
     *         rs.next();
     *         return rs.getLong("n");
     *     });
     * }
     * }</pre>
     */
    public static final class Pool implements AutoCloseable {
        /** Work handed a pooled connection. */
        public interface Work<T> { T run(Connection conn); }

        private final String dsn;
        private final int maxsize;
        private final java.util.ArrayDeque<Connection> idle = new java.util.ArrayDeque<>();
        private boolean closed = false;

        public Pool(String dsn) { this(dsn, 10); }

        public Pool(String dsn, int maxsize) {
            if (maxsize < 1) throw new SkaidbException("maxsize must be >= 1");
            this.dsn = dsn;
            this.maxsize = maxsize;
        }

        /** Check out a usable connection, reusing an idle one when possible. */
        public Connection acquire() {
            for (;;) {
                Connection c;
                synchronized (this) {
                    if (closed) throw new SkaidbException("pool is closed");
                    c = idle.pollLast();
                }
                if (c == null) return Skaidb.connect(dsn);
                // A connection the server closed while it sat idle still looks
                // fine locally, so check before handing it out.
                if (c.isUsable()) return c;
                c.close();
            }
        }

        /** Return a connection, closing it if broken or the pool is full. */
        public void release(Connection c) {
            synchronized (this) {
                if (!closed && c.isUsable() && idle.size() < maxsize) {
                    idle.addLast(c);
                    return;
                }
            }
            c.close();
        }

        /** Run {@code work} with a checked-out connection, returning it after. */
        public <T> T withConnection(Work<T> work) {
            Connection c = acquire();
            try {
                return work.run(c);
            } finally {
                release(c);
            }
        }

        /** Close the pool and every idle connection. */
        @Override public void close() {
            java.util.List<Connection> drained;
            synchronized (this) {
                closed = true;
                drained = new java.util.ArrayList<>(idle);
                idle.clear();
            }
            for (Connection c : drained) c.close();
        }
    }
}
