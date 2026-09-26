package com.skaidb.jdbc;

import com.skaidb.Skaidb;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * {@code jdbc:skaidb://host[:port][,host2[:port]...][/database][?key=value&...]}
 * plus {@link Properties}, which override the URL's parameters.
 */
final class JdbcUrl {
    static final String PREFIX = "jdbc:skaidb:";

    /** Every recognised key, with its description (getPropertyInfo). */
    static final Map<String, String> KEYS = new LinkedHashMap<>();
    static {
        KEYS.put("user", "User name (SCRAM login; with certificate login, optional and must equal the certificate CN)");
        KEYS.put("password", "Password for SCRAM-SHA-256");
        KEYS.put("database", "Session database (same as the URL path)");
        KEYS.put("consistency", "Default consistency: one, quorum (default) or all");
        KEYS.put("tls", "true = encrypt with TLS, verifying the server against the JVM trust store");
        KEYS.put("tls_ca", "PEM file of the CA certificate(s) to trust; implies tls");
        KEYS.put("tls_insecure", "true = TLS without verifying the server (development only)");
        KEYS.put("tls_server_name", "SNI name the server certificate must carry (default skaidb)");
        KEYS.put("tls_client_cert", "PEM client certificate to present; implies tls");
        KEYS.put("tls_client_key", "PEM private key of tls_client_cert (PKCS#8, or PKCS#1 RSA)");
        KEYS.put("auth_mechanism", "scram (default) or certificate");
        KEYS.put("transaction", "Statement that opens a transaction when autoCommit is false: begin (default, standalone server) or atomic (BEGIN ATOMIC, cluster)");
        KEYS.put("read_timeout", "Milliseconds a read may block before the statement fails (0 = none, the default)");
        KEYS.put("fetch_size", "Default Statement fetch size: > 0 streams SELECTs from plain Statements (default 0 = materialise)");
    }

    final Skaidb.ConnectOptions options;
    final Map<String, String> values;
    final boolean atomicTransactions;
    final int readTimeoutMs;
    final int fetchSize;
    final String database;

    private JdbcUrl(Skaidb.ConnectOptions options, Map<String, String> values, boolean atomic,
                    int readTimeoutMs, int fetchSize, String database) {
        this.options = options;
        this.values = values;
        this.atomicTransactions = atomic;
        this.readTimeoutMs = readTimeoutMs;
        this.fetchSize = fetchSize;
        this.database = database;
    }

    static boolean accepts(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    static JdbcUrl parse(String url, Properties info) throws SQLException {
        if (!accepts(url)) throw bad("URL must start with " + PREFIX + "//");
        String rest = url.substring(PREFIX.length());
        if (!rest.startsWith("//")) throw bad("URL must start with " + PREFIX + "//");
        rest = rest.substring(2);
        String query = null;
        int q = rest.indexOf('?');
        if (q >= 0) {
            query = rest.substring(q + 1);
            rest = rest.substring(0, q);
        }
        String path = "";
        int slash = rest.indexOf('/');
        if (slash >= 0) {
            path = decode(rest.substring(slash + 1));
            rest = rest.substring(0, slash);
        }
        if (rest.contains("@"))
            throw bad("put credentials in the user/password parameters or Properties, not before the host");

        Map<String, String> values = new LinkedHashMap<>();
        if (!path.isEmpty()) values.put("database", path);
        if (query != null && !query.isEmpty()) {
            for (String part : query.split("&")) {
                if (part.isEmpty()) continue;
                int eq = part.indexOf('=');
                String k = decode(eq < 0 ? part : part.substring(0, eq)).toLowerCase(Locale.ROOT);
                String v = eq < 0 ? "" : decode(part.substring(eq + 1));
                if (!KEYS.containsKey(k)) throw bad("unknown URL parameter " + k + " (known: " + KEYS.keySet() + ")");
                values.put(k, v);
            }
        }
        if (info != null) {
            for (String k : info.stringPropertyNames()) {
                String key = k.toLowerCase(Locale.ROOT);
                // Unknown Properties are ignored: pools and frameworks pass
                // their own keys through, unlike a URL someone typed.
                if (KEYS.containsKey(key)) values.put(key, info.getProperty(k));
            }
        }

        Skaidb.ConnectOptions o = new Skaidb.ConnectOptions();
        try {
            for (String h : rest.split(",")) {
                if (!h.trim().isEmpty()) o.host(h.trim());
            }
            if (rest.trim().isEmpty() || rest.replace(",", "").trim().isEmpty()) throw bad("URL has no host");
            if (values.containsKey("user")) o.user(values.get("user"));
            if (values.containsKey("password")) o.password(values.get("password"));
            String db = values.getOrDefault("database", "");
            o.database(db);
            if (values.containsKey("consistency")) o.consistency(values.get("consistency"));
            if (values.containsKey("tls")) o.tls(bool("tls", values.get("tls")));
            if (values.containsKey("tls_ca")) o.tlsCa(values.get("tls_ca"));
            if (values.containsKey("tls_insecure")) o.tlsInsecure(bool("tls_insecure", values.get("tls_insecure")));
            if (values.containsKey("tls_server_name")) o.tlsServerName(values.get("tls_server_name"));
            if (values.containsKey("tls_client_cert")) o.tlsClientCert(values.get("tls_client_cert"));
            if (values.containsKey("tls_client_key")) o.tlsClientKey(values.get("tls_client_key"));
            if (values.containsKey("auth_mechanism")) o.authMechanism(values.get("auth_mechanism"));
            String txn = values.getOrDefault("transaction", "begin").toLowerCase(Locale.ROOT);
            if (!txn.equals("begin") && !txn.equals("atomic"))
                throw bad("transaction must be begin or atomic, got " + txn);
            int readTimeout = nonNegative("read_timeout", values.get("read_timeout"));
            int fetchSize = nonNegative("fetch_size", values.get("fetch_size"));
            return new JdbcUrl(o, values, txn.equals("atomic"), readTimeout, fetchSize, db);
        } catch (Skaidb.SkaidbException e) {
            throw bad(e.getMessage());
        }
    }

    private static boolean bool(String key, String v) throws SQLException {
        switch (v.trim().toLowerCase(Locale.ROOT)) {
            case "true": case "1": case "yes": case "on": return true;
            case "false": case "0": case "no": case "off": case "": return false;
            default: throw bad(key + " must be true or false, got " + v);
        }
    }

    private static int nonNegative(String key, String v) throws SQLException {
        if (v == null || v.trim().isEmpty()) return 0;
        try {
            int n = Integer.parseInt(v.trim());
            if (n < 0) throw bad(key + " must be >= 0");
            return n;
        } catch (NumberFormatException e) {
            throw bad(key + " must be a number, got " + v);
        }
    }

    /** Percent-decoding only: a '+' stays a '+', so passwords survive. */
    static String decode(String s) throws SQLException {
        if (s.indexOf('%') < 0) return s;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < b.length; i++) {
            if (b[i] == '%') {
                if (i + 2 >= b.length) throw bad("bad percent-escape in " + s);
                int hi = Character.digit(b[i + 1], 16), lo = Character.digit(b[i + 2], 16);
                if (hi < 0 || lo < 0) throw bad("bad percent-escape in " + s);
                out.write((hi << 4) | lo);
                i += 2;
            } else {
                out.write(b[i]);
            }
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private static SQLException bad(String m) {
        return new SQLNonTransientConnectionException("bad skaidb JDBC URL: " + m, "08001");
    }
}
