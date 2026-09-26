package com.skaidb.jdbc;

import com.skaidb.Skaidb;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The skaidb JDBC driver. {@link DriverManager} finds it through {@code
 * META-INF/services/java.sql.Driver}; loading the class registers it too.
 *
 * <p>URL: {@code jdbc:skaidb://host[:port][,host2[:port]...][/database][?key=value&...]}
 * (port 7000 by default). Keys, in the URL or as {@link Properties} (which
 * win): {@code user}, {@code password}, {@code database}, {@code consistency}
 * ({@code one|quorum|all}), {@code tls}, {@code tls_ca}, {@code tls_insecure},
 * {@code tls_server_name}, {@code tls_client_cert}, {@code tls_client_key},
 * {@code auth_mechanism} ({@code scram|certificate}), {@code transaction}
 * ({@code begin|atomic}), {@code read_timeout} (ms) and {@code fetch_size}.
 * URL values are percent-decoded.
 */
public final class SkaidbDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new SkaidbDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public SkaidbDriver() {}

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;   // DriverManager asks every driver
        JdbcUrl u = JdbcUrl.parse(url, info);
        return open(u, url);
    }

    static SkaidbConnection open(JdbcUrl u, String url) throws SQLException {
        Skaidb.Connection native_;
        try {
            native_ = Skaidb.connect(u.options);
        } catch (RuntimeException e) {
            throw Errors.map(e);
        }
        return new SkaidbConnection(native_, u, url);
    }

    @Override
    public boolean acceptsURL(String url) {
        return JdbcUrl.accepts(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Map<String, String> given = acceptsURL(url) ? JdbcUrl.parse(url, info).values : Map.of();
        DriverPropertyInfo[] out = new DriverPropertyInfo[JdbcUrl.KEYS.size()];
        int i = 0;
        for (Map.Entry<String, String> k : JdbcUrl.KEYS.entrySet()) {
            DriverPropertyInfo p = new DriverPropertyInfo(k.getKey(), given.get(k.getKey()));
            p.description = k.getValue();
            p.required = false;
            switch (k.getKey()) {
                case "consistency": p.choices = new String[] { "one", "quorum", "all" }; break;
                case "auth_mechanism": p.choices = new String[] { "scram", "certificate" }; break;
                case "transaction": p.choices = new String[] { "begin", "atomic" }; break;
                case "tls": case "tls_insecure": p.choices = new String[] { "true", "false" }; break;
                default: break;
            }
            out[i++] = p;
        }
        return out;
    }

    @Override
    public int getMajorVersion() { return versionPart(0); }

    @Override
    public int getMinorVersion() { return versionPart(1); }

    static int versionPart(int i) {
        String[] parts = Skaidb.VERSION.split("[.-]");
        try {
            return i < parts.length ? Integer.parseInt(parts[i]) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Not JDBC compliant: skaidb is not SQL-92 entry level (no general transactions on a cluster, schema-less tables). */
    @Override
    public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw Errors.unsupported("java.util.logging");
    }
}
