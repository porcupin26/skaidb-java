package com.skaidb.jdbc;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A {@link DataSource} for connection pools (HikariCP, DBCP, c3p0, ...) and
 * containers. Configure it with a JDBC URL, bean properties, or both (the
 * properties win over the URL's parameters):
 *
 * <pre>{@code
 * SkaidbDataSource ds = new SkaidbDataSource();
 * ds.setUrl("jdbc:skaidb://db1:7000,db2:7000/app");
 * ds.setUser("app");
 * ds.setPassword("s3cret");
 * }</pre>
 *
 * Every {@link #getConnection()} opens a new physical connection; pooling is
 * the pool's job.
 */
public class SkaidbDataSource implements DataSource, Serializable {
    private static final long serialVersionUID = 1L;

    private String url;
    private final Properties props = new Properties();
    private int loginTimeout = 0;
    private transient PrintWriter logWriter;

    public SkaidbDataSource() {}

    /** {@code jdbc:skaidb://host[:port][,host2[:port]...][/database][?key=value&...]}. */
    public void setUrl(String url) { this.url = url; }
    public String getUrl() { return url; }
    /** Alias of {@link #setUrl}, the name some containers use. */
    public void setURL(String url) { this.url = url; }
    public String getURL() { return url; }

    private void put(String key, Object value) {
        if (value == null) props.remove(key);
        else props.setProperty(key, value.toString());
    }

    public void setUser(String user) { put("user", user); }
    public String getUser() { return props.getProperty("user"); }
    /** Alias of {@link #setUser}. */
    public void setUsername(String user) { setUser(user); }
    public void setPassword(String password) { put("password", password); }
    public void setDatabase(String database) { put("database", database); }
    public String getDatabase() { return props.getProperty("database"); }
    /** {@code one}, {@code quorum} or {@code all}. */
    public void setConsistency(String consistency) { put("consistency", consistency); }
    public void setTls(boolean tls) { put("tls", tls); }
    public void setTlsCa(String path) { put("tls_ca", path); }
    public void setTlsInsecure(boolean insecure) { put("tls_insecure", insecure); }
    public void setTlsServerName(String name) { put("tls_server_name", name); }
    public void setTlsClientCert(String path) { put("tls_client_cert", path); }
    public void setTlsClientKey(String path) { put("tls_client_key", path); }
    /** {@code scram} or {@code certificate}. */
    public void setAuthMechanism(String mechanism) { put("auth_mechanism", mechanism); }
    /** {@code begin} or {@code atomic}. */
    public void setTransaction(String mode) { put("transaction", mode); }
    public void setReadTimeout(int millis) { put("read_timeout", millis); }
    public void setFetchSize(int rows) { put("fetch_size", rows); }

    @Override
    public Connection getConnection() throws SQLException {
        if (url == null) throw new SQLException("SkaidbDataSource has no URL (setUrl)", "08001");
        Properties p = new Properties();
        p.putAll(props);
        return SkaidbDriver.open(JdbcUrl.parse(url, p), url);
    }

    @Override
    public Connection getConnection(String user, String password) throws SQLException {
        if (url == null) throw new SQLException("SkaidbDataSource has no URL (setUrl)", "08001");
        Properties p = new Properties();
        p.putAll(props);
        if (user != null) p.setProperty("user", user);
        if (password != null) p.setProperty("password", password);
        return SkaidbDriver.open(JdbcUrl.parse(url, p), url);
    }

    @Override public PrintWriter getLogWriter() { return logWriter; }
    @Override public void setLogWriter(PrintWriter out) { this.logWriter = out; }

    /** Stored for the pool's benefit; each seed's TCP connect is bounded at 10 s by the driver. */
    @Override public void setLoginTimeout(int seconds) { this.loginTimeout = seconds; }
    @Override public int getLoginTimeout() { return loginTimeout; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw Errors.unsupported("java.util.logging");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName(), "HY000");
    }

    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
