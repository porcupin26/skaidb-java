package com.skaidb.jdbc;

import com.skaidb.Skaidb;
import java.net.SocketTimeoutException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.util.Locale;

/** Maps the driver's messages onto SQLException subclasses and SQLStates. */
final class Errors {
    private Errors() {}

    static SQLException map(RuntimeException e) {
        String msg = e.getMessage() == null ? e.toString() : e.getMessage();
        String m = msg.toLowerCase(Locale.ROOT);
        Throwable cause = e.getCause();
        if (cause instanceof SocketTimeoutException)
            return new SQLTimeoutException(msg, "HYT00", e);
        if (m.startsWith("authentication denied") || m.startsWith("server signature mismatch"))
            return new SQLInvalidAuthorizationSpecException(msg, "28000", e);
        if (m.startsWith("no reachable endpoint") || m.startsWith("connect failed")
                || m.startsWith("tls setup failed") || m.startsWith("bad handshake"))
            return new SQLNonTransientConnectionException(msg, "08001", e);
        if (m.startsWith("certificate authentication needs") || m.startsWith("tls_client_cert and tls_client_key")
                || m.startsWith("no host given") || m.startsWith("tls_client_key ") || m.startsWith("no pem private key")
                || m.startsWith("no certificate found"))
            return new SQLNonTransientConnectionException(msg, "08001", e);
        if (m.startsWith("connection is closed"))
            return new SQLNonTransientConnectionException(msg, "08003", e);
        if (cause instanceof java.io.IOException)
            // The transport broke mid-statement: the connection re-dials on
            // its next statement, so the application can recover by retrying
            // (the failed statement may or may not have executed).
            return new SQLRecoverableException(msg, "08006", e);
        if (!(e instanceof Skaidb.SkaidbException))
            return new SQLException(msg, "HY000", e);
        return server(msg, m, e);
    }

    /** A statement the server (or the driver's own checks) refused. */
    private static SQLException server(String msg, String m, Throwable e) {
        if (m.startsWith("unique violation")) return new SQLIntegrityConstraintViolationException(msg, "23505", e);
        if (m.startsWith("not null violation")) return new SQLIntegrityConstraintViolationException(msg, "23502", e);
        if (m.startsWith("foreign key violation")) return new SQLIntegrityConstraintViolationException(msg, "23503", e);
        if (m.startsWith("check violation")) return new SQLIntegrityConstraintViolationException(msg, "23514", e);
        if (m.startsWith("exclusion violation")) return new SQLIntegrityConstraintViolationException(msg, "23P01", e);
        if (m.contains("violates check option")) return new SQLIntegrityConstraintViolationException(msg, "44000", e);
        if (m.startsWith("constraint violation")) return new SQLIntegrityConstraintViolationException(msg, "23000", e);
        if (m.startsWith("permission denied")) return new SQLSyntaxErrorException(msg, "42501", e);
        if (m.startsWith("parse error") || m.startsWith("syntax error")) return new SQLSyntaxErrorException(msg, "42601", e);
        if (m.startsWith("no such table") || m.startsWith("unknown table") || m.startsWith("table not found")
                || (m.startsWith("table ") && m.contains("does not exist")))
            return new SQLSyntaxErrorException(msg, "42P01", e);
        if (m.startsWith("unknown column") || m.startsWith("no such column")) return new SQLSyntaxErrorException(msg, "42703", e);
        if (m.startsWith("type error: unknown function")) return new SQLSyntaxErrorException(msg, "42883", e);
        if (m.contains("already exists")) return new SQLSyntaxErrorException(msg, "42P07", e);
        if (m.startsWith("scan budget exceeded")) return new SQLNonTransientException(msg, "54000", e);
        if (m.startsWith("parameter index") || m.startsWith("statement expects") || m.startsWith("batch row expects"))
            return new SQLException(msg, "07001", e);
        if (m.startsWith("cannot bind")) return new SQLDataException(msg, "22023", e);
        if (m.startsWith("connection is busy streaming")) return new SQLException(msg, "HY010", e);
        return new SQLException(msg, "HY000", e);
    }

    static SQLFeatureNotSupportedException unsupported(String what) {
        return new SQLFeatureNotSupportedException(what + " is not supported by the skaidb JDBC driver", "0A000");
    }

    static SQLException closed(String what) {
        return new SQLNonTransientConnectionException(what + " is closed", "08003");
    }

    static SQLDataException conversion(Object v, String target) {
        return new SQLDataException("cannot convert " + (v == null ? "NULL" : v.getClass().getSimpleName() + " value " + v)
            + " to " + target, "22018");
    }
}
