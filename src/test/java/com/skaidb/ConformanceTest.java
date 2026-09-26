package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The shared skaidb wire-protocol conformance suite (conformance/README.md):
 * the vectors in conformance/vectors.json come from the server's reference
 * encoders, and every case runs through the driver's PUBLIC API against
 * {@link ConformanceServer}, which replays the reference bytes and checks the
 * exact bytes the driver sends.
 */
class ConformanceTest {

    static final Map<String, Object> V = ConformanceServer.V;

    /** call.method values this harness has no public API for (listed in the README). */
    static final List<String> SKIPPED_METHODS = List.of();

    // ---- tagged JSON <-> Java ------------------------------------------------

    static Map<String, Object> tag(String k, Object v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k, v);
        return m;
    }

    /** A value the driver surfaced, in the vectors' tagged form (floats by bits only). */
    static Object tagged(Object v) {
        if (v == null) return tag("null", true);
        if (v instanceof Boolean) return tag("bool", v);
        if (v instanceof Long) return tag("int", v.toString());
        if (v instanceof Double) {
            return tag("float_bits", String.format("%016x", Double.doubleToRawLongBits((Double) v)));
        }
        if (v instanceof BigDecimal) {
            BigDecimal d = (BigDecimal) v;
            Map<String, Object> dec = new LinkedHashMap<>();
            dec.put("mantissa", d.unscaledValue().toString());
            dec.put("scale", (long) d.scale());
            return tag("decimal", dec);
        }
        if (v instanceof String) return tag("string", v);
        if (v instanceof byte[]) return tag("bytes", ConformanceServer.hex((byte[]) v));
        if (v instanceof UUID) return tag("uuid", v.toString());
        if (v instanceof Instant) return tag("timestamp_ms", Long.toString(((Instant) v).toEpochMilli()));
        if (v instanceof List) {
            List<Object> out = new ArrayList<>();
            for (Object x : (List<?>) v) out.add(tagged(x));
            return tag("array", out);
        }
        if (v instanceof Map) {
            List<Object> out = new ArrayList<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                Map<String, Object> kv = new LinkedHashMap<>();
                kv.put("key", e.getKey());
                kv.put("value", tagged(e.getValue()));
                out.add(kv);
            }
            return tag("document", out);
        }
        throw new AssertionError("unmapped driver value " + v.getClass() + " " + v);
    }

    /** The expected side, normalised the same way: a float compares by its bits. */
    static Object normalise(Object t) {
        if (t instanceof Map) {
            Map<String, Object> m = Json.obj(t);
            if (m.containsKey("float_bits")) return tag("float_bits", m.get("float_bits"));
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : m.entrySet()) out.put(e.getKey(), normalise(e.getValue()));
            return out;
        }
        if (t instanceof List) {
            List<Object> out = new ArrayList<>();
            for (Object x : (List<?>) t) out.add(normalise(x));
            return out;
        }
        return t;
    }

    /** A tagged value as the Java value the driver binds. */
    static Object nativeValue(Object tv) {
        Map<String, Object> t = Json.obj(tv);
        if (t.containsKey("null")) return null;
        if (t.containsKey("bool")) return t.get("bool");
        if (t.containsKey("int")) return Long.parseLong((String) t.get("int"));
        if (t.containsKey("float_bits")) return Double.longBitsToDouble(Long.parseUnsignedLong((String) t.get("float_bits"), 16));
        if (t.containsKey("decimal")) {
            Map<String, Object> d = Json.obj(t.get("decimal"));
            return new BigDecimal(new BigInteger((String) d.get("mantissa")), ((Long) d.get("scale")).intValue());
        }
        if (t.containsKey("string")) return t.get("string");
        if (t.containsKey("bytes")) return ConformanceServer.unhex((String) t.get("bytes"));
        if (t.containsKey("uuid")) return UUID.fromString((String) t.get("uuid"));
        if (t.containsKey("timestamp_ms")) return Instant.ofEpochMilli(Long.parseLong((String) t.get("timestamp_ms")));
        if (t.containsKey("array")) {
            List<Object> out = new ArrayList<>();
            for (Object x : Json.arr(t.get("array"))) out.add(nativeValue(x));
            return out;
        }
        if (t.containsKey("document")) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Object e : Json.arr(t.get("document"))) out.put((String) Json.obj(e).get("key"), nativeValue(Json.obj(e).get("value")));
            return out;
        }
        throw new AssertionError("unknown tagged value " + t);
    }

    static Map<String, Object> rowsJson(String[] cols, List<Object[]> rows) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("columns", new ArrayList<Object>(List.of((Object[]) cols)));
        List<Object> rs = new ArrayList<>();
        for (Object[] r : rows) {
            List<Object> cells = new ArrayList<>();
            for (Object v : r) cells.add(tagged(v));
            rs.add(cells);
        }
        m.put("rows", rs);
        return m;
    }

    static Skaidb.Connection connect(ConformanceServer server) {
        return Skaidb.connect(new Skaidb.ConnectOptions()
            .host("127.0.0.1", server.port())
            .user(ConformanceServer.username())
            .password(ConformanceServer.password()));
    }

    // ---- running a call through the public API -------------------------------

    static Object runCall(Skaidb.Connection conn, Map<String, Object> call) {
        String method = (String) call.get("method");
        if (method.equals("sequence")) {
            List<Object> out = new ArrayList<>();
            for (Object c : Json.arr(call.get("calls"))) out.add(runCall(conn, Json.obj(c)));
            return tag("sequence", out);
        }
        try {
            switch (method) {
                case "query": {
                    Skaidb.Query q = conn.prepare((String) call.get("sql"))
                        .setConsistency(Skaidb.parseConsistency((String) call.get("consistency")));
                    return resultJson(q.execute());
                }
                case "execute_prepared": {
                    Skaidb.Query q = conn.prepare((String) call.get("sql"))
                        .setConsistency(Skaidb.parseConsistency((String) call.get("consistency")));
                    List<Object> params = Json.arr(call.get("params"));
                    for (int i = 0; i < params.size(); i++) q.setObject(i + 1, nativeValue(params.get(i)));
                    return resultJson(q.execute());
                }
                case "execute_batch": {
                    List<Object[]> rows = new ArrayList<>();
                    for (Object r : Json.arr(call.get("rows"))) {
                        List<Object> cells = Json.arr(r);
                        Object[] row = new Object[cells.size()];
                        for (int i = 0; i < row.length; i++) row[i] = nativeValue(cells.get(i));
                        rows.add(row);
                    }
                    long n = conn.prepare((String) call.get("sql"))
                        .setConsistency(Skaidb.parseConsistency((String) call.get("consistency")))
                        .executeBatch(rows);
                    return tag("affected", Long.toString(n));
                }
                case "query_stream": {
                    try (Skaidb.RowStream s = conn.stream((String) call.get("sql"))) {
                        String[] cols = s.getColumnNames();
                        if (cols.length == 0) {
                            return s.getAffected() >= 0 ? tag("affected", Long.toString(s.getAffected()))
                                                        : tag("ddl", true);
                        }
                        List<Object[]> rows = new ArrayList<>();
                        try {
                            while (s.next()) {
                                Object[] row = new Object[cols.length];
                                for (int i = 0; i < cols.length; i++) row[i] = s.getObject(i);
                                rows.add(row);
                            }
                        } catch (Skaidb.SkaidbException e) {
                            Map<String, Object> rte = new LinkedHashMap<>();
                            rte.put("rows", rowsJson(cols, rows));
                            rte.put("error", e.getMessage());
                            return tag("rows_then_error", rte);
                        }
                        return tag("rows", rowsJson(cols, rows));
                    }
                }
                default:
                    throw new AssertionError("unknown call.method " + method);
            }
        } catch (Skaidb.SkaidbException e) {
            return tag("error", e.getMessage());
        }
    }

    static Object resultJson(Skaidb.Result r) {
        if (!r.hasRows()) return r.isDdl() ? tag("ddl", true) : tag("affected", Long.toString(r.getAffected()));
        Skaidb.ResultSet rs = r.getResultSet();
        List<Object> sets = new ArrayList<>();
        do {
            String[] cols = rs.getColumnNames();
            List<Object[]> rows = new ArrayList<>();
            while (rs.next()) {
                Object[] row = new Object[cols.length];
                for (int i = 0; i < cols.length; i++) row[i] = rs.getObject(i + 1);
                rows.add(row);
            }
            sets.add(rowsJson(cols, rows));
        } while (rs.nextResultSet());
        return sets.size() > 1 ? tag("result_sets", sets) : tag("rows", sets.get(0));
    }

    static boolean matches(Object expectObj, Object gotObj) {
        Map<String, Object> expect = Json.obj(expectObj), got = Json.obj(gotObj);
        if (expect.containsKey("error")) {
            return got.containsKey("error") && ((String) got.get("error")).contains((String) expect.get("error"));
        }
        if (expect.containsKey("sequence")) {
            if (!got.containsKey("sequence")) return false;
            List<Object> e = Json.arr(expect.get("sequence")), g = Json.arr(got.get("sequence"));
            if (e.size() != g.size()) return false;
            for (int i = 0; i < e.size(); i++) if (!matches(e.get(i), g.get(i))) return false;
            return true;
        }
        if (expect.containsKey("rows_then_error")) {
            if (!got.containsKey("rows_then_error")) return false;
            Map<String, Object> e = Json.obj(expect.get("rows_then_error")), g = Json.obj(got.get("rows_then_error"));
            return normalise(e.get("rows")).equals(g.get("rows"))
                && ((String) g.get("error")).contains((String) e.get("error"));
        }
        return normalise(expect).equals(got);
    }

    // ---- the tests -------------------------------------------------------------

    @Test
    void formatVersionIsKnown() {
        assertEquals(1L, V.get("format_version"));
    }

    @Test
    void everyValueDecodesAndEncodes() {
        int n = 0;
        for (Object o : Json.arr(V.get("values"))) {
            Map<String, Object> entry = Json.obj(o);
            String name = (String) entry.get("name");
            byte[] encoded = ConformanceServer.unhex((String) entry.get("encoded"));
            Object decoded = Skaidb.decodeValue(new Skaidb.Reader(encoded));
            assertEquals(normalise(entry.get("value")), tagged(decoded), name + ": decode");
            assertEquals(entry.get("encoded"),
                ConformanceServer.hex(Skaidb.encodeValue(nativeValue(entry.get("value")))), name + ": encode");
            n++;
        }
        System.out.println("conformance: " + n + " value vectors decoded and encoded");
    }

    @Test
    void scramComputations() {
        for (Object o : Json.arr(V.get("scram"))) {
            Map<String, Object> s = Json.obj(o);
            String who = (String) s.get("username");
            byte[] salt = ConformanceServer.unhex((String) s.get("salt"));
            int iterations = ((Long) s.get("iterations")).intValue();
            byte[] am = Skaidb.scramAuthMessage(who, (String) s.get("client_nonce"),
                (String) s.get("server_nonce"), salt, iterations);
            assertEquals(s.get("auth_message"), ConformanceServer.hex(am), who + ": auth_message");
            byte[] salted = Skaidb.scramSaltedPassword((String) s.get("password"), salt, iterations);
            assertEquals(s.get("salted_password"), ConformanceServer.hex(salted), who + ": salted_password");
            byte[][] ps = Skaidb.scramProof(salted, am);
            assertEquals(s.get("client_proof"), ConformanceServer.hex(ps[0]), who + ": client_proof");
            assertEquals(s.get("server_signature"), ConformanceServer.hex(ps[1]), who + ": server_signature");
        }
    }

    @Test
    void authOutcomes() throws Exception {
        for (Object o : Json.arr(Json.obj(V.get("auth")).get("outcomes"))) {
            Map<String, Object> outcome = Json.obj(o);
            String name = (String) outcome.get("name");
            ConformanceServer server = new ConformanceServer(name, List.of());
            if ("connected".equals(outcome.get("expect"))) {
                connect(server).close();
            } else {
                Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> connect(server), name);
                if (outcome.containsKey("reason")) {
                    assertTrue(e.getMessage().contains((String) outcome.get("reason")), name + ": " + e.getMessage());
                }
            }
            assertNull(server.finish(), name);
        }
    }

    @Test
    void cases() throws Exception {
        List<String> failures = new ArrayList<>();
        int ran = 0;
        for (Object o : Json.arr(V.get("cases"))) {
            Map<String, Object> c = Json.obj(o);
            String name = (String) c.get("name");
            Map<String, Object> call = Json.obj(c.get("call"));
            if (SKIPPED_METHODS.contains(call.get("method"))) {
                System.out.println("conformance: SKIPPED " + name + " (no API for " + call.get("method") + ")");
                continue;
            }
            ConformanceServer server = new ConformanceServer("ok", ConformanceServer.exchangesOf(c));
            Object got;
            try (Skaidb.Connection conn = connect(server)) {
                got = runCall(conn, call);
            }
            String err = server.finish();
            if (err != null) failures.add(name + ": fake server: " + err);
            else if (!matches(c.get("expect"), got))
                failures.add(name + ":\n  expected " + c.get("expect") + "\n  got      " + got);
            ran++;
        }
        System.out.println("conformance: " + ran + " cases run; skipped methods: "
            + (SKIPPED_METHODS.isEmpty() ? "none" : SKIPPED_METHODS));
        assertTrue(failures.isEmpty(), String.join("\n", failures));
    }
}
