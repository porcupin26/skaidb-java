import com.skaidb.Skaidb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

// The rest of the API in one file: a DSN with a seed list and a session
// database, typed parameters (arrays, documents, UUIDs, timestamps), a
// one-round-trip batch, per-statement consistency, streaming a large result,
// the abandon/drain rule, a pool, a multi-result-set CALL and a change stream.
//
//   javac -d out src/main/java/com/skaidb/Skaidb.java examples/Advanced.java
//   java -cp out Advanced "skaidb://skaidb:secret@h1:7000,h2:7000/app?consistency=quorum"
public class Advanced {
    public static void main(String[] args) {
        String dsn = args.length > 0 ? args[0] : "skaidb://anonymous@localhost:7000";
        System.out.println("skaidb-java " + Skaidb.VERSION);

        try (Skaidb.Connection conn = Skaidb.connect(dsn)) {
            conn.execute("CREATE TABLE IF NOT EXISTS events (PRIMARY KEY (id))");

            // ---- typed parameters: values that have NO SQL literal form travel
            // as typed values through a server-side prepared statement.
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("kind", "signup");
            doc.put("tags", Arrays.asList("web", "trial"));
            conn.prepare("INSERT INTO events (id, at, who, payload, scores) VALUES (?, ?, ?, ?, ?)")
                .setObject(1, UUID.randomUUID())
                .setObject(2, Instant.now())
                .setString(3, "ada")
                .setObject(4, doc)
                .setObject(5, Arrays.asList(1.5, 2.5))
                .executeUpdate();

            // ---- batch: one statement, many rows, ONE round-trip. Each row
            // autocommits on its own, so keep the statement idempotent.
            List<Object[]> rows = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                rows.add(new Object[] { UUID.randomUUID(), Instant.now(), "user" + i, null, null });
            }
            long inserted = conn.prepare("INSERT INTO events (id, at, who, payload, scores) VALUES (?, ?, ?, ?, ?)")
                                .executeBatch(rows);
            System.out.println("batch inserted " + inserted);

            // ---- per-statement consistency: leaves the connection's level alone.
            Skaidb.ResultSet fast = conn.prepare("SELECT count(*) AS n FROM events")
                                        .setConsistency(Skaidb.CONSISTENCY_ONE)
                                        .executeQuery();
            fast.next();
            System.out.println("rows (ONE): " + fast.getLong("n"));

            // ---- streaming: rows arrive a chunk at a time, never the whole set.
            long seen = 0;
            try (Skaidb.RowStream s = conn.stream("SELECT id, who FROM events")) {
                while (s.next()) {
                    seen++;
                    if (seen == 10) break;   // abandoning early is fine: close() drains
                }
            }
            // The stream is closed, so the connection is usable again.
            System.out.println("streamed " + seen + " then stopped; usable=" + conn.isUsable());

            // ---- a CALL whose body EMITs several result sets.
            // Skaidb.ResultSet rs = conn.query("CALL report()");
            // do { while (rs.next()) { ... } } while (rs.nextResultSet());

            conn.execute("DROP TABLE events");
        }

        // ---- pool: one connection per worker thread; maxsize bounds the IDLE set.
        try (Skaidb.Pool pool = new Skaidb.Pool(dsn, 8)) {
            String v = pool.withConnection(c -> {
                Skaidb.ResultSet rs = c.query("SELECT 1 AS one");
                rs.next();
                return rs.getString("one");
            });
            System.out.println("pooled query -> " + v);
        }

        // ---- change streams: CREATE STREAM s ON t; then follow it. Blocks
        // until the handler returns false; keep the last Event.id to resume.
        // try (Skaidb.Connection conn = Skaidb.connect(dsn)) {
        //     conn.subscribe("s", null, ev -> {
        //         System.out.println(ev.id + " " + ev.op + " " + ev.key + " " + ev.doc);
        //         return true;
        //     });
        // }
    }
}
