import com.skaidb.Skaidb;

// Transactions: the helper that commits a body's statements together, a
// commit refused for a conflict and run again, the conflict as a typed
// error, and a body that throws.
//
// This runs against a standalone server (0.346 or newer), where a
// transaction opens with BEGIN. On a cluster pass the opening statement:
// conn.transaction(body, 3, "BEGIN ATOMIC").
//
//   javac -d out src/main/java/com/skaidb/Skaidb.java examples/Transactions.java
//   java -cp out Transactions "skaidb://skaidb:secret@localhost:7000"
public class Transactions {
    public static void main(String[] args) {
        String dsn = args.length > 0 ? args[0] : "skaidb://anonymous@localhost:7000";

        // The transaction lives on ONE connection; `other` plays a second
        // writer, as another process would be.
        try (Skaidb.Connection conn = Skaidb.connect(dsn);
             Skaidb.Connection other = Skaidb.connect(dsn)) {
            conn.execute("CREATE TABLE IF NOT EXISTS tx_accounts (PRIMARY KEY (id))");
            conn.execute("INSERT INTO tx_accounts (id, balance) VALUES (1, 100)");
            conn.execute("INSERT INTO tx_accounts (id, balance) VALUES (2, 100)");

            // ---- the helper: BEGIN, the body, COMMIT; the body's value comes
            // back. Every statement goes through `c`, the connection the
            // transaction is open on.
            long left = conn.transaction(c -> {
                transfer(c, 1, 2, 25);
                return balance(c, 1);
            });
            System.out.println("moved 25: account 1 has " + left + ", account 2 has " + balance(conn, 2));

            // ---- a conflict: another writer changes a row this transaction
            // writes, after its BEGIN. The server refuses the COMMIT, nothing
            // was committed, and the helper runs the body again from BEGIN —
            // up to three times unless told otherwise. So a body must be safe
            // to run more than once.
            int[] runs = { 0 };
            conn.transaction(c -> {
                runs[0]++;
                transfer(c, 1, 2, 25);
                if (runs[0] == 1) {
                    // Only on the first run, so that the second one commits.
                    other.execute("UPDATE tx_accounts SET balance = balance + 1000 WHERE id = 1");
                }
                return null;
            });
            System.out.println("conflict, retried: the body ran " + runs[0] + " times; account 1 has "
                + balance(conn, 1) + ", account 2 has " + balance(conn, 2));

            // ---- out of attempts, the conflict is thrown: a
            // TransactionConflictException, which is a SkaidbException.
            // Nothing was committed and the connection is usable as it is.
            try {
                conn.transaction(c -> {
                    transfer(c, 1, 2, 25);
                    other.execute("UPDATE tx_accounts SET balance = balance + 1 WHERE id = 2");
                    return null;
                }, 1);
            } catch (Skaidb.TransactionConflictException e) {
                System.out.println("one attempt only: " + e.getMessage());
            }

            // ---- a body that throws: the helper sends ROLLBACK and rethrows
            // the body's exception as it is. It is not run again.
            try {
                conn.transaction(c -> {
                    transfer(c, 1, 2, 5000);
                    if (balance(c, 1) < 0) throw new IllegalStateException("not enough funds");
                    return null;
                });
            } catch (IllegalStateException e) {
                System.out.println("rolled back: " + e.getMessage());
            }

            System.out.println("in the end: account 1 has " + balance(conn, 1)
                + ", account 2 has " + balance(conn, 2));
            conn.execute("DROP TABLE tx_accounts");
        }

        // With a pool, pool.transaction(body) checks one connection out for
        // the whole transaction and returns it afterwards:
        // try (Skaidb.Pool pool = new Skaidb.Pool(dsn)) {
        //     pool.transaction(c -> { transfer(c, 1, 2, 25); return null; });
        // }
    }

    static void transfer(Skaidb.Connection c, int from, int to, long amount) {
        c.prepare("UPDATE tx_accounts SET balance = balance - ? WHERE id = ?")
            .setLong(1, amount).setInt(2, from).executeUpdate();
        c.prepare("UPDATE tx_accounts SET balance = balance + ? WHERE id = ?")
            .setLong(1, amount).setInt(2, to).executeUpdate();
    }

    static long balance(Skaidb.Connection c, int id) {
        Skaidb.ResultSet rs = c.prepare("SELECT balance FROM tx_accounts WHERE id = ?").setInt(1, id).executeQuery();
        rs.next();
        return rs.getLong("balance");
    }
}
