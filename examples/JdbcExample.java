import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// The same tour through plain JDBC: DriverManager finds the skaidb driver on
// the classpath by itself (META-INF/services/java.sql.Driver).
//
//   ./mvnw -B -ntp package -DskipTests
//   javac -cp target/classes -d out examples/JdbcExample.java
//   java -cp target/classes:out JdbcExample "jdbc:skaidb://localhost:7000/?user=skaidb&password=secret"
public class JdbcExample {
    public static void main(String[] args) throws SQLException {
        String url = args.length > 0 ? args[0] : "jdbc:skaidb://localhost:7000/";

        try (Connection conn = DriverManager.getConnection(url);
             Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE jdbc_people (PRIMARY KEY (id))");

            // executeBatch sends every row in ONE round trip.
            try (PreparedStatement ins = conn.prepareStatement(
                    "INSERT INTO jdbc_people (id, name, age) VALUES (?, ?, ?)")) {
                String[] names = { "Ada", "Linus", "Grace" };
                for (int i = 0; i < names.length; i++) {
                    ins.setLong(1, i + 1);
                    ins.setString(2, names[i]);
                    ins.setInt(3, 30 + 10 * i);
                    ins.addBatch();
                }
                ins.executeBatch();
            }

            try (PreparedStatement q = conn.prepareStatement(
                    "SELECT id, name, age FROM jdbc_people WHERE age > ? ORDER BY id")) {
                q.setInt(1, 35);
                try (ResultSet rs = q.executeQuery()) {
                    while (rs.next()) {
                        System.out.println(rs.getLong("id") + " " + rs.getString("name") + " " + rs.getInt("age"));
                    }
                }
            }

            st.execute("DROP TABLE jdbc_people");
        }
    }
}
