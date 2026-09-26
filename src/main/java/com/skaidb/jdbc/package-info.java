/**
 * A JDBC 4.3 driver for skaidb over the driver's binary protocol
 * ({@link com.skaidb.Skaidb}). Register nothing by hand: {@link
 * com.skaidb.jdbc.SkaidbDriver} is found by {@link java.sql.DriverManager}
 * through {@code META-INF/services/java.sql.Driver}, and {@link
 * com.skaidb.jdbc.SkaidbDataSource} serves connection pools.
 *
 * <pre>{@code
 * try (Connection c = DriverManager.getConnection(
 *         "jdbc:skaidb://db1:7000,db2:7000/app?user=app&password=s3cret")) {
 *     ...
 * }
 * }</pre>
 */
package com.skaidb.jdbc;
