package com.skaidb.jdbc;

import com.skaidb.Skaidb;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * What skaidb can say about itself. Catalogs are skaidb databases; there are
 * no schemas. Tables come from {@code SHOW TABLES}, primary keys and columns
 * from {@code DESCRIBE}, indexes from {@code SHOW INDEXES} — all for the
 * connection's CURRENT database (other catalogs answer empty). skaidb tables
 * are schema-less, so {@link #getColumns} lists only the columns the catalog
 * knows (primary-key, indexed and generated columns), typed OTHER. Metadata
 * skaidb does not keep (procedures, privileges, foreign keys, UDTs, ...)
 * answers with an empty result set of the columns JDBC specifies.
 */
final class SkaidbDatabaseMetaData implements DatabaseMetaData {
    private final SkaidbConnection conn;

    SkaidbDatabaseMetaData(SkaidbConnection conn) { this.conn = conn; }

    // ---- helpers ------------------------------------------------------------------

    private static ResultSet rs(String[] cols, List<Object[]> rows) {
        return new SkaidbResultSet(null, cols, rows);
    }

    private static ResultSet empty(String... cols) { return rs(cols, new ArrayList<>()); }

    private Skaidb.ResultSet show(String sql) throws SQLException {
        conn.checkOpen();
        try {
            Skaidb.ResultSet r = conn.nativeConnection().executeRaw(sql).getResultSet();
            if (r == null) throw new SQLException(sql + " returned no rows", "HY000");
            return r;
        } catch (RuntimeException e) {
            throw Errors.map(e);
        }
    }

    /** JDBC LIKE pattern ({@code %}, {@code _}, escape {@code \}); null matches everything. */
    static Pattern like(String pattern) {
        if (pattern == null) return null;
        StringBuilder re = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < pattern.length()) re.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
            else if (c == '%') re.append(".*");
            else if (c == '_') re.append('.');
            else re.append(Pattern.quote(String.valueOf(c)));
        }
        return Pattern.compile(re.toString(), Pattern.DOTALL);
    }

    private static boolean matches(Pattern p, String s) { return p == null || (s != null && p.matcher(s).matches()); }

    /** True when {@code catalog} selects the current database (null = do not narrow; "" = without a catalog). */
    private boolean currentCatalog(String catalog) throws SQLException {
        if (catalog == null) return true;
        if (catalog.isEmpty()) return false;
        return catalog.equals(conn.getCatalog());
    }

    private static String quoteIdent(String s) { return "\"" + s.replace("\"", "\"\"") + "\""; }

    private static String tableType(String kind) {
        switch (kind == null ? "row" : kind) {
            case "view": return "VIEW";
            case "matview": return "MATERIALIZED VIEW";
            case "timeseries": return "TIMESERIES";
            case "rollup": return "ROLLUP";
            default: return "TABLE";
        }
    }

    /** [name, jdbc type] of the current database's tables matching the pattern. */
    private List<String[]> tables(String catalog, String tablePattern, String[] types) throws SQLException {
        List<String[]> out = new ArrayList<>();
        if (!currentCatalog(catalog)) return out;
        Pattern p = like(tablePattern);
        List<String> wanted = types == null ? null : Arrays.asList(types);
        Skaidb.ResultSet r = show("SHOW TABLES");
        while (r.next()) {
            String name = r.getString("table");
            String type = tableType(r.getString("kind"));
            if (matches(p, name) && (wanted == null || wanted.contains(type))) out.add(new String[] { name, type });
        }
        out.sort((a, b) -> a[1].equals(b[1]) ? a[0].compareTo(b[0]) : a[1].compareTo(b[1]));
        return out;
    }

    // ---- catalog queries ----------------------------------------------------------------

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        String[] cols = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                          "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION" };
        List<Object[]> rows = new ArrayList<>();
        if (schemaPattern == null || schemaPattern.isEmpty() || schemaPattern.equals("%")) {
            String cat = conn.getCatalog();
            for (String[] t : tables(catalog, tableNamePattern, types))
                rows.add(new Object[] { cat, null, t[0], t[1], "", null, null, null, null, null });
        }
        return rs(cols, rows);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        for (String t : new String[] { "MATERIALIZED VIEW", "ROLLUP", "TABLE", "TIMESERIES", "VIEW" }) rows.add(new Object[] { t });
        return rs(new String[] { "TABLE_TYPE" }, rows);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        Skaidb.ResultSet r = show("SHOW DATABASES");
        List<String> names = new ArrayList<>();
        while (r.next()) names.add(r.getString("database"));
        names.sort(null);
        for (String n : names) rows.add(new Object[] { n });
        return rs(new String[] { "TABLE_CAT" }, rows);
    }

    @Override
    public ResultSet getSchemas() throws SQLException { return empty("TABLE_SCHEM", "TABLE_CATALOG"); }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException { return getSchemas(); }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        String[] cols = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                          "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                          "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                          "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                          "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN" };
        List<Object[]> rows = new ArrayList<>();
        if (schemaPattern != null && !schemaPattern.isEmpty() && !schemaPattern.equals("%")) return rs(cols, rows);
        String cat = conn.getCatalog();
        Pattern cp = like(columnNamePattern);
        for (String[] t : tables(catalog, tableNamePattern, null)) {
            if (t[1].endsWith("VIEW")) continue;   // DESCRIBE describes tables
            Skaidb.ResultSet r = show("DESCRIBE " + quoteIdent(t[0]));
            long pos = 0;
            while (r.next()) {
                pos++;
                String col = r.getString("column");
                if (!matches(cp, col)) continue;
                String key = r.getString("key") == null ? "" : r.getString("key");
                String idx = r.getString("indexes") == null ? "" : r.getString("indexes");
                boolean pk = key.startsWith("primary key");
                String remarks = (key + (key.isEmpty() || idx.isEmpty() ? "" : "; ") + idx).trim();
                rows.add(new Object[] { cat, null, t[0], col, (long) Types.OTHER, "ANY", null, null, null, null,
                    (long) (pk ? columnNoNulls : columnNullableUnknown), remarks, null, null, null, null, pos,
                    pk ? "NO" : "", null, null, null, null, "NO", key.startsWith("generated") ? "YES" : "NO" });
            }
        }
        return rs(cols, rows);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        String[] cols = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME" };
        List<Object[]> rows = new ArrayList<>();
        if (table == null || (schema != null && !schema.isEmpty()) || !currentCatalog(catalog)) return rs(cols, rows);
        boolean exists = false;
        for (String[] t : tables(null, null, null)) exists |= t[0].equals(table) && !t[1].endsWith("VIEW");
        if (!exists) return rs(cols, rows);
        String cat = conn.getCatalog();
        Skaidb.ResultSet r = show("DESCRIBE " + quoteIdent(table));
        java.util.regex.Pattern seq = java.util.regex.Pattern.compile("primary key(?: \\((\\d+)/\\d+\\))?");
        while (r.next()) {
            String key = r.getString("key");
            java.util.regex.Matcher m = key == null ? null : seq.matcher(key);
            if (m == null || !m.matches()) continue;
            long n = m.group(1) == null ? 1 : Long.parseLong(m.group(1));
            rows.add(new Object[] { cat, null, table, r.getString("column"), n, "PRIMARY" });
        }
        rows.sort((a, b) -> Long.compare((Long) a[4], (Long) b[4]));
        return rs(cols, rows);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        String[] cols = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                          "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES",
                          "FILTER_CONDITION" };
        List<Object[]> rows = new ArrayList<>();
        if (table == null || (schema != null && !schema.isEmpty()) || !currentCatalog(catalog)) return rs(cols, rows);
        String cat = conn.getCatalog();
        Skaidb.ResultSet r = show("SHOW INDEXES");
        while (r.next()) {
            if (!table.equals(r.getString("table"))) continue;
            String def = r.getString("definition") == null ? "" : r.getString("definition");
            boolean isUnique = def.toUpperCase(java.util.Locale.ROOT).startsWith("CREATE UNIQUE");
            if (unique && !isUnique) continue;
            String columns = r.getString("columns") == null ? "" : r.getString("columns");
            long pos = 0;
            for (String c : columns.split(",")) {
                if (c.trim().isEmpty()) continue;
                rows.add(new Object[] { cat, null, table, !isUnique, null, r.getString("index"),
                    (long) tableIndexOther, ++pos, c.trim(), null, null, null, null });
            }
        }
        return rs(cols, rows);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        String[] cols = { "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
                          "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",
                          "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE",
                          "SQL_DATETIME_SUB", "NUM_PREC_RADIX" };
        Object[][] types = {
            { "BOOLEAN", Types.BOOLEAN, 1L, null, null },
            { "BIGINT", Types.BIGINT, 19L, null, null },
            { "DOUBLE", Types.DOUBLE, 17L, null, null },
            { "DECIMAL", Types.DECIMAL, 38L, null, null },
            { "TEXT", Types.VARCHAR, (long) Integer.MAX_VALUE, "'", "'" },
            { "BYTES", Types.VARBINARY, (long) Integer.MAX_VALUE, null, null },
            { "TIMESTAMP", Types.TIMESTAMP, 23L, null, null },
            { "ARRAY", Types.ARRAY, 0L, null, null },
            { "UUID", Types.OTHER, 36L, "'", "'" },
            { "DOCUMENT", Types.OTHER, 0L, null, null },
        };
        List<Object[]> rows = new ArrayList<>();
        for (Object[] t : types) {
            int dt = (Integer) t[1];
            boolean numeric = dt == Types.BIGINT || dt == Types.DOUBLE || dt == Types.DECIMAL;
            rows.add(new Object[] { t[0], (long) dt, t[2], t[3], t[4], null, (long) typeNullable,
                dt == Types.VARCHAR, (long) typeSearchable, !numeric, false, false, t[0],
                0L, dt == Types.DECIMAL ? 38L : 0L, null, null, numeric ? 10L : null });
        }
        return rs(cols, rows);
    }

    @Override
    public ResultSet getProcedures(String c, String s, String p) throws SQLException {
        return empty("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "reserved1", "reserved2", "reserved3",
                     "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getProcedureColumns(String c, String s, String p, String col) throws SQLException {
        return empty("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
                     "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                     "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                     "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getFunctions(String c, String s, String f) throws SQLException {
        return empty("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getFunctionColumns(String c, String s, String f, String col) throws SQLException {
        return empty("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
                     "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
                     "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getColumnPrivileges(String c, String s, String t, String col) throws SQLException {
        return empty("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE");
    }

    @Override
    public ResultSet getTablePrivileges(String c, String s, String t) throws SQLException {
        return empty("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE");
    }

    @Override
    public ResultSet getBestRowIdentifier(String c, String s, String t, int scope, boolean nullable) throws SQLException {
        return empty("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN");
    }

    @Override
    public ResultSet getVersionColumns(String c, String s, String t) throws SQLException {
        return empty("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN");
    }

    private static final String[] KEY_COLS = { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
        "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
        "FK_NAME", "PK_NAME", "DEFERRABILITY" };

    @Override public ResultSet getImportedKeys(String c, String s, String t) throws SQLException { return empty(KEY_COLS); }
    @Override public ResultSet getExportedKeys(String c, String s, String t) throws SQLException { return empty(KEY_COLS); }

    @Override
    public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft) throws SQLException {
        return empty(KEY_COLS);
    }

    @Override
    public ResultSet getUDTs(String c, String s, String t, int[] types) throws SQLException {
        return empty("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE");
    }

    @Override
    public ResultSet getSuperTypes(String c, String s, String t) throws SQLException {
        return empty("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME");
    }

    @Override
    public ResultSet getSuperTables(String c, String s, String t) throws SQLException {
        return empty("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME");
    }

    @Override
    public ResultSet getAttributes(String c, String s, String t, String a) throws SQLException {
        return empty("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE",
                     "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF", "SQL_DATA_TYPE",
                     "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
                     "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return empty("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION");
    }

    @Override
    public ResultSet getPseudoColumns(String c, String s, String t, String col) throws SQLException {
        return empty("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE",
                     "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE");
    }

    // ---- product and driver --------------------------------------------------------------

    @Override public String getDatabaseProductName() { return "skaidb"; }
    /** The binary protocol does not report the server version. */
    @Override public String getDatabaseProductVersion() { return "unknown"; }
    @Override public int getDatabaseMajorVersion() { return 0; }
    @Override public int getDatabaseMinorVersion() { return 0; }
    @Override public String getDriverName() { return "skaidb-java"; }
    @Override public String getDriverVersion() { return Skaidb.VERSION; }
    @Override public int getDriverMajorVersion() { return SkaidbDriver.versionPart(0); }
    @Override public int getDriverMinorVersion() { return SkaidbDriver.versionPart(1); }
    @Override public int getJDBCMajorVersion() { return 4; }
    @Override public int getJDBCMinorVersion() { return 3; }
    @Override public String getURL() { return conn.url(); }

    @Override
    public String getUserName() {
        String u = conn.config().values.get("user");
        return u == null ? "" : u;
    }

    @Override public Connection getConnection() { return conn; }
    @Override public boolean isReadOnly() throws SQLException { return conn.isReadOnly(); }

    // ---- SQL dialect -------------------------------------------------------------------------

    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public String getCatalogSeparator() { return "."; }
    @Override public String getCatalogTerm() { return "database"; }
    @Override public String getSchemaTerm() { return "schema"; }
    @Override public String getProcedureTerm() { return "procedure"; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public int getSQLStateType() { return sqlStateSQL; }
    @Override public boolean supportsMixedCaseIdentifiers() { return true; }
    @Override public boolean storesMixedCaseIdentifiers() { return true; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return false; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean nullsAreSortedHigh() { return false; }
    @Override public boolean nullsAreSortedLow() { return false; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return false; }
    @Override public boolean supportsAlterTableWithAddColumn() { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() { return false; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int from, int to) { return false; }
    @Override public boolean supportsTableCorrelationNames() { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return false; }
    @Override public boolean supportsMultipleResultSets() { return true; }
    @Override public boolean supportsMultipleTransactions() { return true; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return false; }
    @Override public boolean supportsCoreSQLGrammar() { return false; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return false; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return true; }
    @Override public boolean supportsFullOuterJoins() { return true; }
    @Override public boolean supportsLimitedOuterJoins() { return true; }
    @Override public boolean supportsSchemasInDataManipulation() { return false; }
    @Override public boolean supportsSchemasInProcedureCalls() { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return false; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() { return true; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsStoredProcedures() { return true; }
    @Override public boolean supportsSubqueriesInComparisons() { return true; }
    @Override public boolean supportsSubqueriesInExists() { return true; }
    @Override public boolean supportsSubqueriesInIns() { return true; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return false; }
    @Override public boolean supportsCorrelatedSubqueries() { return true; }
    @Override public boolean supportsUnion() { return true; }
    @Override public boolean supportsUnionAll() { return true; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return true; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return true; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return true; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return true; }

    // ---- limits: 0 = no limit or unknown ---------------------------------------------------

    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 0; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 0; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 0; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 0; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 0; }

    // ---- transactions and result sets ------------------------------------------------------

    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_READ_COMMITTED; }
    @Override public boolean supportsTransactions() { return true; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return level == Connection.TRANSACTION_READ_COMMITTED; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return true; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    /** DDL inside a transaction runs at once and is not undone by ROLLBACK. */
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean supportsBatchUpdates() { return true; }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return true; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override public int getResultSetHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName(), "HY000");
    }

    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
