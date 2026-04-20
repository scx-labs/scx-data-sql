package dev.scx.data.sql.sql_builder;

import dev.scx.data.query.Query;
import dev.scx.data.sql.parser.SQLWhereParser;
import dev.scx.sql.SQL;
import dev.scx.sql.dialect.Dialect;
import dev.scx.sql.schema.Table;

import static dev.scx.sql.SQL.sql;

public final class CountSQLBuilder {

    private final SQLWhereParser whereParser;
    private final Table table;
    private final Dialect dialect;

    public CountSQLBuilder(Table table, Dialect dialect, SQLWhereParser whereParser) {
        this.whereParser = whereParser;
        this.table = table;
        this.dialect = dialect;
    }

    public SQL buildCountSQL(Query query) {
        var whereClause = whereParser.parse(query.getWhere());
        var sqlStr = GetCountSQL(whereClause.expression());
        return sql(sqlStr, whereClause.params());
    }

    private String GetCountSQL(String whereClause) {
        return "SELECT COUNT(*) AS count FROM " + getTableName(dialect) + getWhereClause(whereClause);
    }

    private String getTableName(Dialect dialect) {
        return dialect.quoteIdentifier(table.name());
    }

    private String getWhereClause(String whereClause) {
        return whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereClause : "";
    }

}
