package dev.scx.data.sql.sql_builder;

import dev.scx.data.query.Query;
import dev.scx.data.sql.parser.SQLOrderByParser;
import dev.scx.data.sql.parser.SQLWhereParser;
import dev.scx.sql.SQL;
import dev.scx.sql.dialect.Dialect;
import dev.scx.sql.schema.Table;

import static dev.scx.sql.SQL.sql;

public final class DeleteSQLBuilder {

    private final Table table;
    private final Dialect dialect;
    private final SQLWhereParser whereParser;
    private final SQLOrderByParser orderByParser;

    public DeleteSQLBuilder(Table table, Dialect dialect, SQLWhereParser whereParser, SQLOrderByParser orderByParser) {
        this.table = table;
        this.dialect = dialect;
        this.whereParser = whereParser;
        this.orderByParser = orderByParser;
    }

    public SQL buildDeleteSQL(Query query) {
        var whereClause = whereParser.parse(query.getWhere());
        var orderByClauses = orderByParser.parse(query.getOrderBys());
        var sqlStr = GetDeleteSQL(whereClause.expression(), orderByClauses, query.getLimit());
        return sql(sqlStr, whereClause.params());
    }

    public String GetDeleteSQL(String whereClause, String[] orderByClauses, Long limit) {
        if (whereClause == null || whereClause.isEmpty()) {
            throw new IllegalArgumentException("删除数据时 必须指定 删除条件 或 自定义的 where 语句 !!!");
        }
        var sqlStr = "DELETE FROM " + getTableName() + getWhereClause(whereClause) + getOrderByClause(orderByClauses);
        // 删除时 limit 不能有 offset (偏移量)
        return dialect.applyLimit(sqlStr, null, limit);
    }

    private String getTableName() {
        return dialect.quoteIdentifier(table.name());
    }

    private String getWhereClause(String whereClause) {
        return whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereClause : "";
    }

    private String getOrderByClause(String[] orderByClauses) {
        return orderByClauses != null && orderByClauses.length != 0 ? " ORDER BY " + String.join(", ", orderByClauses) : "";
    }

}
