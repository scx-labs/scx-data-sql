package dev.scx.data.sql.sql_builder;

import dev.scx.data.aggregation.Aggregation;
import dev.scx.data.aggregation.ExpressionGroupBy;
import dev.scx.data.aggregation.FieldGroupBy;
import dev.scx.data.query.Query;
import dev.scx.data.sql.parser.SQLGroupByParser;
import dev.scx.data.sql.parser.SQLOrderByParser;
import dev.scx.data.sql.parser.SQLWhereParser;
import dev.scx.data.sql.schema_mapping.EntityTable;
import dev.scx.sql.SQL;
import dev.scx.sql.dialect.Dialect;

import java.util.ArrayList;

import static dev.scx.array.ScxArray.concat;

public final class AggregateSQLBuilder {

    private final EntityTable<?> table;
    private final Dialect dialect;
    private final SQLWhereParser whereParser;
    private final SQLGroupByParser groupByParser;
    private final SQLOrderByParser orderByParser;

    public AggregateSQLBuilder(EntityTable<?> table, Dialect dialect, SQLWhereParser whereParser, SQLGroupByParser groupByParser, SQLOrderByParser orderByParser) {
        this.table = table;
        this.dialect = dialect;
        this.whereParser = whereParser;
        this.groupByParser = groupByParser;
        this.orderByParser = orderByParser;
    }

    public SQL buildAggregateSQL(Query beforeAggregateQuery, Aggregation aggregation, Query afterAggregateQuery) {
        //1, 创建聚合列
        var selectColumns = createSelectColumn(aggregation);
        //2, 创建 where 子句
        var whereClause = whereParser.parse(beforeAggregateQuery.getWhere());
        //3, 创建 分组 子句
        var groupByColumn = createGroupByColumn(aggregation);
        //4, 创建 Having 子句
        var havingClause = whereParser.parse(afterAggregateQuery.getWhere());
        //5, 创建 orderBy 子句
        var orderByClauses = orderByParser.parse(afterAggregateQuery.getOrderBys());
        //6, 创建 SQL
        var sqlStr = GetAggregateSQL(selectColumns, whereClause.expression(), groupByColumn, havingClause.expression(), orderByClauses, afterAggregateQuery.getOffset(), afterAggregateQuery.getLimit());
        var finalParams = concat(whereClause.params(), havingClause.params());
        return SQL.sql(sqlStr, finalParams);
    }

    public SQL buildAggregateFirstSQL(Query beforeAggregateQuery, Aggregation aggregation, Query afterAggregateQuery) {
        //1, 创建聚合列
        var selectColumns = createSelectColumn(aggregation);
        //2, 创建 where 子句
        var whereClause = whereParser.parse(beforeAggregateQuery.getWhere());
        //3, 创建 分组 子句
        var groupByColumn = createGroupByColumn(aggregation);
        //4, 创建 Having 子句
        var havingClause = whereParser.parse(afterAggregateQuery.getWhere());
        //5, 创建 orderBy 子句
        var orderByClauses = orderByParser.parse(afterAggregateQuery.getOrderBys());
        //6, 创建 SQL
        var sqlStr = GetAggregateSQL(selectColumns, whereClause.expression(), groupByColumn, havingClause.expression(), orderByClauses, null, 1L);
        var finalParams = concat(whereClause.params(), havingClause.params());
        return SQL.sql(sqlStr, finalParams);
    }

    private String GetAggregateSQL(String[] selectColumns, String whereClause, String[] groupByColumns, String havingClause, String[] orderByClauses, Long offset, Long limit) {
        if (selectColumns.length == 0) {
            throw new IllegalArgumentException("Select 子句错误 : 待查询的数据列 不能为空 !!!");
        }
        var sqlStr = "SELECT " + String.join(", ", selectColumns) + " FROM " + getTableName() +
            getWhereClause(whereClause) + getGroupByColumns(groupByColumns) +
            getHavingClause(havingClause) + getOrderByClause(orderByClauses);
        return dialect.applyLimit(sqlStr, offset, limit);
    }

    private String getOrderByClause(String[] orderByClauses) {
        return orderByClauses != null && orderByClauses.length != 0 ? " ORDER BY " + String.join(", ", orderByClauses) : "";
    }

    private String getHavingClause(String havingClause) {
        return havingClause != null && !havingClause.isEmpty() ? " HAVING " + havingClause : "";
    }

    private String getGroupByColumns(String[] groupByColumns) {
        return groupByColumns != null && groupByColumns.length != 0 ? " GROUP BY " + String.join(", ", groupByColumns) : "";
    }

    private String getWhereClause(String whereClause) {
        return whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereClause : "";
    }

    private String getTableName() {
        return dialect.quoteIdentifier(table.name());
    }

    public String[] createSelectColumn(Aggregation aggregation) {
        // 需要 聚合的列
        var aggs = aggregation.getAggs();
        // 需要 分组的列 (我们需要在查询的时候 同时查出来 分组的列, 因为如果不查返回的结果实际上没有意义)
        var groupBys = aggregation.getGroupBys();
        var selectColumn = new ArrayList<String>();
        //先添加聚合列
        for (var agg : aggs) {
            //这里别忘了 使用方言包一下引用
            selectColumn.add(agg.expression() + " AS " + dialect.quoteIdentifier(agg.alias()));
        }
        //添加分组列
        for (var groupBy : groupBys) {
            if (groupBy instanceof FieldGroupBy f) {
                var s = groupByParser.parseGroupBy(f);
                selectColumn.add(s);
            }
            if (groupBy instanceof ExpressionGroupBy f) {
                selectColumn.add(f.expression() + " AS " + dialect.quoteIdentifier(f.alias()));
            }
        }
        return selectColumn.toArray(String[]::new);
    }

    public String[] createGroupByColumn(Aggregation aggregation) {
        var groupBys = aggregation.getGroupBys();
        var groupByColumn = new ArrayList<String>();
        for (var groupBy : groupBys) {
            if (groupBy instanceof FieldGroupBy f) {
                var s = groupByParser.parseGroupBy(f);
                groupByColumn.add(s);
            }
            if (groupBy instanceof ExpressionGroupBy f) {
                groupByColumn.add(dialect.quoteIdentifier(f.alias()));
            }
        }
        return groupByColumn.toArray(String[]::new);
    }

}
