package dev.scx.data.sql.sql_builder;

import dev.scx.data.field_policy.FieldPolicy;
import dev.scx.data.sql.parser.SQLColumnNameParser;
import dev.scx.data.sql.schema_mapping.EntityTable;
import dev.scx.sql.BatchSQL;
import dev.scx.sql.SQL;
import dev.scx.sql.dialect.Dialect;
import dev.scx.sql.schema.Column;

import java.util.ArrayList;
import java.util.Collection;

import static dev.scx.array.ScxArray.concat;
import static dev.scx.data.sql.sql_builder.SQLBuilderHelper.*;
import static dev.scx.sql.BatchSQL.batchSQL;
import static dev.scx.sql.SQL.sql;

public final class InsertSQLBuilder {

    private final EntityTable<?> table;
    private final Dialect dialect;
    private final SQLColumnNameParser columnNameParser;

    public InsertSQLBuilder(EntityTable<?> table, Dialect dialect, SQLColumnNameParser columnNameParser) {
        this.table = table;
        this.dialect = dialect;
        this.columnNameParser = columnNameParser;
    }

    public static String[] createInsertExpressionsColumns(FieldPolicy fieldPolicy, SQLColumnNameParser parser) {
        var assignFields = fieldPolicy.getAssignFields();
        var result = new String[assignFields.length];
        int i = 0;
        for (var fieldExpression : assignFields) {
            result[i] = parser.parseColumnName(fieldExpression.fieldName(), false);
            i = i + 1;
        }
        return result;
    }

    public static String[] createInsertValues(Column[] columns) {
        var result = new String[columns.length];
        for (var i = 0; i < result.length; i = i + 1) {
            result[i] = "?";
        }
        return result;
    }

    public static String[] createInsertExpressionsValue(FieldPolicy fieldPolicy) {
        var assignFields = fieldPolicy.getAssignFields();
        var result = new String[assignFields.length];
        int i = 0;
        for (var fieldExpression : assignFields) {
            result[i] = fieldExpression.expression();
            i = i + 1;
        }
        return result;
    }

    public SQL buildInsertSQL(Object entity, FieldPolicy fieldPolicy) {
        //1, 根据 字段策略过滤 可以插入的列
        var insertColumns = filterByUpdateFieldPolicy(fieldPolicy, table, entity);
        //2, 根据 字段策略 创建插入的表达式列
        var insertExpressionsColumns = createInsertExpressionsColumns(fieldPolicy, columnNameParser);
        //3, 创建 插入值 其实都是 '?'
        var insertValues = createInsertValues(insertColumns);
        //4, 创建 插入表达式
        var insertExpressionsValue = createInsertExpressionsValue(fieldPolicy);
        //5, 拼接最终的 插入列
        var finalInsertColumns = concat(insertColumns, insertExpressionsColumns);
        //6, 拼接最终的 插入值
        var finalInsertValues = concat(insertValues, insertExpressionsValue);
        //7, 创建 SQL 语句字符串
        var sqlStr = GetInsertSQL(finalInsertColumns, finalInsertValues);
        //8, 提取 entity 中的值作为 SQL 参数
        var params = extractValues(insertColumns, entity);
        return sql(sqlStr, params);
    }

    public BatchSQL buildInsertBatchSQL(Collection<?> entityList, FieldPolicy fieldPolicy) {
        //1, 根据 字段策略过滤 可以插入的列
        var insertColumns = filterByUpdateFieldPolicy(fieldPolicy, table);
        //2, 根据 字段策略 创建插入的表达式列
        var insertExpressionsColumns = createInsertExpressionsColumns(fieldPolicy, columnNameParser);
        //3, 创建 插入值 其实都是 '?'
        var insertValues = createInsertValues(insertColumns);
        //4, 创建 插入表达式
        var insertExpressionsValue = createInsertExpressionsValue(fieldPolicy);
        //5, 拼接最终的 插入列
        var finalInsertColumns = concat(insertColumns, insertExpressionsColumns);
        //6, 拼接最终的 插入值
        var finalInsertValues = concat(insertValues, insertExpressionsValue);
        //7, 创建 SQL 语句字符串
        var sqlStr = GetInsertSQL(finalInsertColumns, finalInsertValues);
        //8, 提取 entity 中的值作为 SQL 参数
        var batchParams = new ArrayList<Object[]>(entityList.size());
        for (var entity : entityList) {
            batchParams.add(extractValues(insertColumns, entity));
        }
        return batchSQL(sqlStr, batchParams);
    }

    /// @param insertColumns 存储列名 如 (name, age)
    /// @param insertValues  存储 values 如 ('scx', 1)
    private String GetInsertSQL(Object[] insertColumns, String[] insertValues) {
        return "INSERT INTO " + getTableName() + " (" + getInsertColumns(insertColumns) + ") VALUES (" + String.join(", ", insertValues) + ")";
    }

    private String getTableName() {
        return dialect.quoteIdentifier(table.name());
    }

    private String getInsertColumns(Object[] insertColumns) {
        return joinWithQuoteIdentifier(insertColumns, dialect);
    }

}
