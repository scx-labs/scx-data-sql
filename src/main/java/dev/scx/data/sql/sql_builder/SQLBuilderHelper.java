package dev.scx.data.sql.sql_builder;

import dev.scx.data.exception.DataAccessException;
import dev.scx.data.field_policy.AssignField;
import dev.scx.data.field_policy.FieldPolicy;
import dev.scx.data.field_policy.VirtualField;
import dev.scx.data.sql.schema_mapping.EntityColumn;
import dev.scx.data.sql.schema_mapping.EntityTable;
import dev.scx.reflect.FieldInfo;
import dev.scx.sql.dialect.Dialect;
import dev.scx.sql.schema.Column;
import dev.scx.sql.schema.Table;

import java.util.ArrayList;
import java.util.Map;

import static java.util.Collections.addAll;

final class SQLBuilderHelper {

    /// 根据 字段策略过滤 可以插入的列
    public static EntityColumn[] filterByUpdateFieldPolicy(FieldPolicy fieldPolicy, EntityTable<?> table, Object entity) {
        //1, 先根据 黑白名单进行过滤
        var columns = filterByFilterMode(fieldPolicy, table);

        //2, 再根据 fieldExpressions 过滤
        var assignFields = fieldPolicy.getAssignFields();
        columns = filterByAssignFields(assignFields, table, columns);

        //3, 根据 是否包含空值进行过滤
        var globalIgnoreNull = fieldPolicy.getIgnoreNull();
        var ignoreNulls = fieldPolicy.getIgnoreNulls();
        return filterByFieldValueIsNull(entity, globalIgnoreNull, ignoreNulls, columns);
    }

    /// 根据 字段策略过滤 可以插入的列 (注意在 fieldExpressions 中存在的 fieldName 也会被移除)
    public static EntityColumn[] filterByUpdateFieldPolicy(FieldPolicy fieldPolicy, EntityTable<?> table) {
        //1, 先根据 黑白名单进行过滤
        var columns = filterByFilterMode(fieldPolicy, table);

        //2, 再根据 fieldExpressions 过滤
        var assignFields = fieldPolicy.getAssignFields();
        return filterByAssignFields(assignFields, table, columns);
    }

    /// 根据 字段策略过滤 可以插入的列 (注意在 fieldExpressions 中存在的 fieldName 也会被移除)
    public static Column[] filterByQueryFieldPolicy(FieldPolicy fieldPolicy, EntityTable<?> table) {
        //1, 先根据 黑白名单进行过滤
        var columns = filterByFilterMode(fieldPolicy, table);

        //2, 再根据 virtualFields 过滤
        var virtualFields = fieldPolicy.getVirtualFields();
        return filterByVirtualFields(virtualFields, table, columns);
    }

    public static Column[] filterByVirtualFields(VirtualField[] virtualFields, Table table, Column... columns) {
        // 快速判断
        if (virtualFields.length == 0) {
            return columns;
        }

        var result = new ArrayList<Column>();
        addAll(result, columns);
        //表达式中存在的就不应该 存在了
        for (var fieldName : virtualFields) {
            result.remove(table.getColumn(fieldName.virtualFieldName()));
        }
        return result.toArray(Column[]::new);
    }

    public static EntityColumn[] filterByFilterMode(FieldPolicy fieldPolicy, EntityTable<?> table) {
        var filterMode = fieldPolicy.getFilterMode();
        return switch (filterMode) {
            case INCLUDED -> filterByIncluded(fieldPolicy.getFieldNames(), table);
            case EXCLUDED -> filterByExcluded(fieldPolicy.getFieldNames(), table);
        };
    }

    /// 根据白名单进行过滤  (注意在 fieldExpressions 中存在的 fieldName 也会被移除)
    public static EntityColumn[] filterByIncluded(String[] fieldNames, EntityTable<?> table) {
        //快速处理
        if (fieldNames.length == 0) {
            return new EntityColumn[0];
        }

        var columns = new ArrayList<EntityColumn>();
        for (var fieldName : fieldNames) {
            var column = table.getColumn(fieldName);
            if (column != null) {
                columns.add(column);
            }
        }
        return columns.toArray(EntityColumn[]::new);
    }

    /// 根据黑名单进行过滤  (注意在 fieldExpressions 中存在的 fieldName 也会被移除)
    public static EntityColumn[] filterByExcluded(String[] fieldNames, EntityTable<?> table) {
        //快速处理
        if (fieldNames.length == 0) {
            return table.columns();
        }

        var allColumns = new ArrayList<EntityColumn>();
        addAll(allColumns, table.columns());

        for (var fieldName : fieldNames) {
            allColumns.remove(table.getColumn(fieldName));
        }
        return allColumns.toArray(EntityColumn[]::new);
    }

    /// 根据是否空值进行过滤
    private static EntityColumn[] filterByFieldValueIsNull(Object entity, boolean globalIgnoreNull, Map<String, Boolean> ignoreNulls, EntityColumn... columns) {
        //快速方法

        if (entity == null && globalIgnoreNull && ignoreNulls.isEmpty()) {
            return new EntityColumn[0];
        }

        if (!globalIgnoreNull && ignoreNulls.isEmpty()) {
            return columns;
        }

        // 正常过滤
        var result = new ArrayList<EntityColumn>();
        for (var column : columns) {
            var fieldName = column.javaField().name();
            //判断是否需要忽略这个字段
            var ignoreNull = ignoreNulls.getOrDefault(fieldName, globalIgnoreNull);
            var value = entity != null ? getJavaFieldValue(column.javaField(), entity) : null;

            // 若字段不忽略 null, 或者字段值本身不为 null, 则保留
            if (!ignoreNull || value != null) {
                result.add(column);
            }
        }
        return result.toArray(EntityColumn[]::new);
    }

    public static EntityColumn[] filterByAssignFields(AssignField[] assignFields, EntityTable<?> table, EntityColumn... columns) {
        // 快速判断
        if (assignFields.length == 0) {
            return columns;
        }

        var result = new ArrayList<EntityColumn>();
        addAll(result, columns);
        //表达式中存在的就不应该 存在了
        for (var fieldName : assignFields) {
            result.remove(table.getColumn(fieldName.fieldName()));
        }
        return result.toArray(EntityColumn[]::new);
    }

    /// 提取值
    public static Object[] extractValues(EntityColumn[] column, Object entity) {
        var result = new Object[column.length];
        if (entity == null) {
            return result;
        }
        for (var i = 0; i < column.length; i = i + 1) {
            result[i] = getJavaFieldValue(column[i].javaField(), entity);
        }
        return result;
    }

    public static String joinWithQuoteIdentifier(Object[] values, Dialect dialect) {
        var isFirst = true;
        var sb = new StringBuilder();
        for (var value : values) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(", ");
            }
            if (value instanceof Column c) {
                sb.append(dialect.quoteIdentifier(c.name()));
            } else {
                sb.append(value.toString());
            }
        }
        return sb.toString();
    }

    public static Object getJavaFieldValue(FieldInfo field, Object entity) {
        try {
            return field.get(entity);
        } catch (IllegalAccessException e) {
            throw new DataAccessException("读取 Entity 字段值时发生异常 !!!", e);
        }
    }

}
