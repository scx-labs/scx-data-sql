package dev.scx.data.sql.schema_mapping;

import dev.scx.collection.multi_map.DefaultMultiMap;
import dev.scx.data.sql.annotation.NoColumn;
import dev.scx.data.sql.annotation.Table;
import dev.scx.reflect.ClassInfo;
import dev.scx.reflect.FieldInfo;
import dev.scx.reflect.ScxReflect;
import dev.scx.reflect.TypeInfo;
import dev.scx.sql.schema.Index;
import dev.scx.sql.schema.Key;
import dev.scx.sql.schema.definition.IndexDefinition;
import dev.scx.sql.schema.definition.KeyDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static dev.scx.reflect.AccessModifier.PUBLIC;
import static dev.scx.reflect.ClassKind.RECORD;

/// AnnotationConfigTable
///
/// @author scx567888
/// @version 0.0.1
public class AnnotationConfigTable<Entity> implements EntityTable<Entity> {

    /// 实体类型不含 @NoColumn 注解的field
    private final AnnotationConfigColumn[] columns;

    /// 表名
    private final String name;

    /// 因为 循环查找速度太慢了 所以这里 使用 map (key:javaFieldName,value:ColumnInfo)
    private final Map<String, AnnotationConfigColumn> columnMap;

    private final Class<Entity> entityClass;
    private final Key[] keys;
    private final Index[] indexes;

    public AnnotationConfigTable(Class<Entity> entityClass) {
        this.entityClass = entityClass;
        this.name = initTableName(entityClass);
        this.columns = initAllColumns(entityClass);
        this.columnMap = initAllColumnMap(columns);
        this.keys = initKeys(this.name, this.columns);
        this.indexes = initIndexes(this.columns);
    }

    /// 获取表名, 必须标注 Table 注解
    public static String initTableName(Class<?> clazz) {
        var table = clazz.getAnnotation(Table.class);
        if (table == null) {
            throw new IllegalArgumentException("entityClass 必须标注 @Table 注解");
        }
        return table.value();
    }

    private static AnnotationConfigColumn[] initAllColumns(Class<?> clazz) {
        TypeInfo typeInfo = ScxReflect.typeOf(clazz);
        if (!(typeInfo instanceof ClassInfo classInfo)) {
            throw new IllegalArgumentException("entityClass 必须是 bean 类型, type: " + clazz);
        }
        FieldInfo[] fields;
        if (classInfo.classKind() == RECORD) {
            fields = classInfo.allFields();
        } else {
            fields = Stream.of(classInfo.allFields())
                .filter(c -> c.accessModifier() == PUBLIC)
                .toArray(FieldInfo[]::new);
        }
        var list = Stream.of(fields)
            .filter(field -> field.findAnnotation(NoColumn.class) == null)
            .map(AnnotationConfigColumn::new)
            .toList();
        checkDuplicateColumnName(list, clazz);
        return list.toArray(AnnotationConfigColumn[]::new);
    }

    private static Key[] initKeys(String tableName, AnnotationConfigColumn[] columns) {
        var primaryColumns = Stream.of(columns)
            .filter(AnnotationConfigColumn::primary)
            .toList();

        if (primaryColumns.isEmpty()) {
            return new Key[0];
        }

        if (primaryColumns.size() > 1) {
            throw new IllegalArgumentException(
                "暂不支持复合主键 !!! table=" + tableName +
                    ", columns=" + primaryColumns.stream().map(AnnotationConfigColumn::name).toList()
            );
        }

        var column = primaryColumns.get(0);
        return new Key[]{
            new KeyDefinition()
                .setName("key_" + column.name())
                .setColumnName(column.name())
                .setPrimary(true)
        };
    }

    private static Index[] initIndexes(AnnotationConfigColumn[] columns) {
        var indexes = new ArrayList<Index>();

        for (var column : columns) {
            if (column.primary()) {
                continue; // 主键通常已隐含 unique + index
            }

            if (column.unique()) {
                indexes.add(new IndexDefinition()
                    .setName("unique_" + column.name())
                    .setColumnName(column.name())
                    .setUnique(true));
            } else if (column.index()) {
                indexes.add(new IndexDefinition()
                    .setName("index_" + column.name())
                    .setColumnName(column.name())
                    .setUnique(false));
            }
        }

        return indexes.toArray(Index[]::new);
    }

    private static Map<String, AnnotationConfigColumn> initAllColumnMap(AnnotationConfigColumn[] infos) {
        var map = new HashMap<String, AnnotationConfigColumn>();
        for (var info : infos) {
            map.put(info.name(), info);
        }
        // javaFieldName 的优先级大于 columnName 所以允许覆盖
        for (var info : infos) {
            map.put(info.javaField().name(), info);
        }
        return map;
    }

    /// 检测 columnName 重复值
    ///
    /// @param list  a
    /// @param clazz a
    private static void checkDuplicateColumnName(List<AnnotationConfigColumn> list, Class<?> clazz) {
        var multiMap = new DefaultMultiMap<String, AnnotationConfigColumn>();
        for (var info : list) {
            multiMap.add(info.name(), info);
        }
        for (var entry : multiMap) {
            var v = entry.values();
            if (v.size() > 1) { // 具有多个相同的 columnName 值
                throw new IllegalArgumentException("重复的 columnName !!! Class -> " + clazz.getName() + ", Field -> " + v.stream().map(c -> c.javaField().name()).toList());
            }
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public Class<Entity> entityClass() {
        return entityClass;
    }

    @Override
    public AnnotationConfigColumn[] columns() {
        return this.columns;
    }

    @Override
    public AnnotationConfigColumn getColumn(String column) {
        return this.columnMap.get(column);
    }

    @Override
    public Key[] keys() {
        return keys;
    }

    @Override
    public Index[] indexes() {
        return indexes;
    }

}
