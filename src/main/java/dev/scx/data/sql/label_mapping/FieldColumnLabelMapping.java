package dev.scx.data.sql.label_mapping;

import dev.scx.reflect.FieldInfo;
import dev.scx.sql.schema.Table;

import java.util.function.Function;

/// 这个映射表示 字段 -> 列名
///
/// @author scx567888
/// @version 0.0.1
public class FieldColumnLabelMapping implements Function<FieldInfo, String> {

    private final Table table;

    public FieldColumnLabelMapping(Table table) {
        this.table = table;
    }

    @Override
    public String apply(FieldInfo field) {
        var column = this.table.getColumn(field.name());
        return column == null ? null : column.name();
    }

}
