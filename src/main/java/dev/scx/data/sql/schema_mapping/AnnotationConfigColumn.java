package dev.scx.data.sql.schema_mapping;

import dev.scx.data.sql.annotation.Column;
import dev.scx.reflect.FieldInfo;

/// AnnotationConfigColumn
///
/// @author scx567888
/// @version 0.0.1
public class AnnotationConfigColumn implements EntityColumn {

    private final FieldInfo javaField;
    private final String columnName;
    private final AnnotationConfigDataType dataType;
    private final String defaultValue;
    private final String onUpdate;
    private final boolean notNull;
    private final boolean autoIncrement;
    private final boolean primary;
    private final boolean unique;
    private final boolean index;

    public AnnotationConfigColumn(FieldInfo javaField) {
        this.javaField = javaField;
        this.javaField.setAccessible(true);

        var column = javaField.findAnnotation(Column.class);
        var defaultColumnName = javaField.name();
        var defaultDataType = new AnnotationConfigDataType(this.javaField.fieldType());

        if (column != null) {
            var _columnName = column.columnName();
            var _defaultValue = column.defaultValue();
            var _onUpdate = column.onUpdate();

            this.columnName = _columnName.length > 0 ? _columnName[0] : defaultColumnName;
            this.dataType = column.dataType().length > 0 ? new AnnotationConfigDataType(column.dataType()[0]) : defaultDataType;
            this.defaultValue = _defaultValue.length > 0 ? _defaultValue[0] : null;
            this.onUpdate = _onUpdate.length > 0 ? _onUpdate[0] : null;
            this.notNull = column.notNull();
            this.autoIncrement = column.autoIncrement();
            this.primary = column.primary();
            this.unique = column.unique();
            this.index = column.index();
        } else {
            this.columnName = defaultColumnName;
            this.dataType = defaultDataType;
            this.defaultValue = null;
            this.onUpdate = null;
            this.notNull = false;
            this.autoIncrement = false;
            this.primary = false;
            this.unique = false;
            this.index = false;
        }
    }

    @Override
    public FieldInfo javaField() {
        return javaField;
    }

    @Override
    public String name() {
        return columnName;
    }

    @Override
    public AnnotationConfigDataType dataType() {
        return dataType;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public String onUpdate() {
        return onUpdate;
    }

    @Override
    public boolean notNull() {
        return notNull;
    }

    @Override
    public boolean autoIncrement() {
        return autoIncrement;
    }

    public boolean primary() {
        return primary;
    }

    public boolean unique() {
        return unique;
    }

    public boolean index() {
        return index;
    }

}
