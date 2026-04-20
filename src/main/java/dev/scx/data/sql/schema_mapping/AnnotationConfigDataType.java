package dev.scx.data.sql.schema_mapping;

import dev.scx.reflect.TypeInfo;
import dev.scx.sql.schema.DataType;
import dev.scx.sql.schema.DataTypeKind;

import static dev.scx.data.sql.schema_mapping.AnnotationConfigHelper.getDataTypeByJavaType;
import static dev.scx.sql.schema.DataTypeKind.VARCHAR;

/// AnnotationConfigDataType
///
/// @author scx567888
/// @version 0.0.1
public class AnnotationConfigDataType implements DataType {

    private static final int DEFAULT_VARCHAR_LENGTH = 128;

    private final DataTypeKind kind;
    private final Integer length;

    public AnnotationConfigDataType(dev.scx.data.sql.annotation.DataType dataType) {
        this.kind = dataType.value();
        var _length = dataType.length() == -1 ? null : dataType.length();
        if (_length == null && this.kind == VARCHAR) {
            _length = DEFAULT_VARCHAR_LENGTH;
        }
        this.length = _length;
    }

    public AnnotationConfigDataType(TypeInfo javaType) {
        this.kind = getDataTypeByJavaType(javaType);
        this.length = this.kind == VARCHAR ? DEFAULT_VARCHAR_LENGTH : null;
    }

    @Override
    public DataTypeKind kind() {
        return this.kind;
    }

    @Override
    public Integer length() {
        return this.length;
    }

}
