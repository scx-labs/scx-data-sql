package dev.scx.data.sql.schema_mapping;

import dev.scx.reflect.ClassInfo;
import dev.scx.reflect.ClassKind;
import dev.scx.reflect.TypeInfo;
import dev.scx.sql.schema.DataTypeKind;

import static dev.scx.sql.schema.DataTypeKind.JSON;
import static dev.scx.sql.schema.DataTypeKind.VARCHAR;

/// AnnotationConfigHelper
final class AnnotationConfigHelper {

    public static DataTypeKind getDataTypeByJavaType(TypeInfo typeInfo) {
        // 处理枚举
        if (typeInfo instanceof ClassInfo classInfo && classInfo.classKind() == ClassKind.ENUM) {
            return VARCHAR;
        }

        var kind = DataTypeKind.ofJavaType(typeInfo.rawClass());

        if (kind != null) {
            return kind;
        }

        return JSON;
    }

}
