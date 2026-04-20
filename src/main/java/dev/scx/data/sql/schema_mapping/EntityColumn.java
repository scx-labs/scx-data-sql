package dev.scx.data.sql.schema_mapping;

import dev.scx.reflect.FieldInfo;
import dev.scx.sql.schema.Column;

public interface EntityColumn extends Column {

    FieldInfo javaField();

}
