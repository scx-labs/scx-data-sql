package dev.scx.data.sql.annotation;

import dev.scx.sql.schema.DataTypeKind;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/// DataType
///
/// @author scx567888
/// @version 0.0.1
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface DataType {

    DataTypeKind value();

    int length() default -1;

}
