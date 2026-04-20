package dev.scx.data.sql.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/// 添加此注解的 字段 不会映射为数据库字段
///
/// @author scx567888
/// @version 0.0.1
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NoColumn {

}
