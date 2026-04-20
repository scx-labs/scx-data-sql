package dev.scx.data.sql.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/// 添加此注解的 字段 在创建数据表是会采用 value 上的类型
/// 如果不添加 则会根据 字段的类型进行创建
///
/// @author scx567888
/// @version 0.0.1
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

    /// 用于标注字段对应的数据库列名.
    /// 如果未显式指定列名 (即 `columnName` 为空), 将默认使用字段名作为列名.
    String[] columnName() default {};

    /// 数据库字段类型 仅用于 创建或修复表时
    DataType[] dataType() default {};

    /// 数据库默认值 仅用于 创建或修复表时
    String[] defaultValue() default {};

    /// 数据库更新时值 仅用于 创建或修复表时
    String[] onUpdate() default {};

    /// 是否必填 仅用于 创建或修复表时
    boolean notNull() default false;

    /// 此字段是否为自增 仅用于 创建或修复表时
    boolean autoIncrement() default false;

    /// 此字段是否为主键 仅用于 创建或修复表时
    boolean primary() default false;

    /// 是否唯一 仅用于 创建或修复表时
    boolean unique() default false;

    /// 是否需要添加索引 仅用于 创建或修复表时
    boolean index() default false;

}
