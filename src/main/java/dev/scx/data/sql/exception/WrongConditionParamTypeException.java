package dev.scx.data.sql.exception;

import dev.scx.data.query.ConditionType;

/// WhereBody 参数类型异常
///
/// @author scx567888
/// @version 0.0.1
public final class WrongConditionParamTypeException extends IllegalArgumentException {

    public WrongConditionParamTypeException(String name, ConditionType conditionType, String paramType) {
        super("Condition 参数类型错误 : name : " + name + " , conditionType 类型 : " + conditionType + " , 参数类型无法转换为 " + paramType + " !!!");
    }

}
