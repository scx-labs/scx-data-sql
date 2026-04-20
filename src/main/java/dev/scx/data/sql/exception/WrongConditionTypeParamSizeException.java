package dev.scx.data.sql.exception;

import dev.scx.data.query.ConditionType;

/// WhereBody 参数长度异常
///
/// @author scx567888
/// @version 0.0.1
public final class WrongConditionTypeParamSizeException extends IllegalArgumentException {

    public WrongConditionTypeParamSizeException(String name, ConditionType conditionType, int paramSize) {
        super("Condition 参数错误 : name : " + name + " , conditionType 类型 : " + conditionType + " , 有效 (不为 null) 的参数数量必须为 " + paramSize + " 个 !!!");
    }

}
