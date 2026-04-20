package dev.scx.data.sql.parser;

import java.util.Collection;

import static dev.scx.array.ScxArray.toWrapper;

final class SQLParserHelper {

    /// 创建重复的 ? (带分隔符)
    public static String placeholders(int count) {
        if (count == 0) {
            return "";
        }
        var element = "?, ";
        var result = element.repeat(count);
        return result.substring(0, result.length() - ", ".length());
    }

    public static Object[] toObjectArray(Object source) {
        if (source instanceof Object[] objectArr) {
            return objectArr;
        }
        if (source == null) {
            return new Object[0];
        }
        if (source instanceof Collection<?> collection) {
            return collection.toArray();
        }
        if (source.getClass().isArray()) {
            return switch (source) {
                case byte[] arr -> toWrapper(arr);
                case short[] arr -> toWrapper(arr);
                case int[] arr -> toWrapper(arr);
                case long[] arr -> toWrapper(arr);
                case float[] arr -> toWrapper(arr);
                case double[] arr -> toWrapper(arr);
                case boolean[] arr -> toWrapper(arr);
                case char[] arr -> toWrapper(arr);
                default -> throw new IllegalStateException("错误值 : " + source);
            };
        }
        throw new IllegalArgumentException("源数据无法转换为数组对象 !!!");
    }

}
