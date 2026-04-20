package dev.scx.data.sql.parser;

import dev.scx.data.query.*;
import dev.scx.data.sql.exception.WrongConditionParamTypeException;
import dev.scx.data.sql.exception.WrongConditionTypeParamSizeException;
import dev.scx.sql.SQL;
import dev.scx.sql.dialect.Dialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static dev.scx.data.query.QueryBuilder.and;
import static dev.scx.data.query.QueryBuilder.or;
import static dev.scx.data.sql.parser.SQLParserHelper.placeholders;
import static dev.scx.data.sql.parser.SQLParserHelper.toObjectArray;
import static java.util.Collections.addAll;

/// WhereParser
///
/// @author scx567888
/// @version 0.0.1
public class SQLWhereParser {

    private final SQLColumnNameParser columnNameParser;
    private final Dialect dialect;

    public SQLWhereParser(SQLColumnNameParser columnNameParser, Dialect dialect) {
        this.columnNameParser = columnNameParser;
        this.dialect = dialect;
    }

    public static String getWhereKeyWord(Condition condition) {
        return switch (condition.conditionType()) {
            case EQ -> "=";
            case NE -> "<>";
            case LT -> "<";
            case LTE -> "<=";
            case GT -> ">";
            case GTE -> ">=";
            case LIKE, LIKE_REGEX -> "LIKE";
            case NOT_LIKE, NOT_LIKE_REGEX -> "NOT LIKE";
            case IN -> "IN";
            case NOT_IN -> "NOT IN";
            case BETWEEN -> "BETWEEN";
            case NOT_BETWEEN -> "NOT BETWEEN";
        };
    }

    public WhereClause parse(Where obj) {
        return switch (obj) {
            case WhereClause w -> parseWhereClause(w);
            case Junction j -> parseJunction(j);
            case Not n -> parseNot(n);
            case Condition w -> parseCondition(w);
            case SQL sql -> parseSQL(sql);
            case null -> new WhereClause(null);
            default -> throw new IllegalArgumentException("Unsupported object type: " + obj.getClass());
        };
    }

    private WhereClause parseWhereClause(WhereClause w) {
        // 我们无法确定用户输入的内容 为了安全起见 我们为这种自定义查询 两端拼接 ()
        // 保证在和其他子句拼接的时候不产生歧义
        return new WhereClause("(" + w.expression() + ")", w.params());
    }

    private WhereClause parseJunction(Junction j) {
        var clauses = new ArrayList<String>();
        var whereParams = new ArrayList<>();
        for (var c : j.clauses()) {
            var w = parse(c);
            if (w != null && !w.isEmpty()) {
                clauses.add(w.expression());
                addAll(whereParams, w.params());
            }
        }

        if (clauses.isEmpty()) {
            return new WhereClause(null);
        }

        var keyWord = switch (j) {
            case Or _ -> "OR";
            case And _ -> "AND";
        };

        var clause = String.join(" " + keyWord + " ", clauses);
        //只有 子句数量 大于 1 时, 我们才在两端拼接 括号
        if (clauses.size() > 1) {
            clause = "(" + clause + ")";
        }
        return new WhereClause(clause, whereParams.toArray());
    }

    private WhereClause parseNot(Not n) {

        var w = parse(n.clause());

        if (w != null && !w.isEmpty()) {
            //因为其余解析方法已经保证了在可能出现歧义的子句两端拼接了括号, 所以这里直接添加 NOT 即可
            return new WhereClause("NOT " + w.expression(), w.params());
        } else {
            return new WhereClause(null);
        }

    }

    private WhereClause parseSQL(SQL sql) {
        return new WhereClause("(" + sql.sql() + ")", sql.params());
    }

    private WhereClause parseCondition(Condition body) {
        if (body.isEmpty()) {
            return new WhereClause(null);
        }
        return switch (body.conditionType()) {
            case EQ, NE -> parseEQ(body);
            case LT, LTE, GT, GTE, LIKE_REGEX, NOT_LIKE_REGEX -> parseLT(body);
            case LIKE, NOT_LIKE -> parseLIKE(body);
            case IN, NOT_IN -> parseIN(body);
            case BETWEEN, NOT_BETWEEN -> parseBETWEEN(body);
        };
    }

    private WhereClause parseEQ(Condition w) {

        if (w.value1() == null) {
            var columnName = columnNameParser.parseColumnName(w);

            return switch (w.conditionType()) {
                case EQ -> new WhereClause(columnName + " IS NULL");
                case NE -> new WhereClause(columnName + " IS NOT NULL");
                default -> throw new IllegalArgumentException("Unexpected value: " + w.conditionType());
            };
        }

        // 类似这种 "name = "
        var columnDefinition = columnNameParser.parseColumnName(w) + " " + getWhereKeyWord(w) + " ";

        //表达式值
        if (w.useExpressionValue()) {
            return new WhereClause(columnDefinition + "(" + w.value1() + ")");
        }

        //针对 参数类型是 SQL 的情况进行特殊处理 下同
        if (w.value1() instanceof SQL a) {
            return new WhereClause(columnDefinition + "(" + a.sql() + ")", a.params());
        }

        return new WhereClause(columnDefinition + "?", w.value1());

    }

    private WhereClause parseLT(Condition w) {
        if (w.value1() == null) {
            throw new WrongConditionTypeParamSizeException(w.selector(), w.conditionType(), 1);
        }

        var columnDefinition = columnNameParser.parseColumnName(w) + " " + getWhereKeyWord(w) + " ";

        //表达式值
        if (w.useExpressionValue()) {
            return new WhereClause(columnDefinition + "(" + w.value1() + ")");
        }

        //针对 参数类型是 SQL 的情况进行特殊处理 下同
        if (w.value1() instanceof SQL a) {
            return new WhereClause(columnDefinition + "(" + a.sql() + ")", a.params());
        }

        return new WhereClause(columnDefinition + "?", w.value1());
    }

    private WhereClause parseLIKE(Condition w) {
        if (w.value1() == null) {
            throw new WrongConditionTypeParamSizeException(w.selector(), w.conditionType(), 1);
        }

        // 类似这种 "name = "
        var columnDefinition = columnNameParser.parseColumnName(w) + " " + getWhereKeyWord(w) + " ";

        //表达式值
        if (w.useExpressionValue()) {
            return new WhereClause(columnDefinition + "CONCAT('%',(" + w.value1() + "),'%')");
        }

        if (w.value1() instanceof SQL a) {
            return new WhereClause(columnDefinition + "CONCAT('%',(" + a.sql() + "),'%')", a.params());
        }

        return new WhereClause(columnDefinition + "CONCAT('%',?,'%')", w.value1());
    }

    private WhereClause parseIN(Condition w) {
        if (w.value1() == null) {
            throw new WrongConditionTypeParamSizeException(w.selector(), w.conditionType(), 1);
        }

        var columnName = columnNameParser.parseColumnName(w);

        // 使用表达式值, 不解析为数组或 SQL, 仅拼接表达式
        if (w.useExpressionValue()) {
            return new WhereClause(columnName + " " + getWhereKeyWord(w) + " (" + w.value1() + ")");
        }

        // SQL 子查询
        if (w.value1() instanceof SQL a) {
            return new WhereClause(columnName + " " + getWhereKeyWord(w) + " (" + a.sql() + ")", a.params());
        }

        // 普通数组值
        Object[] v;
        try {
            v = toObjectArray(w.value1());
        } catch (Exception e) {
            throw new WrongConditionParamTypeException(w.selector(), w.conditionType(), "数组");
        }

        // 空数组
        if (v.length == 0) {
            return switch (w.conditionType()) {
                case IN -> new WhereClause(dialect.falseExpression());
                case NOT_IN -> new WhereClause(dialect.trueExpression());
                default -> throw new IllegalArgumentException("Unexpected value: " + w.conditionType());
            };
        }

        // 去重非 null 元素, 检测是否含 null
        var nonNullValues = Arrays.stream(v).filter(Objects::nonNull).distinct().toArray();
        var containsNull = Arrays.stream(v).anyMatch(Objects::isNull);

        // 处理 null 情况（拼接 IS NULL）
        if (containsNull) {

            var nullClause = switch (w.conditionType()) {
                case IN -> new WhereClause(columnName + " IS NULL");
                case NOT_IN -> new WhereClause(columnName + " IS NOT NULL");
                default -> throw new IllegalArgumentException("Unexpected value: " + w.conditionType());
            };

            //只需要返回 null 即可
            if (nonNullValues.length == 0) {
                return nullClause;
            }

            var placeholders = placeholders(nonNullValues.length);

            var inClause = new WhereClause(columnName + " " + getWhereKeyWord(w) + " (" + placeholders + ")", nonNullValues);

            var j = switch (w.conditionType()) {
                case IN -> or(inClause, nullClause);
                case NOT_IN -> and(inClause, nullClause);
                default -> throw new IllegalArgumentException("Unexpected value: " + w.conditionType());
            };

            return parseJunction(j);
        }

        // 正常拼接参数占位符
        var placeholders = placeholders(nonNullValues.length);

        return new WhereClause(columnName + " " + getWhereKeyWord(w) + " (" + placeholders + ")", nonNullValues);

    }

    private WhereClause parseBETWEEN(Condition w) {
        if (w.value1() == null || w.value2() == null) {
            throw new WrongConditionTypeParamSizeException(w.selector(), w.conditionType(), 2);
        }

        var columnName = columnNameParser.parseColumnName(w);
        var columnDefinition = columnName + " " + getWhereKeyWord(w) + " ";

        String v1;
        String v2;
        var whereParams = new ArrayList<>();

        // 表达式值: 直接拼接 SQL, 不加参数
        if (w.useExpressionValue()) {
            v1 = "(" + w.value1() + ")";
            v2 = "(" + w.value2() + ")";
            return new WhereClause(columnDefinition + v1 + " AND " + v2);
        }

        // SQL 对象
        if (w.value1() instanceof SQL a1) {
            v1 = "(" + a1.sql() + ")";
            addAll(whereParams, a1.params());
        } else {
            v1 = "?";
            whereParams.add(w.value1());
        }

        if (w.value2() instanceof SQL a2) {
            v2 = "(" + a2.sql() + ")";
            addAll(whereParams, a2.params());
        } else {
            v2 = "?";
            whereParams.add(w.value2());
        }

        return new WhereClause(columnDefinition + v1 + " AND " + v2, whereParams.toArray());
    }

}
