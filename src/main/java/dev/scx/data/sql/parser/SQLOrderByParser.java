package dev.scx.data.sql.parser;

import dev.scx.data.query.OrderBy;

import java.util.ArrayList;

/// OrderByParser
///
/// @author scx567888
/// @version 0.0.1
public class SQLOrderByParser {

    private final SQLColumnNameParser columnNameParser;

    public SQLOrderByParser(SQLColumnNameParser columnNameParser) {
        this.columnNameParser = columnNameParser;
    }

    public String[] parse(OrderBy[] orderBys) {
        var list = new ArrayList<String>();
        for (var obj : orderBys) {
            var s = parseOrderBy(obj);
            list.add(s);
        }
        return list.toArray(String[]::new);
    }

    private String parseOrderBy(OrderBy o) {
        var columnName = columnNameParser.parseColumnName(o);
        return columnName + " " + o.orderByType().name();
    }

}
