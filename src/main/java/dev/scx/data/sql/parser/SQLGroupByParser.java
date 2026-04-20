package dev.scx.data.sql.parser;

import dev.scx.data.aggregation.FieldGroupBy;

/// GroupByParser
///
/// @author scx567888
/// @version 0.0.1
public class SQLGroupByParser {

    private final SQLColumnNameParser columnNameParser;

    public SQLGroupByParser(SQLColumnNameParser columnNameParser) {
        this.columnNameParser = columnNameParser;
    }

    public String parseGroupBy(FieldGroupBy g) {
        return columnNameParser.parseColumnName(g);
    }

}
