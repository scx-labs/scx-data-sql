package dev.scx.data.sql;

import dev.scx.data.*;
import dev.scx.data.aggregation.Aggregation;
import dev.scx.data.exception.DataAccessException;
import dev.scx.data.field_policy.FieldPolicy;
import dev.scx.data.query.Query;
import dev.scx.data.sql.label_mapping.FieldColumnLabelMapping;
import dev.scx.data.sql.label_mapping.MapKeyMapping;
import dev.scx.data.sql.parser.SQLColumnNameParser;
import dev.scx.data.sql.parser.SQLGroupByParser;
import dev.scx.data.sql.parser.SQLOrderByParser;
import dev.scx.data.sql.parser.SQLWhereParser;
import dev.scx.data.sql.schema_mapping.AnnotationConfigTable;
import dev.scx.data.sql.schema_mapping.EntityTable;
import dev.scx.data.sql.sql_builder.*;
import dev.scx.sql.BatchSQL;
import dev.scx.sql.SQL;
import dev.scx.sql.SQLClient;
import dev.scx.sql.dialect.Dialect;
import dev.scx.sql.extractor.ResultSetExtractor;
import dev.scx.sql.extractor.bean.BeanBuilder;
import dev.scx.sql.extractor.map.MapBuilder;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import static dev.scx.sql.SQL.sql;
import static dev.scx.sql.extractor.ResultSetExtractor.*;

/// SQLRepository
///
/// @author scx567888
/// @version 0.0.1
public class SQLRepository<Entity> implements AggregatableRepository<Entity, Long>, LockableRepository<Entity, Long> {

    // *********** 基本字段 ***************
    final Class<Entity> entityClass;
    final EntityTable<Entity> table;
    final SQLClient sqlClient;
    final Dialect dialect;

    // *********** 结果解析器 ***************
    final FieldColumnLabelMapping fieldColumnLabelMapping;
    final MapKeyMapping mapKeyMapping;
    final BeanBuilder<Entity> beanBuilder;
    final MapBuilder mapBuilder;
    final ResultSetExtractor<List<Entity>, RuntimeException> entityBeanListExtractor;
    final ResultSetExtractor<Entity, RuntimeException> entityBeanExtractor;
    final ResultSetExtractor<Long, RuntimeException> countResultExtractor;

    // *********** SQL 语句构造器 ***************
    final InsertSQLBuilder insertSQLBuilder;
    final SelectSQLBuilder selectSQLBuilder;
    final UpdateSQLBuilder updateSQLBuilder;
    final DeleteSQLBuilder deleteSQLBuilder;
    final CountSQLBuilder countSQLBuilder;
    final AggregateSQLBuilder aggregateSQLBuilder;

    public SQLRepository(Class<Entity> entityClass, SQLClient sqlClient,Dialect dialect) {
        this(new AnnotationConfigTable<>(entityClass), sqlClient,dialect);
    }

    public SQLRepository(EntityTable<Entity> table, SQLClient sqlClient,Dialect dialect) {
        // 1, 初始化基本字段
        this.entityClass = table.entityClass();
        this.table = table;
        this.sqlClient = sqlClient;
        this.dialect = dialect;

        // 2, 创建返回值解析器
        this.fieldColumnLabelMapping = new FieldColumnLabelMapping(table);
        this.mapKeyMapping = new MapKeyMapping(table);
        this.beanBuilder = BeanBuilder.of(this.entityClass, fieldColumnLabelMapping);
        this.mapBuilder = MapBuilder.of(mapKeyMapping);
        this.entityBeanListExtractor = ofBeanList(beanBuilder);
        this.entityBeanExtractor = ofBean(beanBuilder);
        this.countResultExtractor = ofSingleValue("count", Long.class);

        // 3, 创建 SQL 语句构造器
        var columnNameParser = new SQLColumnNameParser(table, this.dialect);
        var whereParser = new SQLWhereParser(columnNameParser, this.dialect);
        var groupByParser = new SQLGroupByParser(columnNameParser);
        var orderByParser = new SQLOrderByParser(columnNameParser);
        this.insertSQLBuilder = new InsertSQLBuilder(table, this.dialect, columnNameParser);
        this.selectSQLBuilder = new SelectSQLBuilder(table, this.dialect, whereParser, orderByParser);
        this.updateSQLBuilder = new UpdateSQLBuilder(table, this.dialect, columnNameParser, whereParser, orderByParser);
        this.deleteSQLBuilder = new DeleteSQLBuilder(table, this.dialect, whereParser, orderByParser);
        this.countSQLBuilder = new CountSQLBuilder(table, this.dialect, whereParser);
        this.aggregateSQLBuilder = new AggregateSQLBuilder(table, this.dialect, whereParser, groupByParser, orderByParser);
    }

    @Override
    public final Long add(Entity entity, FieldPolicy fieldPolicy) throws DataAccessException {
        try {
            return sqlClient.update(buildInsertSQL(entity, fieldPolicy)).firstGeneratedKey();
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public final List<Long> add(Collection<Entity> entityList, FieldPolicy fieldPolicy) throws DataAccessException {
        try {
            return sqlClient.update(buildInsertBatchSQL(entityList, fieldPolicy)).generatedKeys();
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public final Finder<Entity> finder(Query query, FieldPolicy fieldPolicy, LockMode lockMode) {
        return new SQLFinder<>(this, query, fieldPolicy, lockMode);
    }

    @Override
    public final Finder<Entity> finder(Query query, FieldPolicy fieldPolicy) {
        return new SQLFinder<>(this, query, fieldPolicy);
    }

    @Override
    public final long update(Entity entity, FieldPolicy fieldPolicy, Query query) throws DataAccessException {
        try {
            return sqlClient.update(buildUpdateSQL(entity, fieldPolicy, query)).affectedItemsCount();
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public final long delete(Query query) throws DataAccessException {
        try {
            return sqlClient.update(buildDeleteSQL(query)).affectedItemsCount();
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public final void clear() throws DataAccessException {
        try {
            sqlClient.execute(sql("truncate " + table.name()));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public final Aggregator aggregator(Query beforeAggregateQuery, Aggregation aggregation, Query afterAggregateQuery) {
        return new SQLAggregator(this, beforeAggregateQuery, aggregation, afterAggregateQuery);
    }

    public final Class<Entity> entityClass() {
        return entityClass;
    }

    public final EntityTable<Entity> table() {
        return table;
    }

    public SQLClient sqlClient() {
        return sqlClient;
    }

    public Dialect dialect() {
        return dialect;
    }

    public BeanBuilder<Entity> beanBuilder() {
        return beanBuilder;
    }

    public ResultSetExtractor<List<Entity>, RuntimeException> entityBeanListExtractor() {
        return entityBeanListExtractor;
    }

    public ResultSetExtractor<Entity, RuntimeException> entityBeanExtractor() {
        return entityBeanExtractor;
    }

    public SQL buildInsertSQL(Entity entity, FieldPolicy fieldPolicy) {
        return insertSQLBuilder.buildInsertSQL(entity, fieldPolicy);
    }

    public BatchSQL buildInsertBatchSQL(Collection<? extends Entity> entityList, FieldPolicy fieldPolicy) {
        return insertSQLBuilder.buildInsertBatchSQL(entityList, fieldPolicy);
    }

    public SQL buildSelectSQL(Query query, FieldPolicy fieldPolicy) {
        return selectSQLBuilder.buildSelectSQL(query, fieldPolicy);
    }

    public SQL buildSelectFirstSQL(Query query, FieldPolicy fieldPolicy) {
        return selectSQLBuilder.buildSelectFirstSQL(query, fieldPolicy);
    }

    public SQL buildUpdateSQL(Entity entity, FieldPolicy fieldPolicy, Query query) {
        return updateSQLBuilder.buildUpdateSQL(entity, fieldPolicy, query);
    }

    public SQL buildDeleteSQL(Query query) {
        return deleteSQLBuilder.buildDeleteSQL(query);
    }

    public SQL buildCountSQL(Query query) {
        return countSQLBuilder.buildCountSQL(query);
    }

    public SQL buildSelectFirstSQLWithAlias(Query query, FieldPolicy fieldPolicy) {
        return selectSQLBuilder.buildSelectFirstSQLWithAlias(query, fieldPolicy);
    }

    public SQL buildSelectSQLWithAlias(Query query, FieldPolicy fieldPolicy) {
        return selectSQLBuilder.buildSelectSQLWithAlias(query, fieldPolicy);
    }

    public SQL buildAggregateSQL(Query beforeAggregateQuery, Aggregation aggregation, Query afterAggregateQuery) {
        return aggregateSQLBuilder.buildAggregateSQL(beforeAggregateQuery, aggregation, afterAggregateQuery);
    }

    public SQL buildAggregateFirstSQL(Query beforeAggregateQuery, Aggregation aggregation, Query afterAggregateQuery) {
        return aggregateSQLBuilder.buildAggregateFirstSQL(beforeAggregateQuery, aggregation, afterAggregateQuery);
    }

    /// lockMode 可以为 null
    public SQL buildSelectSQL(Query query, FieldPolicy fieldPolicy, LockMode lockMode) {
        return selectSQLBuilder.buildSelectSQL(query, fieldPolicy, lockMode);
    }

    /// lockMode 可以为 null
    public SQL buildSelectFirstSQL(Query query, FieldPolicy fieldPolicy, LockMode lockMode) {
        return selectSQLBuilder.buildSelectFirstSQL(query, fieldPolicy, lockMode);
    }

}
