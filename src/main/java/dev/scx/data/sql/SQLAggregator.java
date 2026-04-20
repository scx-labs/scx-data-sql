package dev.scx.data.sql;

import dev.scx.data.Aggregator;
import dev.scx.data.aggregation.Aggregation;
import dev.scx.data.exception.DataAccessException;
import dev.scx.data.query.Query;
import dev.scx.exception.ScxWrappedException;
import dev.scx.function.Function1Void;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static dev.scx.sql.extractor.ResultSetExtractor.*;

public class SQLAggregator implements Aggregator {

    private final SQLRepository<?> repository;
    private final Query beforeAggregateQuery;
    private final Aggregation aggregation;
    private final Query afterAggregateQuery;

    public SQLAggregator(SQLRepository<?> repository, Query beforeAggregateQuery, Aggregation aggregation, Query afterAggregateQuery) {
        this.repository = repository;
        this.beforeAggregateQuery = beforeAggregateQuery;
        this.aggregation = aggregation;
        this.afterAggregateQuery = afterAggregateQuery;
    }

    @Override
    public <T> List<T> list(Class<T> resultType) throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildAggregateSQL(beforeAggregateQuery, aggregation, afterAggregateQuery), ofBeanList(resultType, repository.fieldColumnLabelMapping));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public List<Map<String, Object>> list() throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildAggregateSQL(beforeAggregateQuery, aggregation, afterAggregateQuery), ofMapList(repository.mapBuilder));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public <T> void forEach(Function1Void<T, ?> resultConsumer, Class<T> resultType) throws DataAccessException, ScxWrappedException {
        try {
            repository.sqlClient.query(repository.buildAggregateSQL(beforeAggregateQuery, aggregation, afterAggregateQuery), ofBeanConsumer(resultType, repository.fieldColumnLabelMapping, resultConsumer));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        } catch (Throwable e) {
            throw new ScxWrappedException(e);
        }
    }

    @Override
    public void forEach(Function1Void<Map<String, Object>, ?> resultConsumer) throws DataAccessException, ScxWrappedException {
        try {
            repository.sqlClient.query(repository.buildAggregateSQL(beforeAggregateQuery, aggregation, afterAggregateQuery), ofMapConsumer(repository.mapBuilder, resultConsumer));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        } catch (Throwable e) {
            throw new ScxWrappedException(e);
        }
    }

    @Override
    public <T> T first(Class<T> resultType) throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildAggregateFirstSQL(beforeAggregateQuery, aggregation, afterAggregateQuery), ofBean(resultType, repository.fieldColumnLabelMapping));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public Map<String, Object> first() throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildAggregateFirstSQL(beforeAggregateQuery, aggregation, afterAggregateQuery), ofMap(repository.mapBuilder));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

}
