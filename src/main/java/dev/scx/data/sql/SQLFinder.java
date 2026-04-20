package dev.scx.data.sql;

import dev.scx.data.Finder;
import dev.scx.data.LockMode;
import dev.scx.data.exception.DataAccessException;
import dev.scx.data.field_policy.FieldPolicy;
import dev.scx.data.query.Query;
import dev.scx.exception.ScxWrappedException;
import dev.scx.function.Function1Void;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static dev.scx.sql.extractor.ResultSetExtractor.*;

public class SQLFinder<Entity> implements Finder<Entity> {

    private final SQLRepository<Entity> repository;
    private final Query query;
    private final FieldPolicy fieldPolicy;
    /// lockMode 可以为 null
    private final LockMode lockMode;

    public SQLFinder(SQLRepository<Entity> repository, Query query, FieldPolicy fieldPolicy) {
        this(repository, query, fieldPolicy, null);
    }

    public SQLFinder(SQLRepository<Entity> repository, Query query, FieldPolicy fieldPolicy, LockMode lockMode) {
        this.repository = repository;
        this.query = query;
        this.fieldPolicy = fieldPolicy;
        this.lockMode = lockMode;
    }

    @Override
    public List<Entity> list() throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildSelectSQL(query, fieldPolicy, lockMode), repository.entityBeanListExtractor);
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public <T> List<T> list(Class<T> resultType) throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildSelectSQL(query, fieldPolicy, lockMode), ofBeanList(resultType, repository.fieldColumnLabelMapping));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public List<Map<String, Object>> listMap() throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildSelectSQL(query, fieldPolicy, lockMode), ofMapList(repository.mapBuilder));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public void forEach(Function1Void<Entity, ?> entityConsumer) throws DataAccessException, ScxWrappedException {
        try {
            repository.sqlClient.query(repository.buildSelectSQL(query, fieldPolicy, lockMode), ofBeanConsumer(repository.beanBuilder, entityConsumer));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        } catch (Throwable e) {
            throw new ScxWrappedException(e);
        }
    }

    @Override
    public <T> void forEach(Function1Void<T, ?> entityConsumer, Class<T> resultType) throws DataAccessException, ScxWrappedException {
        try {
            repository.sqlClient.query(repository.buildSelectSQL(query, fieldPolicy, lockMode), ofBeanConsumer(resultType, repository.fieldColumnLabelMapping, entityConsumer));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        } catch (Throwable e) {
            throw new ScxWrappedException(e);
        }
    }

    @Override
    public void forEachMap(Function1Void<Map<String, Object>, ?> entityConsumer) throws DataAccessException, ScxWrappedException {
        try {
            repository.sqlClient.query(repository.buildSelectSQL(query, fieldPolicy, lockMode), ofMapConsumer(repository.mapBuilder, entityConsumer));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        } catch (Throwable e) {
            throw new ScxWrappedException(e);
        }
    }

    @Override
    public Entity first() throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildSelectFirstSQL(query, fieldPolicy, lockMode), repository.entityBeanExtractor);
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public <T> T first(Class<T> resultType) throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildSelectFirstSQL(query, fieldPolicy, lockMode), ofBean(resultType, repository.fieldColumnLabelMapping));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public Map<String, Object> firstMap() throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildSelectFirstSQL(query, fieldPolicy, lockMode), ofMap(repository.mapBuilder));
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public long count() throws DataAccessException {
        try {
            return repository.sqlClient.query(repository.buildCountSQL(query), repository.countResultExtractor);
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

}
