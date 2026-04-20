package dev.scx.data.sql.schema_mapping;

import dev.scx.sql.schema.Table;

public interface EntityTable<Entity> extends Table {

    Class<Entity> entityClass();

    @Override
    EntityColumn[] columns();

    @Override
    EntityColumn getColumn(String name);

}
