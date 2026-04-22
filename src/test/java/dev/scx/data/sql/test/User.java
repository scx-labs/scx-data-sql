package dev.scx.data.sql.test;

import dev.scx.data.sql.annotation.Column;
import dev.scx.data.sql.annotation.Table;

@Table("user")
public class User {

    @Column(primary = true, autoIncrement = true)
    public Long id;

    @Column(defaultValue = "0")
    public Long money;

    public String name;

}
