package dev.scx.data.sql.test;

import dev.scx.data.sql.annotation.Column;
import dev.scx.data.sql.annotation.Table;

@Table("car")
public class Car {

    /// id
    @Column(primary = true, autoIncrement = true)
    public Long id;

    public String name;

    @Column(columnName = "sIzE")
    public Integer size;

    public String color;

    @Column(columnName = "CITY")
    public String city;

}
