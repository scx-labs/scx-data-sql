package dev.scx.data.sql.test;

import com.mysql.cj.jdbc.MysqlDataSource;
import dev.scx.array.ScxArray;
import dev.scx.data.sql.SQLRepository;
import dev.scx.jdbc.spy.ScxJdbcSpy;
import dev.scx.jdbc.spy.listener.logging.LoggingDataSourceListener;
import dev.scx.jdbc.spy.listener.logging.PreparedStatementLogStyle;
import dev.scx.logging.ScxLogging;
import dev.scx.sql.SQL;
import dev.scx.sql.SQLClient;
import dev.scx.sql.dialect.Dialect;
import dev.scx.sql.dialect.DialectSelector;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.mysql.cj.conf.PropertyKey.*;
import static dev.scx.data.aggregation.AggregationBuilder.agg;
import static dev.scx.data.aggregation.AggregationBuilder.groupBy;
import static dev.scx.data.field_policy.FieldPolicyBuilder.exclude;
import static dev.scx.data.field_policy.FieldPolicyBuilder.virtualField;
import static dev.scx.data.query.QueryBuilder.eq;
import static dev.scx.data.query.QueryBuilder.lt;

public class DataSQLTest {

    public static final DataSource dataSource = getMySQLDataSource();
    public static final SQLClient sqlClient = SQLClient.of(dataSource);
    public static final Dialect dialect = DialectSelector.findDialect(dataSource);
    public static SQLRepository<Car> carRepository;

    static {
        ScxLogging.getLogger("ScxJdbcSpy").config().setLevel(System.Logger.Level.DEBUG);
    }

    public static void main(String[] args) throws SQLException {
        beforeTest();
        testAll();
    }

    private static DataSource getMySQLDataSource() {
        var mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setServerName("127.0.0.1");
        mysqlDataSource.setDatabaseName("scx");
        mysqlDataSource.setUser("root");
        mysqlDataSource.setPassword("root");
        mysqlDataSource.setPort(3306);
        // 设置参数值
        mysqlDataSource.getProperty(allowMultiQueries).setValue(true);
        mysqlDataSource.getProperty(rewriteBatchedStatements).setValue(true);
        mysqlDataSource.getProperty(createDatabaseIfNotExist).setValue(true);
        return ScxJdbcSpy.spy(mysqlDataSource, new LoggingDataSourceListener(PreparedStatementLogStyle.RENDERED_SQL));
    }

    @BeforeTest
    public static void beforeTest() throws SQLException {
        sqlClient.execute(SQL.sql("drop table if exists car"));
        carRepository = new SQLRepository<>(Car.class, sqlClient,dialect);
        var createTableDDLs = dialect.getCreateTableDDLs(carRepository.table());
        for (var createTableDDL : createTableDDLs) {
            sqlClient.execute(SQL.sql(createTableDDL));
        }
        //清空
        carRepository.clear();
    }

    @Test
    public static void testAll() throws SQLException {
        testAdd();
        testAdd2();
        testFind();
        testAgg();
    }

    public static void testAdd() throws SQLException {
        var s = new ArrayList<Long>();
        //测试单条插入
        for (int i = 0; i < 10; i = i + 1) {
            var car = new Car();
            car.name = ScxArray.randomGet("奔驰", "宝马", "奥迪");
            car.size = i;
            car.city = "city" + i;
            //测试混合插入
            Long id = carRepository.add(car, exclude("city").assignField("color", """
                CONCAT('#',
                    LPAD(HEX(FLOOR(RAND() * 256)), 2, '0'),
                    LPAD(HEX(FLOOR(RAND() * 256)), 2, '0'),
                    LPAD(HEX(FLOOR(RAND() * 256)), 2, '0')
                  )
                """));
            s.add(id);
        }
        Assert.assertEquals(s, List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
        //
        //测试列表查询 应该有 color 但是没有 city
        List<Car> cars = carRepository.find();
        for (Car car : cars) {
            if (car.city != null || car.color == null) {
                Assert.fail();
            }
        }
        //测试 map 查询 这里测试的是 map 是否和 entity 的字段名相符而不是数据库中的名字
        var carMaps = carRepository.finder().listMap();
        for (var carMAP : carMaps) {
            if (carMAP.get("city") != null || carMAP.get("color") == null || carMAP.get("size") == null) {
                Assert.fail();
            }
        }
        carRepository.clear();
    }

    public static void testAdd2() {
        //测试批量插入
        var carList = new ArrayList<Car>();
        for (int i = 0; i < 10; i = i + 1) {
            var car = new Car();
            car.name = ScxArray.randomGet("奔驰", "宝马", "奥迪");
            car.size = i;
            carList.add(car);
        }
        List<Long> add = carRepository.add(carList, exclude("id"));
        Assert.assertEquals(add, List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
    }

    public static void testFind() {
        var carList = new ArrayList<Car>();
        carRepository.finder().forEach(c -> {
            carList.add(c);
        });
        Assert.assertEquals(carList.size(), 10);
        var cars = carRepository.finder(virtualField("name", "REVERSE(name)")).listMap();
        Assert.assertEquals(cars.size(), 10);
    }

    public static void testAgg() {
        var list1 = carRepository.aggregate(lt("id", 3), groupBy("name").agg("totalSize", "SUM(size)"), eq("name", "奔驰"));
        var list2 = carRepository.aggregateFirst(agg("totalSize", "SUM(size)"));
        System.out.println(list2);
    }

}
