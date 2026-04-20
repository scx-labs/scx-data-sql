package dev.scx.data.sql.test;

import dev.scx.data.LockMode;
import dev.scx.data.sql.SQLRepository;
import dev.scx.sql.SQL;
import dev.scx.sql.support.TableSupport;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static dev.scx.data.field_policy.FieldPolicyBuilder.include;
import static dev.scx.data.query.QueryBuilder.eq;
import static dev.scx.data.sql.test.DataSQLTest.sqlContext;

/// 转账测试
public class TransferTest {

    public static SQLRepository<User> userRepository;

    public static void main(String[] args) throws SQLException, InterruptedException {
        beforeTest();
        testConcurrentTransfers();
    }

    @BeforeTest
    public static void beforeTest() throws SQLException {
        sqlContext.sqlClient().execute(SQL.sql("drop table if exists user"));
        userRepository = new SQLRepository<>(User.class, sqlContext);
        TableSupport.fixTable(userRepository.table(), sqlContext);
        //清空
        userRepository.clear();
    }

    public static void transfer(Long fromUserId, Long toUserId, Long amount) throws SQLException {
        sqlContext.sqlClient().autoTransaction(() -> {
            var fromUser = userRepository.findFirst(eq("id", fromUserId), LockMode.EXCLUSIVE);
            var toUser = userRepository.findFirst(eq("id", toUserId), LockMode.EXCLUSIVE);

            if (fromUser == null || toUser == null) {
                throw new RuntimeException("用户不存在");
            }
            if (fromUser.money < amount) {
                throw new RuntimeException("余额不足");
            }

            fromUser.money -= amount;
            toUser.money += amount;

            userRepository.update(fromUser, include("money"), eq("id", fromUserId));
            userRepository.update(toUser, include("money"), eq("id", toUserId));

        });
    }

    @Test
    public static void testConcurrentTransfers() throws SQLException, InterruptedException {
        // 初始化两条用户数据
        var user1 = new User();
        user1.name = "Alice";
        user1.money = 10000L;
        user1.id = userRepository.add(user1);

        var user2 = new User();
        user2.name = "Bob";
        user2.money = 0L;
        user2.id = userRepository.add(user2);

        System.out.println("转账前: ");
        System.out.println("Alice 钱: " + user1.money);
        System.out.println("Bob 钱: " + user2.money);

        int threadCount = 50;
        long transferAmount = 200L;
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i = i + 1) {
            var t = Thread.ofVirtual().start(() -> {
                try {
                    transfer(user1.id, user2.id, transferAmount);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }

        var updatedUser1 = userRepository.findFirst(eq("id", user1.id));
        var updatedUser2 = userRepository.findFirst(eq("id", user2.id));

        System.out.println("转账后: ");
        System.out.println("Alice 钱: " + updatedUser1.money);
        System.out.println("Bob 钱: " + updatedUser2.money);

        long expectedUser1Money = user1.money - threadCount * transferAmount;
        long expectedUser2Money = user2.money + threadCount * transferAmount;

        if (updatedUser1.money != expectedUser1Money || updatedUser2.money != expectedUser2Money) {
            throw new AssertionError("余额不正确, 出现了并发异常");
        } else {
            System.out.println("测试通过, 余额正确无误");
        }
    }

}
