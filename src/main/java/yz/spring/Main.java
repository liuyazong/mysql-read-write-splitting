package yz.spring;

import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * author: liuyazong
 * datetime: 2017/11/30 上午10:24
 */
@Slf4j
public class Main {

    public static void main(String[] args) throws SQLException, InterruptedException {

        //将所使用数据源的key存储在线程本地变量中，默认使用master
        ThreadLocal<Object> threadLocal = ThreadLocal.withInitial(() -> "master");

        //实现protected Object determineCurrentLookupKey()方法
        AbstractRoutingDataSource dataSource = new AbstractRoutingDataSource() {
            @Override
            protected Object determineCurrentLookupKey() {
                Object o = threadLocal.get();
                log.info("db {} selected", o);
                return o;
            }

            //just for log
            //根据上面protected Object determineCurrentLookupKey()返回值决定使用那个数据源
            @Override
            protected javax.sql.DataSource determineTargetDataSource() {
                javax.sql.DataSource source = super.determineTargetDataSource();
                log.info("db {} selected.", source);
                return source;
            }
        };

        Map<Object, Object> targetDataSources = new HashMap<>();
        //配置master数据源
        {
            DataSource master = new DataSource();
            master.setUrl("jdbc:mysql://127.0.0.1:3306/dev?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&useSSL=false");
            master.setUsername("root");
            master.setPassword("mysql");
            master.setInitialSize(20);
            master.setMaxActive(200);
            master.setMinIdle(20);
            master.setMaxIdle(20);
            master.setMaxWait(5000);
            master.setInitSQL("select 1");
            master.setValidationQuery("select 1");
            String name = Driver.class.getName();
            master.setDriverClassName(name);

            targetDataSources.put("master", master);

            dataSource.setDefaultTargetDataSource(master);
        }
        //配置slave数据源
        {
            DataSource slave = new DataSource();
            slave.setUrl("jdbc:mysql://127.0.0.1:3307/dev?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&useSSL=false");
            slave.setUsername("root");
            slave.setPassword("mysql");
            slave.setInitialSize(20);
            slave.setMaxActive(200);
            slave.setMinIdle(20);
            slave.setMaxIdle(20);
            slave.setMaxWait(5000);
            slave.setInitSQL("select 1");
            slave.setValidationQuery("select 1");
            String name = Driver.class.getName();
            slave.setDriverClassName(name);

            targetDataSources.put("slave", slave);

            dataSource.setTargetDataSources(targetDataSources);
        }

        dataSource.afterPropertiesSet();


        Thread slave = new Thread(() -> {
            try {
                threadLocal.set("slave");
                Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement("select * from test where id = ?;");
                preparedStatement.setInt(1, 1);
                boolean execute = preparedStatement.execute();
                ResultSet resultSet = preparedStatement.getResultSet();
                resultSet.first();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    int columnType = metaData.getColumnType(i + 1);
                    Object object = resultSet.getObject(i + 1);
                    log.info(String.format("column:%-15s, type:%-5s, value:%s", columnName, columnType, object));
                }
                resultSet.close();
                connection.close();
                threadLocal.remove();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        slave.start();
        slave.join();

        Thread master = new Thread(() -> {
            try {
                threadLocal.set("master");
                Connection connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test(test) VALUES (?);");
                preparedStatement.setString(1, "test master");
                boolean execute = preparedStatement.execute();
                Statement statement = connection.createStatement();
                statement.execute("SELECT last_insert_id();");
                ResultSet resultSet = statement.getResultSet();
                resultSet.first();
                log.info(String.format("id:%-15s", resultSet.getInt(1)));
                connection.commit();
                connection.close();
                threadLocal.remove();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        master.start();
        master.join();


        Thread slaveMaster = new Thread(() -> {
            try {
                {
                    Connection connection = dataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement("select * from test where id = ?;");
                    preparedStatement.setInt(1, 1);
                    boolean execute = preparedStatement.execute();
                    ResultSet resultSet = preparedStatement.getResultSet();
                    resultSet.first();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        String columnName = metaData.getColumnName(i + 1);
                        int columnType = metaData.getColumnType(i + 1);
                        Object object = resultSet.getObject(i + 1);
                        log.info(String.format("column:%-15s, type:%-5s, value:%s", columnName, columnType, object));
                    }
                    resultSet.close();
                    connection.close();
                }


                {
                    threadLocal.set("master");
                    Connection connection = dataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement("select * from test where id = ?;");
                    preparedStatement.setInt(1, 51);
                    boolean execute = preparedStatement.execute();
                    ResultSet resultSet = preparedStatement.getResultSet();
                    resultSet.first();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        String columnName = metaData.getColumnName(i + 1);
                        int columnType = metaData.getColumnType(i + 1);
                        Object object = resultSet.getObject(i + 1);
                        log.info(String.format("column:%-15s, type:%-5s, value:%s", columnName, columnType, object));
                    }
                    resultSet.close();
                    connection.close();
                    threadLocal.remove();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        slaveMaster.start();
        slaveMaster.join();
    }
}
