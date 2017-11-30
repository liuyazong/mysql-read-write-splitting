package yz.mysql;

import com.mysql.cj.jdbc.Driver;
import com.mysql.cj.jdbc.ha.ReplicationMySQLConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.*;

/**
 * author: liuyazong
 * datetime: 2017/11/30 下午5:38
 */
@Slf4j
public class Main {
    public static void main(String[] args) throws SQLException {
        DataSource dataSource = new DataSource();
        dataSource.setUrl("jdbc:mysql:replication://127.0.0.1:3306,127.0.0.1:3307/dev?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("mysql");
        dataSource.setInitialSize(20);
        dataSource.setMaxActive(200);
        dataSource.setMinIdle(20);
        dataSource.setMaxIdle(20);
        dataSource.setMaxWait(5000);
        dataSource.setInitSQL("select 1");
        dataSource.setValidationQuery("select 1");
        String name = Driver.class.getName();
        dataSource.setDriverClassName(name);

        //读read，从库
        {
            Connection connection = dataSource.getConnection();
            connection.setReadOnly(true);//设置连接只读true，切换到slave
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

        //读read，主库
        {
            Connection connection = dataSource.getConnection();
            connection.setReadOnly(false);//设置连接只读为false，切换到master
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
        //写write，主库
        {
            Connection connection = dataSource.getConnection();
            connection.setReadOnly(false);//设置连接只读为false，切换到master
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
        }
    }
}
