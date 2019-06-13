package jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import conf.ConfigurationManager;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public enum JDBCHelplerEnum {
    Instance;

    DruidDataSource dds = null;

    JDBCHelplerEnum()  {
        try {
            Properties properties = new Properties();
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            properties.load(in);
            dds = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public  Connection getConnection() {
        Connection conn = null;
        try {
            conn = dds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

}
