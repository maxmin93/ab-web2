package net.bitnine.agensbrowser.web.config.dao;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class PersistenceOuterConfig {

    @Autowired
    private Environment env;

    public PersistenceOuterConfig() {
        super();
    }

    // beans

    @Bean(name = "outerDataSource")
    public DataSource dataSource() {

        final HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(env.getProperty("agens.outer.datasource.driverClassName"));
        dataSource.setJdbcUrl(env.getProperty("agens.outer.datasource.url"));
        dataSource.setUsername(env.getProperty("agens.outer.datasource.username"));
        dataSource.setPassword(env.getProperty("agens.outer.datasource.password"));
        // **NOTE: If your driver supports JDBC4 we strongly recommend not setting this property
        // dataSource.setConnectionTestQuery("SELECT 1 as test");

        // adjust only for AgensGraph
        String graphPath = env.getProperty("agens.outer.datasource.graph_path");
        dataSource.setSchema(graphPath);

        // set session variables by initSql
        dataSource.setConnectionInitSql(
                "set client_encoding='utf8'; "+
                "SET GRAPH_PATH="+graphPath+"; "+
                "set statement_timeout to "+env.getProperty("agens.api.query-timeout")+";"
        );

        // Hikari Connection-Pool Config
        dataSource.setMinimumIdle(5);
        dataSource.setMaximumPoolSize(10);
        // dataSource.setIdleTimeout(30000);    // **NOTE: Driver is not yet implemented
        dataSource.setPoolName("agensHikariCP");
        // dataSource.setRegisterMbeans(true);  // **ERROR: MXBean already registered

        dataSource.addDataSourceProperty("dataSource.cachePrepStmts", "true");
        dataSource.addDataSourceProperty("dataSource.prepStmtCacheSize", "250");
        dataSource.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", "2048");
        dataSource.addDataSourceProperty("dataSource.useServerPrepStmts", "true");
        dataSource.addHealthCheckProperty("connectivityCheckTimeoutMs", "1000");

        System.out.println("<config> agens.datasource.url = "+env.getProperty("agens.outer.datasource.url"));
        System.out.println("<config> agens.datasource.schema = "+graphPath);

        return dataSource;
    }
/*
    // **NOTE: 필요 없음!
    // 참고 https://stackoverflow.com/questions/50770462/springboot-2-monitor-db-connection
    //
    @Bean(name = "outerPoolMXBean")
    public HikariPoolMXBean outerPoolMXBean(@Qualifier("outerDataSource") HikariDataSource dataSource){
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        HikariPoolMXBean poolProxy = null;
        try {
            ObjectName poolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + dataSource.getPoolName() + ")");
            poolProxy = JMX.newMXBeanProxy(mBeanServer, poolName, HikariPoolMXBean.class);
        }catch(Exception e){
            System.out.println("ERROR at outerPoolMXBean: '"+dataSource.getPoolName()+"' cannot be parsed");
        }
        return poolProxy;
    }
*/
    @Bean(name = "outerJdbcTemplate")
    public JdbcTemplate outerJdbcTemplate(@Qualifier("outerDataSource") DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

}