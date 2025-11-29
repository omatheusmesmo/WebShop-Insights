package br.com.analytics.pipeline.product_ranking_batch.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Autowired
    private Environment env;

    @Bean(name = "appDataSource")
    public DataSource appDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(env.getProperty("spring.datasource.app.driver-class-name"));
        dataSource.setJdbcUrl(env.getProperty("spring.datasource.app.url"));
        dataSource.setUsername(env.getProperty("spring.datasource.app.username"));
        dataSource.setPassword(env.getProperty("spring.datasource.app.password"));
        return dataSource;
    }

    @Bean(name = "batchDataSource")
    public DataSource batchDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(env.getProperty("spring.datasource.batch.driver-class-name"));
        dataSource.setJdbcUrl(env.getProperty("spring.datasource.batch.url"));
        dataSource.setUsername(env.getProperty("spring.datasource.batch.username"));
        dataSource.setPassword(env.getProperty("spring.datasource.batch.password"));
        return dataSource;
    }

    @Bean(name = "appTransactionManager")
    public DataSourceTransactionManager appTransactionManager(@Qualifier("appDataSource") DataSource appDataSource) {
        return new DataSourceTransactionManager(appDataSource);
    }

    @Bean(name = "batchTransactionManager")
    public DataSourceTransactionManager batchTransactionManager(@Qualifier("batchDataSource") DataSource batchDataSource) {
        return new DataSourceTransactionManager(batchDataSource);
    }

}