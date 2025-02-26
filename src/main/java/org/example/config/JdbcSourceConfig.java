package org.example.config;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcSourceConfig {

    private static JdbcTemplate jdbcTemplate;

    static {
        Properties props = new Properties();
        InputStream inputStream = JdbcSinkConfig.class.getClassLoader().getResourceAsStream("jdbc.properties");
        try {
            props.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String url = props.getProperty("jdbc.source.url");
        String user = props.getProperty("jdbc.source.user");
        String password = props.getProperty("jdbc.source.password");

        DataSource ds = new DriverManagerDataSource(url, user, password);
        jdbcTemplate = new JdbcTemplate(ds);
    }

    public static JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

}
