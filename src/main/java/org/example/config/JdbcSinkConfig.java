package org.example.config;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class JdbcSinkConfig {

    private final static JdbcConnectionOptions connectionOptions;
    private final static  JdbcExecutionOptions executionOptions;

    static {
        Properties props = new Properties();
        InputStream inputStream = JdbcSinkConfig.class.getClassLoader().getResourceAsStream("jdbc.properties");
        try {
            props.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName(props.getProperty("jdbc.driver")).withUrl(props.getProperty("jdbc.url"))
                .withUsername(props.getProperty("jdbc.user")).withPassword(props.getProperty("jdbc.password")).build();

        executionOptions = JdbcExecutionOptions.builder().
                withBatchSize(Integer.parseInt(props.getProperty("jdbc.batchSize")))
                .withBatchIntervalMs(Integer.parseInt(props.getProperty("jdbc.batchIntervalMs")))
                .withMaxRetries(Integer.parseInt(props.getProperty("jdbc.maxRetries")))
                .build();
    }

    public static <T> DataStreamSink<T> addSink(DataStream<T> dataStream, String sql, JdbcStatementBuilder<T> statementBuilder) {
        return dataStream.addSink(
                JdbcSink.sink(
                        sql,
                        statementBuilder,
                        executionOptions,
                        connectionOptions
                )
        );
    }

}
