package org.example.config;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {

    private static final String bootstrapServer;

    static {
        Properties props = new Properties();
        InputStream inputStream = KafkaConfig.class.getClassLoader().getResourceAsStream("kafka.properties");
        try {
            props.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        bootstrapServer = props.getProperty("broker.mode").equals("INTERNAL") ? props.getProperty("broker.internal.socket") : props.getProperty("broker.external.socket");

    }

   // private static final
    public static  KafkaSource<String> getKafkaSource(String topic) throws IOException
    {

        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(topic)
                .setGroupId("dns-situation")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

    }
}
