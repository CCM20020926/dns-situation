package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.config.JdbcSinkConfig;
import org.example.config.KafkaConfig;
import org.example.stream.DigStreamOperations;

import java.util.Map;

public class MainClass
{

    public static void main( String[] args ) throws Exception
    {
        // Initialize the basic environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> digSource = KafkaConfig.getKafkaSource("dig");
        KafkaSource<String> pingSource = KafkaConfig.getKafkaSource("ping");

        // Create the datastreams
        DataStreamSource<String> digStream = env.fromSource(digSource, WatermarkStrategy.noWatermarks(), "dig-source");
        DataStreamSource<String> pingStream = env.fromSource(pingSource, WatermarkStrategy.noWatermarks(), "ping-source");

        // Clean the dataStream

        DataStream<Map<String,String>> cleanedDigStream = DigStreamOperations.clean(digStream);
        cleanedDigStream.print();

        // Reduce the dataStream
        DataStream<Tuple8<String, String, String, String, String, Integer, Integer, Integer>> reducedDigStream = DigStreamOperations.reduce(cleanedDigStream);
        reducedDigStream.print();

        // Sink to database
        JdbcSinkConfig.addSink(cleanedDigStream,
                "INSERT INTO domain_resolve_result(timestamp, city, domain, resolver, type, answer)"+
                "VALUES (?,?,?,?,?,?);",
                ((statement, data) -> {
                    statement.setString(1, data.get("timestamp"));
                    statement.setString(2, data.get("city"));
                    statement.setString(3, data.get("domain"));
                    statement.setString(4, data.get("resolver"));
                    statement.setString(5, data.get("type"));
                    statement.setString(6, data.get("answer"));
                })
                );

        JdbcSinkConfig.addSink(reducedDigStream,
                "INSERT INTO dns_situation(timestamp, city, domain, resolver, type, total_answers, total_creds, true_answers)"
        + "VALUES(?,?,?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE total_answers=new.total_answers, total_creds=new.total_creds, true_answers=new.true_answers",
                ((statement, data) -> {
                    statement.setString(1, data.f0);
                    statement.setString(2,data.f1);
                    statement.setString(3, data.f2);
                    statement.setString(4, data.f3);
                    statement.setString(5, data.f4);
                    statement.setInt(6, data.f5);
                    statement.setInt(7, data.f6);
                    statement.setInt(8, data.f7);
                })
        );

        env.execute("dns situation");

    }
}
