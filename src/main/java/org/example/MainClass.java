package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.config.KafkaConfig;
import org.example.entity.DomainResolveResult;
import org.example.entity.DomainSituation;
import org.example.stream.DigStreamOperations;
import org.example.stream.DigStreamSink;

public class MainClass
{

    public static void main( String[] args ) throws Exception
    {
        // Initialize the basic environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> digSource = KafkaConfig.getKafkaSource("dig");
        DataStreamSource<String> digStream = env.fromSource(digSource, WatermarkStrategy.noWatermarks(), "dig-source");
        DataStream<DomainResolveResult> cleanedDigStream = DigStreamOperations.clean(digStream);
        DataStream<DomainSituation> reducedDigStream = DigStreamOperations.reduce(cleanedDigStream);

        DigStreamSink.cleanedStreamSink(cleanedDigStream);
        DigStreamSink.reducedStreamSink(reducedDigStream);

        env.execute("dns situation");

    }
}
