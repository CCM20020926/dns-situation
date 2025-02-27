package org.example.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.config.JdbcSinkConfig;
import org.example.config.KafkaConfig;
import org.example.entity.DomainResolveResult;
import org.example.entity.DomainSituation;

public class DigStreamSink {
    public static void cleanedStreamSink(DataStream<DomainResolveResult> cleanedStream)
    {
        JdbcSinkConfig.addSink(cleanedStream,
                "INSERT INTO domain_resolve_result(timestamp, city, domain, resolver, type, answer, ttl)" +
                        "VALUES(?,?,?,?,?,?,?)",
                ((statement, data) -> {
                    statement.setString(1, data.timestamp);
                    statement.setString(2, data.city);
                    statement.setString(3, data.domain);
                    statement.setString(4, data.resolver);
                    statement.setString(5, data.type);
                    statement.setString(6, data.answer);
                    statement.setInt(7, data.ttl);
                })
        );
    }

    public static void reducedStreamSink(DataStream<DomainSituation> reducedStream)
    {
        JdbcSinkConfig.addSink(reducedStream,
                "INSERT INTO domain_situation(timestamp, city, domain, resolver, type, total_answers, total_creds, true_answers)"
                        + "VALUES(?,?,?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE total_answers=new.total_answers, total_creds=new.total_creds, true_answers=new.true_answers",
                ((statement, data) -> {
                    statement.setString(1, data.timestamp);
                    statement.setString(2,data.city);
                    statement.setString(3, data.domain);
                    statement.setString(4, data.resolver);
                    statement.setString(5, data.type);
                    statement.setInt(6, data.total_answers);
                    statement.setInt(7, data.total_creds);
                    statement.setInt(8, data.true_answers);
                })
        );
    }
}
