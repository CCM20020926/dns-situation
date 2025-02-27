package org.example.stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.entity.DomainResolveResult;
import org.example.entity.DomainSituation;

public class DigStreamOperations {
    public static DataStream<DomainResolveResult> clean(DataStream<String> originalStream)
    {
        return originalStream.flatMap(new DigStreamOperators.digCleanFlatMap());
    }

    public static DataStream<DomainSituation> reduce(DataStream<DomainResolveResult> cleanedStream)
    {
        return cleanedStream.map(new DigStreamOperators.digMap()).keyBy(
                new KeySelector<DomainSituation, Tuple5<String, String, String, String, String>>() {

                    @Override
                    public Tuple5<String, String, String, String, String> getKey(DomainSituation domainSituation) throws Exception {
                        return new Tuple5<>(domainSituation.timestamp, domainSituation.city, domainSituation.resolver, domainSituation.domain, domainSituation.type);
                    }
                }
        ).reduce(new DigStreamOperators.digReduce());
    }
}
