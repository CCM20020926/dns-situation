package org.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.example.datasource.CredDataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class DigStreamOperations {
    public static DataStream<Map<String, String>> clean(DataStream<String> originalStream)
    {
        // 清洗后格式：(timestamp,city,domain,resolver,type,answer)
        return originalStream.flatMap(new FlatMapFunction<String, Map<String, String>>() {

            @Override
            public void flatMap(String s, Collector<Map<String, String>> collector) throws Exception {


                Map<String, Object> deserializedStream = new ObjectMapper().readValue(s, Map.class);

                Map<String, Object> response = (Map<String, Object>) deserializedStream.get("response");
                if(response == null)
                {
                    return;
                }

                List<Map<String, Object>> answers = (List<Map<String, Object>>) response.get("answers");
                if (answers == null) {
                    return; // 如果 answers 为 null，则忽略此条记录
                }

                for(Map<String, Object> answer :  answers)
                {
                    Map<String, String> flattedStream = new HashMap<>();

                    flattedStream.put("timestamp", (String) deserializedStream.get("timestamp"));
                    flattedStream.put("city", (String) deserializedStream.get("city"));
                    flattedStream.put("resolver", (String) deserializedStream.get("resolver"));

                    flattedStream.put("domain", (String) answer.get("name"));
                    flattedStream.put("type", (String) answer.get("type"));
                    flattedStream.put("answer", (String) answer.get("data"));

                    collector.collect(flattedStream);
                }

            }
        });
    }

    public static DataStream<Tuple8<String, String, String, String, String, Integer, Integer, Integer>> reduce(DataStream<Map<String, String>> cleanedStream)
    {    // 返回的数据格式:(timestamp, city, domain, resolver, type, total_answers, total_creds, true_answers)
        return cleanedStream.map(
                new MapFunction<Map<String, String>, Tuple8<String, String, String, String, String, Integer, Integer, Integer> >() {
                    @Override
                    public Tuple8<String, String, String, String, String, Integer, Integer, Integer> map(Map<String, String> rawData) throws Exception {

                        String domain = rawData.get("domain");
                        String type = rawData.get("type");
                        String answer = rawData.get("answer");

                        List<String> creds = CredDataSource.getCreds(domain, type);

                        Tuple8<String, String, String, String, String, Integer, Integer, Integer> e =new Tuple8<>(
                                rawData.get("timestamp"),
                                rawData.get("city"),
                                domain,
                                rawData.get("resolver"),
                                type,
                                1,
                                creds.size(),
                                creds.contains(answer) ? 1:0
                        );

                        return e;
                    }
                }
        ).keyBy(
                new KeySelector<Tuple8<String, String, String, String, String, Integer, Integer, Integer>, Tuple5<String, String, String, String, String>>() {

                    @Override
                    public Tuple5<String, String, String, String, String> getKey(Tuple8<String, String, String, String, String, Integer, Integer, Integer> e) throws Exception {
                        return new Tuple5<>(e.f0, e.f1, e.f2, e.f3, e.f4);
                    }
                }
        ).reduce(
                new ReduceFunction<Tuple8<String, String, String, String, String, Integer, Integer, Integer>>() {

                    @Override
                    public Tuple8<String, String, String, String, String, Integer, Integer, Integer> reduce(Tuple8<String, String, String, String, String, Integer, Integer, Integer> e1, Tuple8<String, String, String, String, String, Integer, Integer, Integer> e2) throws Exception {
                        return new Tuple8<>(
                                e1.f0,
                                e1.f1,
                                e1.f2,
                                e1.f3,
                                e1.f4,
                                e1.f5 + e2.f5,
                                e1.f6,
                                e1.f7 + e2.f7
                        );
                    }
                }
        );
    }
}
