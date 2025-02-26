package org.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.example.entity.DomainResolveResult;

import java.util.List;
import java.util.Map;

public class StreamOperators {

    // Dig报文清洗算子
    public static class digCleanFlatMap implements FlatMapFunction<String, DomainResolveResult> {
        @Override
        public void flatMap(String s, Collector<DomainResolveResult> collector) throws Exception {
            // 反序列化接收到的载荷数据和DNS响应报文
            Map<String, Object> payload = new ObjectMapper().readValue(s, Map.class);
            Map<String, Object> response = (Map<String, Object>) payload.get("response");
            if(response == null)
            {
                return;
            }

            // 提取响应报文中需要的字段
            List<Map<String, Object>> answers = (List<Map<String, Object>>) response.get("answers");
            if (answers == null) {
                return; // 如果 answers 为 null，则忽略此条记录
            }

            for(Map<String, Object> answer :  answers)
            {
                collector.collect(
                        new DomainResolveResult(
                                (String) payload.get("timestamp"),
                                (String) payload.get("city"),
                                (String) payload.get("resolver"),
                                (String) answer.get("domain"),
                                (String) answer.get("type"),
                                (String) answer.get("answer"),
                                (Integer) answer.get("ttl")
                        )
                );
            }
        }
    }

    public static class digMap implements MapFunction<> {}

}
