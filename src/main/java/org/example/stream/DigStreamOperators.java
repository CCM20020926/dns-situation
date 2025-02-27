package org.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.example.datasource.CredDataSource;
import org.example.entity.DomainResolveResult;
import org.example.entity.DomainSituation;

import java.util.List;
import java.util.Map;

public class DigStreamOperators {

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
                String rawDomainName = (String) answer.get("name"); // 记录中的原始域名,以.结尾，如baidu.com.
                String domain = rawDomainName.substring(0, rawDomainName.length()-1);   // 去掉最后一个字符. 以匹配可信数据库

                collector.collect(
                        new DomainResolveResult(
                                (String) payload.get("timestamp"),
                                (String) payload.get("city"),
                                (String) payload.get("resolver"),
                                domain,
                                (String) answer.get("type"),
                                (String) answer.get("data"),
                                (Integer) answer.get("ttl")
                        )
                );
            }
        }
    }

    public static class digMap implements MapFunction<DomainResolveResult, DomainSituation> {
        @Override
        public DomainSituation map(DomainResolveResult domainResolveResult) throws Exception {
            List<String> creds = CredDataSource.getCreds(domainResolveResult.domain, domainResolveResult.type);
            return new DomainSituation(
                    domainResolveResult.timestamp,
                    domainResolveResult.city,
                    domainResolveResult.resolver,
                    domainResolveResult.domain,
                    domainResolveResult.type,
                    1,
                    creds.size(),
                    creds.contains(domainResolveResult.answer)? 1:0

            );
        }
    }

    public static class digReduce implements ReduceFunction<DomainSituation> {

        @Override
        public DomainSituation reduce(DomainSituation t1, DomainSituation t2) throws Exception {
            return new DomainSituation(
                    t1.timestamp,
                    t1.city,
                    t1.resolver,
                    t1.domain,
                    t1.type,
                    t1.total_answers + t2.total_answers,
                    t1.total_creds,
                    t1.true_answers + t2.true_answers
            );
        }
    }

}
