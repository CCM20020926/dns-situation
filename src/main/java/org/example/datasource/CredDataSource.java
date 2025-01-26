package org.example.datasource;

import java.util.List;
import java.util.Map;

public class CredDataSource {
    //!!! 注意：在dig记录中探测域名最后都以.结尾
    private static final Map<String, Map<String, List<String>>> creds = Map.of(
            "baidu.com.", Map.of("A", List.of("39.156.66.10", "110.242.68.66"),
                    "NS", List.of("ns7.baidu.com.", "ns4.baidu.com.", "dns.baidu.com.",
                            "ns2.baidu.com.", "ns3.baidu.com.")),
            "sina.com.", Map.of("A", List.of("64.71.151.11"),
                    "NS", List.of("ns1.sina.com.", "ns2.sina.com.cn.", "ns2.sina.com.",
                            "ns3.sina.com.", "ns1.sina.com.cn.", "ns4.sina.com.",
                            "ns4.sina.com.cn.", "ns3.sina.com.cn."))
    );

    public static List<String> getCreds(String domain, String type) {
        if (creds.containsKey(domain) && creds.get(domain).containsKey(type)) {
            return creds.get(domain).get(type);
        }
        return List.of();
    }
}
