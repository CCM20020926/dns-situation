package org.example.datasource;

import org.example.config.JdbcSourceConfig;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CredDataSource {

    private static final Map<String, String> typeMapper = Map.of(
      "A", "dig_a", "AAAA", "dig_aaaa", "NS", "dig_ns", "MX", "dig_mx", "CNAME", "dig_cname"
    );

    private static final JdbcTemplate jdbcTemplate;

    static {
        jdbcTemplate = JdbcSourceConfig.getJdbcTemplate();
    }

    public static List<String> getCreds(String domain, String type) {

        if(!typeMapper.containsKey(type))
        {
            throw new RuntimeException("Invalid type: " + type);
        }
        String sql = String.format("SELECT %s FROM dig_data WHERE domain_name=?", typeMapper.get(type));
        List<String> creds = new ArrayList<>();
        List<String> rawRecords = jdbcTemplate.queryForList(sql, String.class, domain);
        for(String rawRecord : rawRecords)
        {
            String [] records = rawRecord.split(",");
            creds.addAll(Arrays.asList(records));
        }

        return creds;
    }
}
