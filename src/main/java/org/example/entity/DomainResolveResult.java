package org.example.entity;

public class DomainResolveResult {
    public String timestamp;
    public String city;
    public String resolver;
    public String domain;
    public String type;
    public String answer;
    public Integer ttl;

    public DomainResolveResult(String timestamp, String city, String resolver, String domain, String type, String answer, Integer ttl) {
        this.timestamp = timestamp;
        this.city = city;
        this.resolver = resolver;
        this.domain = domain;
        this.type = type;
        this.answer = answer;
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "DomainResolveResult [timestamp=" + timestamp + ", city=" + city  + ", resolver=" + resolver  + ", domain=" + domain +
                 ", type=" + type + ", answer=" + answer + ", ttl=" + ttl + "]";
    }
}
