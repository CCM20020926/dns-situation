package org.example.entity;

public class DomainSituation {
    public String timestamp;
    public String city;
    public String resolver;
    public String domain;
    public String type;
    public Integer total_answers;
    public Integer total_creds;
    public Integer true_answers;

    public DomainSituation(String timestamp, String city, String resolver, String domain, String type, Integer total_answers, Integer total_creds, Integer true_answers) {
        this.timestamp = timestamp;
        this.city = city;
        this.resolver = resolver;
        this.domain = domain;
        this.type = type;
        this.total_answers = total_answers;
        this.total_creds = total_creds;
        this.true_answers = true_answers;
    }

    @Override
    public String toString() {
        return "DomainSituation [timestamp=" + timestamp + ", city=" + city + ", resolver="
                + resolver + ", domain=" + domain + ", type=" + type + ", total_answers=" + total_answers + ", total_creds=" + total_creds + ", true_answers=" + true_answers + "]";
    }
}
