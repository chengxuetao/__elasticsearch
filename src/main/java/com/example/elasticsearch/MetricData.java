package com.example.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetricData {

    private String metric;

    private long totalSize;

    private List<Map<String, Object>> data = new ArrayList<>();

    public void appendData(Map<String, Object> data) {
        this.data.add(data);
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }

    @Override
    public String toString() {
        return "[" + this.metric + ":" + this.data + "]\r\n";
    }

}
