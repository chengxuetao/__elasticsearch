package com.example.elasticsearch.metric;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class MeticAvgValue {
    public static void main(String[] args) throws Exception {
        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        long start = 1514739661000L;
        long end = 1565857686000L;

        // "e63c50bd7142ba93a15894bbd77fb177","594d8f3cce467e40966778d0a5e382e9","a4696c726cd0605b89e10c64554aaca7","","",""
        String[] instIds = new String[] { "e63c50bd7142ba93a15894bbd77fb177", "594d8f3cce467e40966778d0a5e382e9",
            "a4696c726cd0605b89e10c64554aaca7" };

        double avgValue = 0;
        String aggName = "metricAvg";
        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("taskTime").gte(start).lte(end));
        TermQueryBuilder metricBuilder = QueryBuilders.termQuery("metric", "CPURate");
        queryBuilder.must(metricBuilder);
        TermsQueryBuilder instBuilder = QueryBuilders.termsQuery("instId", instIds);
        queryBuilder.must(instBuilder);
        AvgAggregationBuilder metricAvgAgg = AggregationBuilders.avg(aggName).field("val");
        SearchRequestBuilder builder = client.prepareSearch("metrics-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryBuilder).setSize(0).addAggregation(metricAvgAgg);
        SearchResponse response = builder.get();
        System.out.println(response);
        if (response.getAggregations() != null) {
            InternalAvg avg = response.getAggregations().get(aggName);
            avgValue = avg.getValue();
        }
        System.out.println(avgValue);
        client.close();
    }
}
