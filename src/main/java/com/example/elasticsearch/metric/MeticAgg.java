package com.example.elasticsearch.metric;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class MeticAgg {
    public static void main(String[] args) throws Exception {
        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        long start = 1514739661000L;
        long end = 1565857686000L;
        String aggType = "sum";
        String aggName = "agg";
        String subAggName = "subAgg";
        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("taskTime").gte(start).lte(end));
        TermsQueryBuilder instQuery =
            QueryBuilders.termsQuery("instId", "76ec00010e8378ea8c9c61bceabb4782", "a4696c726cd0605b89e10c64554aaca7");
        // QueryBuilders.termsQuery("instId", "71f84d9979c59c6f91568c775beb7763", "98cd49b77fd30b655a50a90dcf43316");
        queryBuilder.must(instQuery);
        TermQueryBuilder metric = QueryBuilders.termQuery("metric", "CPURate");
        queryBuilder.must(metric);
        AggregationBuilder agg = AggregationBuilders.terms(aggName).field("instId").size(2);

        AggregationBuilder statsAgg = AggregationBuilders.stats(subAggName).field("val");
        agg.subAggregation(statsAgg);

        SearchRequestBuilder builder = client.prepareSearch("metrics-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryBuilder).setSize(0).addAggregation(agg);
        SearchResponse response = builder.get();
        System.out.println(response);
        Map<String, Object> map = new HashMap<>();
        if (response.getAggregations() != null) {
            Terms terms = response.getAggregations().get(aggName);
            for (Bucket bucket : terms.getBuckets()) {
                InternalStats stats = bucket.getAggregations().get(subAggName);
                Object val = null;
                if ("count".equals(aggType)) {
                    val = stats.getCount();
                } else if ("min".equals(aggType)) {
                    val = stats.getMin();
                } else if ("max".equals(aggType)) {
                    val = stats.getMax();
                } else if ("avg".equals(aggType)) {
                    val = stats.getAvg();
                } else if ("sum".equals(aggType)) {
                    val = stats.getSum();
                }
                map.put(bucket.getKeyAsString(), val);
            }
        }
        System.out.println(map);
        client.close();
    }
}
