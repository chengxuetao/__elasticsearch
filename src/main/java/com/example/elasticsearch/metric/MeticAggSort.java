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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class MeticAggSort {
    public static void main(String[] args) throws Exception {
        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        long start = 1L;
        long end = 1566204971000L;

        JSONArray result = new JSONArray();
        String aggName = "agg";
        String subAggName = "metricAvg";
        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("taskTime").gte(start).lte(end));
        TermQueryBuilder metricBuilder = QueryBuilders.termQuery("metric", "PingAvailStatus");
        queryBuilder.must(metricBuilder);
        BucketOrder order = InternalOrder.aggregation(subAggName, false);
        AggregationBuilder aggregation = AggregationBuilders.terms(aggName).field("instId").order(order).size(10);
        AvgAggregationBuilder metricAvgAgg = AggregationBuilders.avg(subAggName).field("val");
        AggregationBuilder topAgg = AggregationBuilders.topHits("topDetail").sort("taskTime", SortOrder.ASC).size(1);
        aggregation.subAggregation(topAgg);
        aggregation.subAggregation(metricAvgAgg);
        SearchRequestBuilder builder = client.prepareSearch("metrics-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryBuilder).setSize(0).addAggregation(aggregation);
        SearchResponse response = builder.get();
        System.out.println(response);
        System.out.println("");
        if (response.getAggregations() != null) {
            Terms term = response.getAggregations().get(aggName);
            for (Terms.Bucket entry : term.getBuckets()) {
                String key = entry.getKeyAsString();
                InternalAvg avg = entry.getAggregations().get(subAggName);
                double value = avg.getValue();
                JSONObject item = new JSONObject();
                item.put(key, value);
                result.add(item);
            }
        }
        System.out.println(result);
        client.close();
    }
}
