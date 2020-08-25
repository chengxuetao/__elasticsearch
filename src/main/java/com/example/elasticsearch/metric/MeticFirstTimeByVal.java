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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class MeticFirstTimeByVal {
    public static void main(String[] args) throws Exception {
        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        long startMs = 1L;
        long endMs = 1566204971000L;

        JSONArray result = new JSONArray();
        String aggName = "agg";
        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("taskTime").gte(startMs).lte(endMs));
        TermQueryBuilder metricBuilder = QueryBuilders.termQuery("metric", "PingAvailStatus");
        queryBuilder.must(metricBuilder);
        TermsQueryBuilder valQuery = QueryBuilders.termsQuery("stringVal", "ok", "error");
        queryBuilder.must(valQuery);

        AggregationBuilder aggregation = AggregationBuilders.terms(aggName).field("instId").size(1000);
        AggregationBuilder topAgg = AggregationBuilders.topHits("topDetail").sort("taskTime", SortOrder.ASC).size(1);
        aggregation.subAggregation(topAgg);
        SearchRequestBuilder builder = client.prepareSearch("metrics-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryBuilder).setSize(0).addAggregation(aggregation);
        SearchResponse response = builder.get();
        // System.out.println(response);
        if (response.getAggregations() != null) {
            Terms term = response.getAggregations().get(aggName);
            for (Terms.Bucket entry : term.getBuckets()) {
                TopHits topHits = entry.getAggregations().get("topDetail");
                if (topHits != null && topHits.getHits() != null) {
                    SearchHit[] hits = topHits.getHits().getHits();
                    if (hits != null && hits.length > 0) {
                        String detail = hits[0].getSourceAsString();
                        result.add(JSONObject.parseObject(detail));
                    }
                }
            }
        }
        System.out.println(result);
        client.close();
    }
}
