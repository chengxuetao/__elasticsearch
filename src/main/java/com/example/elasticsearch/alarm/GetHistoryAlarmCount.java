package com.example.elasticsearch.alarm;

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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class GetHistoryAlarmCount {
    public static void main(String[] args) throws Exception {
        try {
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
            // 创建client
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));
            long startMs = 1230354976957L;
            long endMs = 1545715015621L;

            String levelAggregationName = "levelAgg";
            String recoveredTypeAggregationName = "recoveredTypeAgg";
            String resFullTypeAggregationName = "resFullTypeAgg";
            String locationAggregationName = "locationAgg";
            String orgAggregationName = "orgAgg";

            Map<String, Object> map = new HashMap<>();
            int page = 1;
            int pageSize = 20;

            BoolQueryBuilder queryBuilder =
                QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("createdTime").gte(startMs).lte(endMs));

            AggregationBuilder levelAggregation =
                AggregationBuilders.terms(levelAggregationName).field("level").size(pageSize);
            AggregationBuilder recoveredTypeAggregation =
                AggregationBuilders.terms(recoveredTypeAggregationName).field("recoveredType").size(pageSize);
            AggregationBuilder resFullTypeAggregation =
                AggregationBuilders.terms(resFullTypeAggregationName).field("resFullType").size(pageSize);
            AggregationBuilder localhostAggregation =
                AggregationBuilders.terms(locationAggregationName).field("location").size(pageSize);
            AggregationBuilder orgAggregation =
                AggregationBuilders.terms(orgAggregationName).field("org").size(pageSize);

            SearchRequestBuilder builder = client.prepareSearch("alarm-history-*")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(queryBuilder).setSize(pageSize)
                .setFrom((page - 1) * pageSize).addAggregation(levelAggregation)
                .addAggregation(recoveredTypeAggregation).addAggregation(resFullTypeAggregation)
                .addAggregation(localhostAggregation).addAggregation(orgAggregation);

            SearchResponse response = builder.get();
            SearchHits hits = response.getHits();
            System.err.println(hits.getTotalHits());
            for (SearchHit sh : hits.getHits()) {
                System.err.println(sh.getSourceAsString());
            }

            if (response.getAggregations() != null) {
                Map<String, Long> levelMap = new HashMap<>();
                Terms levelTerm = response.getAggregations().get(levelAggregationName);
                for (Terms.Bucket entry : levelTerm.getBuckets()) {
                    levelMap.put(entry.getKeyAsString(), entry.getDocCount());
                }
                map.put("level", levelMap);

                Map<String, Long> recoveredTypeMap = new HashMap<>();
                Terms recoveredTypeTerm = response.getAggregations().get(recoveredTypeAggregationName);
                for (Terms.Bucket entry : recoveredTypeTerm.getBuckets()) {
                    recoveredTypeMap.put(entry.getKeyAsString(), entry.getDocCount());
                }
                map.put("recoveredType", recoveredTypeMap);

                Map<String, Long> resFullTypeMap = new HashMap<>();
                Terms resFullTypeTerm = response.getAggregations().get(resFullTypeAggregationName);
                for (Terms.Bucket entry : resFullTypeTerm.getBuckets()) {
                    resFullTypeMap.put(entry.getKeyAsString(), entry.getDocCount());
                }
                map.put("resFullType", resFullTypeMap);

                Map<String, Long> locationMap = new HashMap<>();
                Terms locationTerm = response.getAggregations().get(locationAggregationName);
                for (Terms.Bucket entry : locationTerm.getBuckets()) {
                    locationMap.put(entry.getKeyAsString(), entry.getDocCount());
                }
                map.put("location", locationMap);

                Map<String, Long> orgMap = new HashMap<>();
                Terms orgTerm = response.getAggregations().get(orgAggregationName);
                for (Terms.Bucket entry : orgTerm.getBuckets()) {
                    orgMap.put(entry.getKeyAsString(), entry.getDocCount());
                }
                map.put("org", orgMap);
            }

            System.out.println(map);

            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
