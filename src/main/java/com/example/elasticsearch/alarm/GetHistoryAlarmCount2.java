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
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class GetHistoryAlarmCount2 {
    public static void main(String[] args) throws Exception {
        try {
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
            // 创建client
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.108"), 9300));
            long startMs = 1230354976957L;
            long endMs = 1546396144961L;

            String instAgg = "instAgg";
            Map<String, Long> map = new HashMap<>();
            String instId = "2fb8ac1dfc0d41f1b6f739c921fc3bbe,ba4b9b33f6818b653b2a289662ce06ec";
            String alarmType = "health/sleepTime";
            BoolQueryBuilder queryBuilder =
                QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("createdTime").gte(startMs).lte(endMs));
            TermsQueryBuilder instQuery = QueryBuilders.termsQuery("instId", instId.split(","));
            queryBuilder.must(instQuery);
            TermQueryBuilder alarmTypeQuery = QueryBuilders.termQuery("alarmType", alarmType);
            queryBuilder.must(alarmTypeQuery);
            AggregationBuilder instAggregation = AggregationBuilders.terms(instAgg).field("instId");
            SearchRequestBuilder builder =
                client.prepareSearch("alarm-history-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(queryBuilder).setSize(0).addAggregation(instAggregation);

            SearchResponse response = builder.get();
            SearchHits hits = response.getHits();
            System.err.println(hits.getTotalHits());
            for (SearchHit sh : hits.getHits()) {
                System.err.println(sh.getSourceAsString());
            }
            if (response.getAggregations() != null) {
                Terms term = response.getAggregations().get(instAgg);
                for (Terms.Bucket entry : term.getBuckets()) {
                    map.put(entry.getKeyAsString(), entry.getDocCount());
                }
            }

            System.out.println(map);

            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
