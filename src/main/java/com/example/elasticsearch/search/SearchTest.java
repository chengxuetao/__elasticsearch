package com.example.elasticsearch.search;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSON;

public class SearchTest {

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();// cluster.name在elasticsearch.yml中配置
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.197"), 9300));

        String aggregationName = "agg";
        String condition = "59";

        int page = 1;
        int pageSize = 20;

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        QueryStringQueryBuilder conditionBuilder = QueryBuilders.queryStringQuery(condition);
        boolBuilder.should(conditionBuilder);

        // String[] field = "mainIp,connections,properties".split(",");
        // for (String f : field) {
        // PrefixQueryBuilder conditionPrefixQuery = QueryBuilders.prefixQuery(f, condition);
        // boolBuilder.should(conditionPrefixQuery);
        // }

        queryBuilder.must(boolBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:#fff;background-color:#ffac38;\">");
        highlightBuilder.postTags("</span>");

        AggregationBuilder aggregation = AggregationBuilders.terms(aggregationName).field("indexType");

        SearchRequestBuilder builder = client.prepareSearch("search", "alarm-history-*", "fluentd-*")
            .highlighter(highlightBuilder).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(queryBuilder)
            .setSize(pageSize).setFrom((page - 1) * pageSize).addAggregation(aggregation);

        SearchResponse response = builder.get();
        System.out.println(response);
        SearchHits hits = response.getHits();

        for (SearchHit sh : hits.getHits()) {
            Map<String, Object> fields = sh.getSourceAsMap();
            Map<String, HighlightField> highlight = sh.getHighlightFields();
            Map<String, Object> m = new HashMap<>();
            for (Map.Entry<String, HighlightField> entry : highlight.entrySet()) {
                HighlightField hf = entry.getValue();
                String key = entry.getKey();
                Text[] text = hf.fragments();
                List<String> string = new ArrayList<>();
                for (Text t : text) {
                    string.add(t.string());
                }
                m.put(key, string);
            }

            m.remove("customerId");
            fields.put("highlight", JSON.toJSON(m));
        }

        client.close();
    }

}
