package com.example.elasticsearch.alarm;

import com.example.elasticsearch.utils.BaseTest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class GetHistoryAlarmById extends BaseTest {
    public static void main(String[] args) throws Exception {
        init("172.16.3.198:9200");
        try {
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("instId", "973a0f8fc801deca4611316ad44abcaf"));
            SearchRequest request = new SearchRequest("alarm*");
            SearchSourceBuilder source = new SearchSourceBuilder().size(1).trackTotalHits(true);
            source.query(queryBuilder);
            request.source(source);

            SearchResponse response = esSearch.searchByRequest(request);
            String result = null;
            if (response != null && response.getHits() != null) {
                SearchHits hits = response.getHits();
                if (hits.getHits().length > 0) {
                    result = hits.getHits()[0].getSourceAsString();
                }
            }

            System.out.println(result);

            // 关闭client
            factory.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
