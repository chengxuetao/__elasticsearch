package com.example.elasticsearch;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.example.elasticsearch.utils.BaseTest;

public class GetLastedMetrics extends BaseTest {

    public static void main(String[] args) throws Exception {
        init(null);

        SearchRequest request = new SearchRequest("metrics-lasted");
        SearchSourceBuilder source = new SearchSourceBuilder().size(10000).trackTotalHits(true);
        request.source(source);
        request.scroll(TimeValue.timeValueMinutes(1));
        SearchResponse response = esSearch.searchByRequest(request);
        while (true) {
            for (SearchHit hit : response.getHits().getHits()) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
            String scorllId = response.getScrollId();
            if (scorllId != null && scorllId.length() > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scorllId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(1));
                response = esSearch.searchByScrollRequest(scrollRequest);
            } else {
                break;
            }
            if (response.getHits().getHits().length == 0) {
                System.out.println("search end...");
                break;
            }
        }
        System.out.println("main end...");
        factory.destroy();
    }

}
