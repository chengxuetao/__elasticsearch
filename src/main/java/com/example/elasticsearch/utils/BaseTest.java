package com.example.elasticsearch.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BaseTest {

    protected static final int BATCH_COUNT = 100;

    protected static final String SPLITER = ",";

    protected static final String COLON = "[:]";

    protected static ESSearch esSearch;

    protected static ESOperation esOperation;

    protected static ElasticsearchService elasticsearchService;

    protected static ElasticsearchFactory factory;

    protected ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        init("172.16.3.197:9200");
        String destIndexName = "model_test_001";
        SearchRequest request = new SearchRequest("8a7a808770e7febd0170e8026329000c*");
        SearchSourceBuilder source = new SearchSourceBuilder().size(10000).trackTotalHits(true);
        request.source(source);
        request.scroll(TimeValue.timeValueMinutes(1));
        SearchResponse response = esSearch.searchByRequest(request);
        int count = 0;
        while (true) {
            List<Object> dataList = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                count++;
                Map<String, Object> dataMap = hit.getSourceAsMap();
                dataList.add(dataMap);
                if (count == BATCH_COUNT) {
                    esOperation.save(dataList, destIndexName);
                    dataList.clear();
                    count = 0;
                }
            }
            if (!dataList.isEmpty()) {
                esOperation.save(dataList, destIndexName);
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
                break;
            }
        }

        TimeUnit.MINUTES.sleep(1);
        factory.destroy();
    }

    public static void init(String host) throws Exception {
        if (host == null || host.length() == 0) {
            host = "172.16.3.198:9200";
        }
        factory = new ElasticsearchFactory(host, true, "elastic", "detadatapoint");
        esOperation = new ESOperation(factory);
        esSearch = new ESSearch(factory);
        elasticsearchService = new ElasticsearchService(factory, esOperation, esSearch);
    }
}
