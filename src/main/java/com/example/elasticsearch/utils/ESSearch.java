package com.example.elasticsearch.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

/**
 * ES查询相关实现类
 */
public class ESSearch {

    private ElasticsearchFactory factory;

    public ESSearch(ElasticsearchFactory factory) {
        this.factory = factory;
    }

    public String[] loadAvailIndices() {
        String[] results = {};
        try {
            GetIndexRequest request =
                new GetIndexRequest("*").indicesOptions(IndicesOptions.fromOptions(false, false, true, false));
            GetIndexResponse response = factory.getClient().indices().get(request, RequestOptions.DEFAULT);
            results = response.getIndices();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> loadFieldMappingByIndice(String index) {
        Map<String, String> result = new HashMap<>();
        try {
            GetFieldMappingsRequest request = new GetFieldMappingsRequest().indices(index).fields("*");
            GetFieldMappingsResponse response =
                factory.getClient().indices().getFieldMapping(request, RequestOptions.DEFAULT);
            response.mappings().forEach((k, v) -> {
                v.forEach((field, data) -> {
                    Map<String, Object> sources = data.sourceAsMap();
                    sources.values().forEach(type -> {
                        result.put(field, ((Map<String, Object>) type).get("type").toString());
                    });
                    ;
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean exitsIndices(String index) {
        boolean result = false;
        GetIndexRequest request = new GetIndexRequest(index);
        try {
            result = factory.getClient().indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public SearchResponse searchByRequest(SearchRequest request) {
        SearchResponse response = null;
        try {
            response = factory.getClient().search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public SearchResponse searchByScrollRequest(SearchScrollRequest request) {
        SearchResponse response = null;
        try {
            response = factory.getClient().scroll(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public GetIndexResponse getIndice(String... indices) {
        try {
            GetIndexRequest request = new GetIndexRequest(indices);
            return factory.getClient().indices().get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
