package com.example.elasticsearch.esaction;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Test1 {

    private RestClient restClient;

    public static void main(String[] args) throws Exception {
        RestClientBuilder builder = RestClient.builder(new HttpHost("172.16.3.198", 9200));
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "detadatapoint"));
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestClient restClient = builder.build();

        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().fields("metricName").indices("metrics-lasted", "8a7a808f736ab99001736b9ecbbd001a_filterstep_1");
        request.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);

        FieldCapabilitiesResponse fieldCapabilitiesResponse = restHighLevelClient.fieldCaps(request, RequestOptions.DEFAULT);

        String[] indices = fieldCapabilitiesResponse.getIndices();
        for (String index : indices) {
            System.out.println("index==========>" + index);
        }

        Map<String, Map<String, FieldCapabilities>> map = fieldCapabilitiesResponse.get();
        map.forEach((k, v) -> {
            System.out.println(k + "=========" + v);
        });

        Map<String, FieldCapabilities> metricName = fieldCapabilitiesResponse.getField("metricName");
        FieldCapabilities text = metricName.get("keyword");
        boolean searchable = text.isSearchable();
        System.out.println(searchable);
        boolean aggregatable = text.isAggregatable();
        System.out.println(aggregatable);
        String[] indices1 = text.indices();
        System.out.println(Arrays.stream(indices).collect(Collectors.joining()));
        String[] strings = text.nonSearchableIndices();
        System.out.println(Arrays.stream(strings).collect(Collectors.joining()));
        String[] strings1 = text.nonAggregatableIndices();
        System.out.println(Arrays.stream(strings1).collect(Collectors.joining()));


        restClient.close();
        restHighLevelClient.close();
    }

}
