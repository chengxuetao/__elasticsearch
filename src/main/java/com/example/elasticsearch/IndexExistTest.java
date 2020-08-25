package com.example.elasticsearch;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;

import com.google.common.io.ByteStreams;

public class IndexExistTest {

    private static final String SPLITER = ",";

    private static final String COLON = "[:]";

    private static RestHighLevelClient client;

    private static JsonMapper mapper = new JsonMapper();

    public static void main(String[] args) throws Exception {
        buildClient(null);

//        GetIndexRequest request = new GetIndexRequest("metrics-lasted1");
//
//        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
//
//        System.out.println(exists);
//
//        GetIndexRequest r =
//                new GetIndexRequest("*").indicesOptions(IndicesOptions.fromOptions(false, false, true, false));
//        GetIndexResponse response = client.indices().get(r, RequestOptions.DEFAULT);
//        String[] results = response.getIndices();
//
//        Arrays.stream(results).forEach((result) -> {
//            System.out.println(result);
//        });
//        Map<String, List<AliasMetaData>> aliases = response.getAliases();
//        aliases.forEach((key, value) -> {
//            System.out.println(key + "---->" + value);
//        });
//        System.out.println("index size=" + results.length + ", aliases size=" + aliases.size());

        GetIndexRequest request1 = new GetIndexRequest("metrics-*");
        request1.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, false, true, true, false));
        //.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN)
        //.indicesOptions(IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED)
        //.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
        //.includeDefaults(true) 是否输出包含默认配置(settings)，默认为false
        //.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
        GetIndexResponse indexResponse = client.indices().get(request1, RequestOptions.DEFAULT);
        String[] indices = indexResponse.getIndices();
        // System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>\r\n" + Arrays.stream(indices).collect(Collectors.joining("\r\n")));

//        Request req = new Request("GET", "/_cat/indices?index=metrics*&s=status,index&h=index,status&bytes=b&format=json");
//        Response response = client.getLowLevelClient().performRequest(req);
//        String s = new String(ByteStreams.toByteArray(response.getEntity().getContent()));

        Request request = new Request("GET", "/_cat/indices?index=metrics*&s=status,index&h=index,status&bytes=b&format=json");


        Response response = client.getLowLevelClient().performRequest(request);
        String s = new String(ByteStreams.toByteArray(response.getEntity().getContent()));


        JsonNode jsonNode = mapper.readTree(s);
        jsonNode.forEach((node) -> {
            // System.out.println(node);
            String status = node.get("status").asText();
            String name = node.get("index").asText();
            if ("open".equals(status)) {
                System.out.println(name);
            }
        });


//        Request request = new Request("GET", "metrics-*");
//        Response response = client.getLowLevelClient().performRequest(request);
//        System.out.println(new String(ByteStreams.toByteArray(response.getEntity().getContent())));
        client.close();
    }

    public static String loadAllIndicesStats(String endPoint) {
        return commonLowLevelGet(endPoint);
    }

    public static String commonLowLevelGet(String endPoint) {
        Request request = new Request("GET", endPoint);
        try {
            Response response = client.getLowLevelClient().performRequest(request);
            return new String(ByteStreams.toByteArray(response.getEntity().getContent()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static HttpHost[] joinHosts(String host) {
        if (host == null || "".equals(host)) {
            host = "120.27.34.14:15920";
        }
        String[] splits = host.split(SPLITER);
        HttpHost[] hosts = new HttpHost[splits.length];
        for (int i = 0; i < hosts.length; i++) {
            String[] hostPorts = splits[i].split(COLON);
            hosts[i] = new HttpHost(hostPorts[0], Integer.parseInt(hostPorts[1]));
        }
        return hosts;
    }

    /**
     * 构建ElasticsearchClient
     */
    private static void buildClient(String host) {
        HttpHost[] hosts = joinHosts(host);
        if (hosts == null) {
            System.out.println("Init elasticsearch rest high level client error.");
            System.exit(1);
        }
        boolean isAuth = true;
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isAuth) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials("elastic", "Deta2020"));
            builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {

                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }
        client = new RestHighLevelClient(builder);
    }
}
