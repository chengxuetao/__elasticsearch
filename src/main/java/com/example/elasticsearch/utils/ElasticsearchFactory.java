package com.example.elasticsearch.utils;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class ElasticsearchFactory {

    private static final String SPLITER = ",";

    private static final String COLON = "[:]";

    private String addr;

    private String username;

    private String password;

    /**
     * 是否开启安全验证，默认是
     */
    private boolean isAuth;

    private RestHighLevelClient client;

    private BulkProcessor processor;

    public ElasticsearchFactory(String addr, boolean isAuth, String username, String password) throws Exception {
        this.addr = addr;
        this.isAuth = isAuth;
        this.username = username;
        this.password = password;
        init();
    }

    public void destroy() throws Exception {
        if (client != null) {
            client.close();
        }
        if (processor != null) {
            processor.awaitClose(30, TimeUnit.SECONDS);
        }
    }

    private void init() throws Exception {
        buildClient();
        buildBulkProcessor();
    }

    private HttpHost[] joinHosts() {
        String[] splits = addr.split(SPLITER);
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
    private void buildClient() {
        HttpHost[] hosts = joinHosts();
        if (hosts == null) {
            System.err.println("Init elasticsearch rest high level client error.");
            System.exit(1);
        }
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isAuth) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }
        this.client = new RestHighLevelClient(builder);
    }

    /**
     * 构建BulkProcessor
     */
    private void buildBulkProcessor() {
        BulkProcessor.Listener listener = new Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                System.out.println("Executing bulk [" + executionId + "] with " + numberOfActions + " requests.");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.err.println("Failed to execute bulk.");
                failure.printStackTrace();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures())
                    System.out.println("Bulk [" + executionId + "] executed with failures, response = "
                        + response.buildFailureMessage());
                else {
                    System.out.println(
                        "Bulk [" + executionId + "] completed in " + response.getTook().getMillis() + " milliseconds.");
                }
            }
        };
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer =
            (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        this.processor = BulkProcessor.builder(consumer, listener).setBulkActions(500)
            .setBulkSize(new ByteSizeValue(5L, ByteSizeUnit.MB)).setConcurrentRequests(0)
            .setFlushInterval(TimeValue.timeValueSeconds(1L))
            .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3)).build();
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public BulkProcessor getBulkProcessor() {
        return processor;
    }

}
