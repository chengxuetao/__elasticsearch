package com.example.elasticsearch;

import com.example.elasticsearch.utils.ESOperation;
import com.example.elasticsearch.utils.ESSearch;
import com.example.elasticsearch.utils.ElasticsearchFactory;
import com.example.elasticsearch.utils.ElasticsearchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.TimeUnit;

public class WriteData {

    protected static ESSearch esSearch;

    protected static ESOperation esOperation;

    protected static ElasticsearchService elasticsearchService;

    protected static ElasticsearchFactory factory;

    protected static ObjectMapper mapper = new ObjectMapper();

    public static void init(String host) throws Exception {
        if (host == null || host.length() == 0) {
            host = "172.16.3.198:9200";
        }
        factory = new ElasticsearchFactory(host, true, "elastic", "detadatapoint");
        esOperation = new ESOperation(factory);
        esSearch = new ESSearch(factory);
        elasticsearchService = new ElasticsearchService(factory, esOperation, esSearch);
    }

    public static void main(String[] args) throws Exception {
        init(null);

        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("key1", "value1");
        objectNode.put("key2", "value2");
        objectNode.put("key3", "value3");

        esOperation.save(objectNode.toString(), "index_name");

        TimeUnit.SECONDS.sleep(10);
        factory.destroy();
    }

}
