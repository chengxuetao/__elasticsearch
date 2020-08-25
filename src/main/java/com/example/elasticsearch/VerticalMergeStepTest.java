package com.example.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.example.elasticsearch.utils.BaseTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class VerticalMergeStepTest extends BaseTest {

    private String srcIndexName;

    private String destIndexName;

    private Map<String, Object> columnMap = new HashMap<>();

    private ArrayNode tableMeta;

    private ObjectNode config;

    public static void main(String[] args) throws Exception {
        try {
            init(null);
            VerticalMergeStepTest test = new VerticalMergeStepTest();
            test.prepare();
            test.execute();

            System.out.println("execute end...");
            TimeUnit.SECONDS.sleep(5);
            System.out.println("main end...");
        } finally {
            if (factory != null)
                factory.destroy();
        }
    }

    public void prepare() {
        srcIndexName = "8a7a808770e7febd0170e8026329000c";
        destIndexName = "8a7a808770e812880170e8149f9e000d_verticalmergestep_1";
        // 初始化tableMeta
        tableMeta = mapper.createArrayNode();
        ObjectNode idNode = mapper.createObjectNode();
        idNode.put("name", "id");
        idNode.put("type", "keyword");
        idNode.put("comment", "id");
        tableMeta.add(idNode);

        ObjectNode nameNode = mapper.createObjectNode();
        nameNode.put("name", "name");
        nameNode.put("type", "keyword");
        nameNode.put("comment", "name");
        tableMeta.add(nameNode);

        ObjectNode ageNode = mapper.createObjectNode();
        ageNode.put("name", "age");
        ageNode.put("type", "long");
        ageNode.put("comment", "age");
        tableMeta.add(ageNode);

        ObjectNode dataTimeNode = mapper.createObjectNode();
        dataTimeNode.put("name", "dataTime");
        dataTimeNode.put("type", "date");
        dataTimeNode.put("comment", "dataTime");
        tableMeta.add(dataTimeNode);

        System.out.println("===================================tableMeta=============================");
        System.out.println(tableMeta.toString());
        System.out.println("=========================================================================");

        tableMeta.forEach((field) -> {
            columnMap.put(field.get("name").asText(), null);
        });

        // 初始化config
        // {"mergeIndices":[{"name":"234sdfsdfsdf","fieldMapping":[{"srcField":"cf-f-14-w","srcField":"cf-f-14-q"}]]}
        config = mapper.createObjectNode();
        ArrayNode mergeIndices = mapper.createArrayNode();
        ObjectNode mergeIndex = mapper.createObjectNode();
        mergeIndex.put("name", "8a7a808870ec3cb80170ecda69cd0011");
        ArrayNode fieldMappings = mapper.createArrayNode();
        ObjectNode fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "id");
        fieldMapping.put("destField", "student_id");
        fieldMappings.add(fieldMapping);
        fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "name");
        fieldMapping.put("destField", "address");
        fieldMappings.add(fieldMapping);
        fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "age");
        fieldMapping.put("destField", "age");
        fieldMappings.add(fieldMapping);
        fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "dataTime");
        fieldMapping.put("destField", "dataTime");
        fieldMappings.add(fieldMapping);
        mergeIndex.replace("fieldMapping", fieldMappings);
        mergeIndices.add(mergeIndex);

        mergeIndex = mapper.createObjectNode();
        mergeIndex.put("name", "data_model_student");
        fieldMappings = mapper.createArrayNode();
        fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "id");
        fieldMapping.put("destField", "student_id_1");
        fieldMappings.add(fieldMapping);
        fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "name");
        fieldMapping.put("destField", "district");
        fieldMappings.add(fieldMapping);
        fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "age");
        fieldMapping.put("destField", "age_1");
        fieldMappings.add(fieldMapping);
        fieldMapping = mapper.createObjectNode();
        fieldMapping.put("srcField", "dataTime");
        fieldMapping.put("destField", "dataTime_1");
        fieldMappings.add(fieldMapping);
        mergeIndex.replace("fieldMapping", fieldMappings);
        mergeIndices.add(mergeIndex);

        config.replace("mergeIndices", mergeIndices);
        System.out.println("===================================config================================");
        System.out.println(config.toString());
        System.out.println("=========================================================================");
        if (esOperation.indexExists(destIndexName)) {
            esOperation.deleteIndices(destIndexName);
        }
        createDestIndex();
    }

    public void execute() {
        this.copySrcIndexData();
        JsonNode mergeIndices = this.config.get("mergeIndices");
        for (JsonNode mergeIndex : mergeIndices) {
            String indexName = mergeIndex.get("name").asText();
            JsonNode fieldMapping = mergeIndex.get("fieldMapping");
            Map<String, String> fieldMappingMap = new HashMap<>();
            fieldMapping.forEach((jsonNode) -> {
                String srcField = jsonNode.get("srcField").asText();
                String destField = jsonNode.get("destField").asText();
                fieldMappingMap.put(destField, srcField);
            });
            SearchRequest request = new SearchRequest(indexName + "*");
            SearchResponse response = esSearch.searchByRequest(request);
            SearchHit[] hits = response.getHits().getHits();

            int count = 0;
            List<Object> dataList = new ArrayList<>();
            for (SearchHit hit : hits) {
                count++;
                Map<String, Object> sourceDataMap = hit.getSourceAsMap();
                Map<String, Object> dataMap = new HashMap<>(columnMap);
                sourceDataMap.forEach((key, value) -> {
                    String srcField = fieldMappingMap.get(key);
                    if (srcField != null && dataMap.containsKey(srcField)) {
                        dataMap.put(srcField, value);
                    }
                });
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
        }
    }

    private void copySrcIndexData() {
        SearchRequest request = new SearchRequest(srcIndexName + "*");
        SearchSourceBuilder source = new SearchSourceBuilder().size(10000).trackTotalHits(true);
        request.source(source);
        request.scroll(TimeValue.timeValueMinutes(1));
        SearchResponse response = esSearch.searchByRequest(request);
        int count = 0;
        while (true) {
            List<Object> dataList = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                count++;
                Map<String, Object> sourceDataMap = hit.getSourceAsMap();
                Map<String, Object> dataMap = new HashMap<>(columnMap);
                sourceDataMap.forEach((key, value) -> {
                    if (dataMap.containsKey(key)) {
                        dataMap.put(key, value);
                    }
                });
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
    }

    private void createDestIndex() {
        ObjectNode properties = mapper.createObjectNode();
        tableMeta.forEach((field) -> {
            String name = field.get("name").asText();
            String type = field.get("type").asText();
            ObjectNode typeNode = mapper.createObjectNode();
            typeNode.put("type", type);
            properties.replace(name, typeNode);
        });
        ObjectNode templateNode = mapper.createObjectNode();
        templateNode.put("index_patterns", destIndexName);
        ObjectNode settingsNode = mapper.createObjectNode();
        settingsNode.put("index.number_of_shards", 1);
        settingsNode.put("number_of_replicas", 0);
        templateNode.replace("settings", settingsNode);
        ObjectNode mappings = mapper.createObjectNode();
        mappings.replace("properties", properties);
        templateNode.replace("mappings", mappings);
        esOperation.addOrUpdateTemplate(destIndexName, templateNode.toString());
    }

}
