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

import com.coredata.utils.elasticsearch.vo.FieldValue;
import com.example.elasticsearch.utils.BaseTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class HorizontalMergeStepTest extends BaseTest {

    private String srcIndexName;

    private String destIndexName;

    private Map<String, Object> columnMap = new HashMap<>();

    private ArrayNode tableMeta;

    private ObjectNode config;

    public static void main(String[] args) throws Exception {
        init(null);
        HorizontalMergeStepTest test = new HorizontalMergeStepTest();
        test.prepare();
        test.execute();

        System.out.println("execute end...");
        TimeUnit.SECONDS.sleep(5);
        System.out.println("main end...");
        factory.destroy();
    }

    public void prepare() {
        srcIndexName = "8a7a808770e7febd0170e8026329000c";
        destIndexName = "data_model_student";
        // 初始化tableMeta
        // [{"name":"Sub_Market_group3","type":"keyword","comment":"姓名"},{"name":"xxx_1","type":"double","comment":"新增字段测试"}]
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

        ObjectNode heightNode = mapper.createObjectNode();
        heightNode.put("name", "height");
        heightNode.put("type", "long");
        heightNode.put("comment", "height");
        tableMeta.add(heightNode);

        ObjectNode dataTimeNode = mapper.createObjectNode();
        dataTimeNode.put("name", "dataTime");
        dataTimeNode.put("type", "date");
        dataTimeNode.put("comment", "dataTime");
        tableMeta.add(dataTimeNode);

        // 右连接表字段
        ObjectNode id2Node = mapper.createObjectNode();
        id2Node.put("name", "student_id");
        id2Node.put("type", "keyword");
        id2Node.put("comment", "student_id");
        tableMeta.add(id2Node);

        ObjectNode age2Node = mapper.createObjectNode();
        age2Node.put("name", "age_1");
        age2Node.put("type", "long");
        age2Node.put("comment", "age_1");
        tableMeta.add(age2Node);

        ObjectNode dataTime2Node = mapper.createObjectNode();
        dataTime2Node.put("name", "dataTime_1");
        dataTime2Node.put("type", "date");
        dataTime2Node.put("comment", "dataTime_1");
        tableMeta.add(dataTime2Node);

        ObjectNode addressNode = mapper.createObjectNode();
        addressNode.put("name", "address");
        addressNode.put("type", "keyword");
        addressNode.put("comment", "address");
        tableMeta.add(addressNode);

        // 子连接表
        ObjectNode id3Node = mapper.createObjectNode();
        id3Node.put("name", "student_id_1");
        id3Node.put("type", "keyword");
        id3Node.put("comment", "student_id_1");
        tableMeta.add(id3Node);

        ObjectNode nationNode = mapper.createObjectNode();
        nationNode.put("name", "nation");
        nationNode.put("type", "keyword");
        nationNode.put("comment", "nation");
        tableMeta.add(nationNode);

        ObjectNode provinceNode = mapper.createObjectNode();
        provinceNode.put("name", "province");
        provinceNode.put("type", "keyword");
        provinceNode.put("comment", "province");
        tableMeta.add(provinceNode);

        ObjectNode districtNode = mapper.createObjectNode();
        districtNode.put("name", "district");
        districtNode.put("type", "keyword");
        districtNode.put("comment", "district");
        tableMeta.add(districtNode);

        System.out.println("===================================tableMeta=============================");
        System.out.println(tableMeta.toString());
        System.out.println("=========================================================================");

        tableMeta.forEach((field) -> {
            columnMap.put(field.get("name").asText(), null);
        });

        // 初始化config
        // { "mergeIndices":[{"name":"234sdfsdfsdf","mergeType":"left|right|union|intersection",
        // "relaFields":[{"srcField":"cf-f-14-w","destField":"cf-f-14-q"}],"mergeFields":[{"srcField":"xxx","destField":"xxx_1"}...],"mergeIndices":[...]]}
        config = mapper.createObjectNode();
        ArrayNode mergeIndices = mapper.createArrayNode();
        ObjectNode mergeIndex = mapper.createObjectNode();
        mergeIndex.put("name", "8a7a808870ec3cb80170ecda69cd0011");
        mergeIndex.put("mergeType", "left");

        ArrayNode relaFields = mapper.createArrayNode();
        ObjectNode relaField = mapper.createObjectNode();
        relaField.put("srcField", "id");
        relaField.put("destField", "student_id");
        relaFields.add(relaField);
        mergeIndex.replace("relaFields", relaFields);

        ArrayNode mergeFields = mapper.createArrayNode();
        ObjectNode mergeField = mapper.createObjectNode();
        mergeField.put("srcField", "student_id");
        mergeField.put("destField", "student_id");
        mergeFields.add(mergeField);
        mergeField = mapper.createObjectNode();
        mergeField.put("srcField", "age");
        mergeField.put("destField", "age_1");
        mergeFields.add(mergeField);
        mergeField = mapper.createObjectNode();
        mergeField.put("srcField", "dataTime");
        mergeField.put("destField", "dataTime_1");
        mergeFields.add(mergeField);
        mergeField = mapper.createObjectNode();
        mergeField.put("srcField", "address");
        mergeField.put("destField", "address");
        mergeFields.add(mergeField);

        mergeIndex.replace("mergeFields", mergeFields);

        // 子表
        ArrayNode subMergeIndices = mapper.createArrayNode();
        ObjectNode subMergeIndex = mapper.createObjectNode();
        subMergeIndex.put("name", "8a7a808870ec3cb80170eced766e0012");
        subMergeIndex.put("mergeType", "left");

        ArrayNode subRelaFields = mapper.createArrayNode();
        ObjectNode subRelaField = mapper.createObjectNode();
        subRelaField.put("srcField", "student_id");
        subRelaField.put("destField", "student_id");
        subRelaFields.add(subRelaField);
        subMergeIndex.replace("relaFields", subRelaFields);

        ArrayNode subMergeFields = mapper.createArrayNode();
        ObjectNode subMergeField = mapper.createObjectNode();
        subMergeField.put("srcField", "student_id");
        subMergeField.put("destField", "student_id_1");
        subMergeFields.add(subMergeField);
        subMergeField = mapper.createObjectNode();
        subMergeField.put("srcField", "nation");
        subMergeField.put("destField", "nation");
        subMergeFields.add(subMergeField);
        subMergeField = mapper.createObjectNode();
        subMergeField.put("srcField", "province");
        subMergeField.put("destField", "province");
        subMergeFields.add(subMergeField);
        subMergeField = mapper.createObjectNode();
        subMergeField.put("srcField", "district");
        subMergeField.put("destField", "district");
        subMergeFields.add(subMergeField);

        subMergeIndex.replace("mergeFields", subMergeFields);

        subMergeIndices.add(subMergeIndex);

        mergeIndex.replace("mergeIndices", subMergeIndices);

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
        JsonNode mergeIndices = config.get("mergeIndices");
        SearchRequest request = new SearchRequest(srcIndexName + "*");
        SearchSourceBuilder source = new SearchSourceBuilder().size(10000).trackTotalHits(true);
        request.source(source);
        request.scroll(TimeValue.timeValueMinutes(1));
        SearchResponse response = esSearch.searchByRequest(request);
        int count = 0;
        while (true) {
            List<Object> dataList = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> sourceDataMap = hit.getSourceAsMap();
                Map<String, Object> dataMap = new HashMap<>(columnMap);
                sourceDataMap.forEach((key, value) -> {
                    if (dataMap.containsKey(key)) {
                        dataMap.put(key, value);
                    }
                });
                if (!this.merge(dataMap, mergeIndices)) {
                    continue;
                }
                count++;
                dataList.add(dataMap);
                if (count == 1000) {
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
                System.out.println("search end...");
                break;
            }
        }
    }

    private boolean merge(Map<String, Object> dataMap, JsonNode mergeIndices) {
        boolean result = true;
        for (JsonNode mergeIndex : mergeIndices) {
            String indexName = mergeIndex.get("name").asText();
            // left | right | union | intersection
            String mergeType = mergeIndex.get("mergeType").asText();
            JsonNode relaFields = mergeIndex.get("relaFields");
            JsonNode mergeFields = mergeIndex.get("mergeFields");
            JsonNode subMergeIndices = mergeIndex.get("mergeIndices");
            List<FieldValue> fieldValues = new ArrayList<>();
            relaFields.forEach((jsonNode) -> {
                String srcField = jsonNode.get("srcField").asText();
                String destField = jsonNode.get("destField").asText();
                String value = dataMap.get(srcField) != null ? dataMap.get(srcField).toString() : null;
                if (value != null && !value.isEmpty()) {
                    fieldValues.add(new FieldValue(destField, value));
                }
            });
            String relaData = null;
            // 关联条件完整在进行查询
            if (relaFields.size() == fieldValues.size()) {
                relaData = elasticsearchService.queryByConditions(indexName, fieldValues);
            }
            if (MergeType.intersection.name().equals(mergeType) && (relaData == null || relaData.length() == 0)) {
                // 如果是交集，没有查询到关联数据则merge失败
                System.err.println("merge failed, mergeType is " + mergeType + ", mergeIndex is " + indexName
                    + ", dataMap is " + dataMap + ", condition is " + fieldValues);
                result = false;
                break;
            }
            try {
                if (relaData != null && relaData.length() > 0) {
                    JsonNode dataNode = mapper.readTree(relaData);
                    mergeFields.forEach((field) -> {
                        // 为了避免字段重名，合并表重名字段做了处理，srcField是原名称，destField是tableMeta中的列名称
                        String srcField = field.get("srcField").asText();
                        String destField = field.get("destField").asText();
                        JsonNode valueNode = dataNode.get(srcField);
                        dataMap.put(destField, valueNode.asText());
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (subMergeIndices != null && subMergeIndices.size() > 0) {
                result = this.merge(dataMap, subMergeIndices);
                if (!result) {
                    break;
                }
            }
        }
        return result;

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

    private enum MergeType {
        left, // 左合并
        right, // 右合并
        union, // 并集
        intersection // 交集
    }

}
