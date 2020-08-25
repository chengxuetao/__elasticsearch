package com.example.elasticsearch.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * ES操作相关实现类
 */
public class ESOperation {

    private final ObjectMapper mapper = new ObjectMapper();

    private ElasticsearchFactory factory;

    public ESOperation(ElasticsearchFactory factory) {
        this.factory = factory;
    }

    public void save(byte[] data, String index) {
        save(data, index, Constants.DEFAULT_TYPE, null);
    }

    public void save(byte[] data, String index, String type) {
        save(data, index, type, null);
    }

    @SuppressWarnings("deprecation")
    public void save(byte[] data, String index, String type, String id) {
        IndexRequest request = new IndexRequest(index);
        request.source(data, XContentType.JSON);
        if (type != null)
            request.type(type);
        if (id != null)
            request.id(id);
        factory.getBulkProcessor().add(request);
    }

    public void save(byte[] data, String index, boolean refreshPolicy) throws IOException {
        IndexRequest request = null;
        if (refreshPolicy) {
            request = new IndexRequest(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        } else {
            request = new IndexRequest(index);
        }
        request.source(data, XContentType.JSON);
        factory.getClient().index(request, RequestOptions.DEFAULT);
    }

    public void save(Map<String, Object> data, String index) {
        save(data, index, Constants.DEFAULT_TYPE, null);
    }

    public void save(Map<String, Object> data, String index, String type) {
        save(data, index, type, null);
    }

    @SuppressWarnings("deprecation")
    public void save(Map<String, Object> data, String index, String type, String id) {
        IndexRequest request = new IndexRequest(index, type);
        request.source(data);
        if (id != null)
            request.id(id);
        factory.getBulkProcessor().add(request);
    }

    public void save(List<?> data, String index) {
        save(data, index, Constants.DEFAULT_TYPE);
    }

    @SuppressWarnings("unchecked")
    public void save(List<?> data, String index, String type) {
        if (data == null || data.isEmpty())
            return;
        data.forEach(m -> {
            if (m instanceof Map) {
                save((Map<String, Object>) m, index, type);
            } else if (m instanceof String) {
                save(m.toString(), index, type);
            }
        });
    }

    public void save(String data, String index) {
        save(data, index, Constants.DEFAULT_TYPE);
    }

    public void save(String data, String index, String type) {
        save(data, index, type, null);
    }

    @SuppressWarnings("deprecation")
    public void save(String data, String index, String type, String id) {
        IndexRequest request = new IndexRequest(index, type);
        request.source(data, XContentType.JSON);
        if (id != null)
            request.id(id);
        factory.getBulkProcessor().add(request);
    }

    public void upsert(List<Map<String, Object>> datas, String index) {
        upsert(datas, index, Constants.DEFAULT_TYPE);
    }

    @SuppressWarnings("deprecation")
    public void upsert(List<Map<String, Object>> datas, String index, String type) {
        for (Map<String, Object> data : datas) {
            String id = data.get("indexId").toString();
            XContentBuilder content;
            try {
                content = convert(data);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
            IndexRequest indexRequest = new IndexRequest(index, type, id).source(content);
            UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(content).upsert(indexRequest);
            factory.getBulkProcessor().add(updateRequest);
        }
    }

    private XContentBuilder convert(Map<String, Object> source) throws IOException {
        XContentBuilder object = XContentFactory.jsonBuilder().startObject();
        Set<Entry<String, Object>> entrySet = source.entrySet();
        for (Entry<String, Object> entry : entrySet) {
            object.field(entry.getKey(), entry.getValue());
        }
        object = object.endObject();
        return object;
    }

    public long deleteByQuery(String index, QueryBuilder builder) {
        long deleted = 0L;
        if (indexExists(index)) {
            DeleteByQueryRequest request = new DeleteByQueryRequest(index).setQuery(builder).setRefresh(true);
            try {
                BulkByScrollResponse response = factory.getClient().deleteByQuery(request, RequestOptions.DEFAULT);
                deleted = response.getDeleted();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return deleted;
    }

    @SuppressWarnings("deprecation")
    public void deleteByQuery(String index, String type, QueryBuilder builder) {
        if (indexExists(index)) {
            DeleteByQueryRequest request =
                new DeleteByQueryRequest(index).types(type).setQuery(builder).setRefresh(true);
            try {
                factory.getClient().deleteByQuery(request, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void addOrUpdateTemplate(String templateName, InputStream is) {
        addOrUpdateTemplate(templateName, is, null);
    }

    public void addOrUpdateTemplate(String templateName, InputStream is, Map<String, Object> params) {
        try {
            ObjectNode template = (ObjectNode) mapper.readTree(is);
            if (params != null && params.size() > 0) {
                template.put("index_patterns", params.get("index_patterns").toString());
                ObjectNode propDef = (ObjectNode) template.get("mappings");
                propDef.set("properties", (JsonNode) params.get("fields"));
            }
            ClusterHealthResponse response =
                factory.getClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
            int nodeCount = response.getNumberOfDataNodes();
            int shardNumber = nodeCount >= 3 ? 3 : nodeCount;
            int replicasNumber = nodeCount > 1 ? 1 : 0;
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName);
            request.settings(Settings.builder().put("index.number_of_shards", shardNumber)
                .put("index.number_of_replicas", replicasNumber));
            ObjectNode settings = (ObjectNode) template.get("settings");
            if (settings != null) {
                settings.set("index.number_of_shards", new IntNode(shardNumber));
                settings.set("number_of_replicas", new IntNode(replicasNumber));
            }
            request.source(mapper.writeValueAsString(template), XContentType.JSON);
            factory.getClient().indices().putTemplate(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                ;
            }
        }
    }

    public void addOrUpdateTemplate(String templateName, String template) {
        try {
            ObjectNode templateObj = (ObjectNode) mapper.readTree(template);
            ClusterHealthResponse response =
                factory.getClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
            int nodeCount = response.getNumberOfDataNodes();
            int shardNumber = nodeCount >= 3 ? 3 : nodeCount;
            int replicasNumber = nodeCount > 1 ? 1 : 0;
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName);
            request.settings(Settings.builder().put("index.number_of_shards", shardNumber)
                .put("index.number_of_replicas", replicasNumber));
            ObjectNode settings = (ObjectNode) templateObj.get("settings");
            if (settings != null) {
                settings.set("index.number_of_shards", new IntNode(shardNumber));
                settings.set("number_of_replicas", new IntNode(replicasNumber));
            }
            request.source(mapper.writeValueAsString(templateObj), XContentType.JSON);
            factory.getClient().indices().putTemplate(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteTemplate(String templateName) {
        try {
            DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest();
            request.name(templateName);
            factory.getClient().indices().deleteTemplate(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void update(byte[] data, String index, String documentId) {
        UpdateRequest request = new UpdateRequest(index, documentId);
        request.doc(data, XContentType.JSON);
        try {
            factory.getClient().update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void update(byte[] data, String index, String documentId, boolean refreshPolicy) {
        UpdateRequest request = null;
        if (refreshPolicy) {
            request = new UpdateRequest(index, documentId).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        } else {
            request = new UpdateRequest(index, documentId);
        }
        request.doc(data, XContentType.JSON);
        try {
            factory.getClient().update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("deprecation")
    public void update(byte[] data, String index, String type, String documentId) {
        UpdateRequest request = new UpdateRequest(index, type, documentId);
        request.doc(data, XContentType.JSON);
        try {
            factory.getClient().update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void delete(String index, String documentId) {
        delete(index, Constants.DEFAULT_TYPE, documentId);
    }

    @SuppressWarnings("deprecation")
    public void delete(String index, String type, String documentId) {
        if (indexExists(index)) {
            DeleteRequest request = new DeleteRequest(index, type, documentId);
            try {
                factory.getClient().delete(request, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean templateExists(String templateName) {
        boolean exist = false;
        IndexTemplatesExistRequest request = new IndexTemplatesExistRequest(templateName);

        try {
            exist = factory.getClient().indices().existsTemplate(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exist;
    }

    public boolean indexExists(String indexName) {
        boolean exist = false;
        GetIndexRequest request = new GetIndexRequest(indexName);

        try {
            exist = factory.getClient().indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exist;
    }

    public void deleteIndices(String... indices) {
        DeleteIndexRequest request = new DeleteIndexRequest(indices);
        try {
            factory.getClient().indices().delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeIndices(String... indices) {
        CloseIndexRequest request = new CloseIndexRequest(indices);
        try {
            factory.getClient().indices().close(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void cloneIndex(String targetIndex, String sourceIndex) {
        ResizeRequest request = new ResizeRequest(targetIndex, sourceIndex);
        request.setResizeType(ResizeType.CLONE);
        try {
            factory.getClient().indices().clone(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reIndices(String destIndice, String... sourceIndices) {
        reIndices(null, destIndice, sourceIndices);
    }

    public void reIndices(QueryBuilder query, String destIndice, String... sourceIndices) {
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices(sourceIndices).setDestIndex(destIndice);
        if (query != null) {
            request.setSourceQuery(query);
        }
        try {
            factory.getClient().reindex(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createIndicesAliases(Map<String, String> indicesAliases) {
        if (indicesAliases == null || indicesAliases.size() == 0)
            return;
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        indicesAliases.forEach((k, v) -> {
            AliasActions action = new AliasActions(AliasActions.Type.ADD).index(k).alias(v);
            request.addAliasAction(action);
        });
        try {
            factory.getClient().indices().updateAliases(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void forceMergeIndices(String... indices) {
        ForceMergeRequest request = new ForceMergeRequest(indices);
        request.maxNumSegments(1);
        try {
            factory.getClient().indices().forcemerge(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long updateByQuery(String index, QueryBuilder builder) {
        long updated = 0L;
        UpdateByQueryRequest request = new UpdateByQueryRequest(index).setQuery(builder).setRefresh(true);
        try {
            BulkByScrollResponse response = factory.getClient().updateByQuery(request, RequestOptions.DEFAULT);
            updated = response.getUpdated();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return updated;
    }

}
