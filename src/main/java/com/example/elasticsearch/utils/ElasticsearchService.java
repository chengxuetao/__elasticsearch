package com.example.elasticsearch.utils;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedDerivative;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.coredata.utils.elasticsearch.querydsl.BaseDSLBuilder;
import com.coredata.utils.elasticsearch.querydsl.ESFilterBuilder;
import com.coredata.utils.elasticsearch.vo.FieldValue;
import com.coredata.utils.elasticsearch.vo.ScrollResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ElasticsearchService {

    private ObjectMapper mapper = new ObjectMapper();

    private ESOperation esOperation;

    private ESSearch esSearch;

    private ElasticsearchFactory factory;

    public ElasticsearchService(ElasticsearchFactory factory, ESOperation esOperation, ESSearch esSearch) {
        this.factory = factory;
        this.esOperation = esOperation;
        this.esSearch = esSearch;
    }

    public String queryByCondition(String index, String field, Object val) {
        SearchRequest request = new SearchRequest(index + "*");
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(field, val));
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.trackTotalHits(true);
        builder.query(query);
        request.source(builder);
        SearchResponse response = esSearch.searchByRequest(request);
        SearchHits hits = response.getHits();
        if (hits != null && hits.getTotalHits().value > 0) {
            SearchHit searchHit = response.getHits().getAt(0);
            return searchHit.getSourceAsString();
        }
        return null;
    }

    public List<String> sortQueryListByCondition(String index, String field, Object val, FieldSortBuilder sort,
        int size) {
        List<String> result = new ArrayList<>();
        SearchRequest request = new SearchRequest(index + "*");
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(field, val));
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(query).trackTotalHits(true).sort(sort.getFieldName(), sort.order());
        request.source(builder);
        SearchResponse response = esSearch.searchByRequest(request);
        SearchHits hits = response.getHits();
        if (hits != null && hits.getTotalHits().value > 0) {
            int i = 0;
            for (SearchHit sh : hits) {
                if (++i > size) {
                    break;
                }
                result.add(sh.getSourceAsString());
            }
        }
        return result;
    }

    public String queryByConditions(String index, List<FieldValue> fieldValues) {
        SearchRequest request = new SearchRequest(index + "*");
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (FieldValue fv : fieldValues) {
            query.must(QueryBuilders.termQuery(fv.getField(), fv.getValue()));
        }
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(query);
        builder.trackTotalHits(true);
        request.source(builder);
        SearchResponse response = esSearch.searchByRequest(request);
        SearchHits hits = response.getHits();
        if (hits != null && hits.getTotalHits().value > 0) {
            SearchHit searchHit = response.getHits().getAt(0);
            return searchHit.getSourceAsString();
        }
        return null;
    }

    public String sortQueryByConditions(String index, String sortField, SortOrder sortOrder,
        List<FieldValue> fieldValues) {
        SearchRequest request = new SearchRequest(index + "*");
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (FieldValue fv : fieldValues) {
            query.must(QueryBuilders.termQuery(fv.getField(), fv.getValue()));
        }
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.trackTotalHits(true);
        builder.query(query).sort(sortField, sortOrder);
        request.source(builder);
        SearchResponse response = esSearch.searchByRequest(request);
        SearchHits hits = response.getHits();
        if (hits != null && hits.getTotalHits().value > 0) {
            SearchHit searchHit = response.getHits().getAt(0);
            return searchHit.getSourceAsString();
        }
        return null;
    }

    public List<String> queryListByCondition(String index, String field, Object val, int size) {
        List<String> result = new ArrayList<>();
        SearchRequest request = new SearchRequest(index + "*");
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(field, val));
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.trackTotalHits(true);
        builder.query(query);
        request.source(builder);
        SearchResponse response = esSearch.searchByRequest(request);
        SearchHits hits = response.getHits();
        if (hits != null && hits.getTotalHits().value > 0) {
            int i = 0;
            for (SearchHit sh : hits) {
                if (++i > size) {
                    break;
                }
                result.add(sh.getSourceAsString());
            }
        }
        return result;

    }

    public CommResult queryByQueryBuilder(SearchRequest request) {
        CommResult result = new CommResult();
        SearchResponse response = esSearch.searchByRequest(request);
        result.setUsed(response.getTook().getMillis());
        result.setTotal(response.getHits().getTotalHits().value);
        response.getHits().forEach(hit -> {
            result.addRecord(hit.getSourceAsString());
        });
        Aggregations aggs = response.getAggregations();
        if (aggs != null) {
            result.getAggregations().putAll(createAggs(null, aggs.asList()));
        }
        return result;
    }

    public CommResult queryByCondition(String conditionDsl, String index, String[] includes, String[] excludes)
        throws Exception {
        CommResult result = new CommResult();
        SearchRequest request = new SearchRequest(index + "*");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.trackTotalHits(true);
        builder.fetchSource(includes, excludes);
        long startTime = System.currentTimeMillis();
        if (conditionDsl != null && conditionDsl.length() > 0) {
            ESFilterBuilder filterBuilder = new ESFilterBuilder() {
            };
            filterBuilder.prepare(conditionDsl);
            QueryBuilder queryBuilder = filterBuilder.buildFilters();
            builder.query(queryBuilder);
            List<AggregationBuilder> builders = filterBuilder.buildAggregation();
            for (AggregationBuilder aggBuilder : builders) {
                builder.aggregation(aggBuilder);
            }
            request.source(builder);
            filterBuilder.pagination(request);
            filterBuilder.orderBy(request);

        }
        startTime = System.currentTimeMillis();
        SearchResponse response = esSearch.searchByRequest(request);
        startTime = System.currentTimeMillis();
        result.setUsed(response.getTook().getMillis());
        result.setTotal(response.getHits().getTotalHits().value);
        response.getHits().forEach(hit -> {
            String sourceAsString = hit.getSourceAsString();
            try {
                // 返回index、id，前台更新数据时使用
                JsonNode jsonNode = mapper.readTree(sourceAsString);
                ObjectNode objectNode = (ObjectNode) jsonNode;
                objectNode.put("_index", hit.getIndex());
                objectNode.put("_id", hit.getId());
                sourceAsString = objectNode.toString();
            } catch (Exception e) {
                // nothing todo
            }
            result.addRecord(sourceAsString);
        });
        Aggregations aggs = response.getAggregations();
        if (aggs != null) {
            result.getAggregations().putAll(createAggs(null, aggs.asList()));
        }
        return result;
    }

    public CommResult queryByCondition(String conditionDsl, String index) throws Exception {
        return queryByCondition(conditionDsl, index, null, null);
    }

    private Map<String, JsonNode> createAggs(ObjectNode parent, List<Aggregation> aggs) {
        Map<String, JsonNode> aggregations = new HashMap<>();
        for (Aggregation agg : aggs) {
            ArrayNode arrayJson = mapper.createArrayNode();
            if (agg instanceof Range) {
                Range tmp = (Range) agg;
                for (Range.Bucket entry : tmp.getBuckets()) {
                    String key = entry.getKeyAsString();
                    String fromAsString = entry.getFromAsString();
                    String toAsString = entry.getToAsString();
                    if (key == null || key.length() == 0) {
                        key = ((fromAsString == null || fromAsString.length() == 0) ? "*" : fromAsString) + "-"
                            + ((toAsString == null || toAsString.length() == 0) ? "*" : toAsString);
                    }
                    long docCount = entry.getDocCount();
                    ObjectNode tmpJson = mapper.createObjectNode();
                    tmpJson.put("key", key);
                    tmpJson.put("from", fromAsString);
                    tmpJson.put("to", toAsString);
                    tmpJson.put("count", docCount);
                    arrayJson.add(tmpJson);
                    if (entry.getAggregations() != null) {
                        createAggs(tmpJson, entry.getAggregations().asList());
                    }
                }
                aggregations.put(agg.getName(), arrayJson);
            } else if (agg instanceof InternalReverseNested) {
                InternalReverseNested tmp = (InternalReverseNested) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                createAggs(tmpJson, tmp.getAggregations().asList());
                // aggregations = mapper.convertValue(parent, Map.class);
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof Histogram) {
                Histogram tmp = (Histogram) agg;
                for (Histogram.Bucket entry : tmp.getBuckets()) {
                    ObjectNode tmpJson = mapper.createObjectNode();
                    ZonedDateTime key = (ZonedDateTime) entry.getKey();
                    tmpJson.put("key", key.toOffsetDateTime().toInstant().toEpochMilli());
                    tmpJson.put("keyAsMs", key.toOffsetDateTime().toInstant().toEpochMilli());
                    tmpJson.put("count", entry.getDocCount());
                    arrayJson.add(tmpJson);
                    if (entry.getAggregations() != null) {
                        createAggs(tmpJson, entry.getAggregations().asList());
                    }
                }
                aggregations.put(agg.getName(), arrayJson);
            } else if (agg instanceof Terms) {
                Terms tmp = (Terms) agg;
                for (Terms.Bucket entry : tmp.getBuckets()) {
                    ObjectNode tmpJson = mapper.createObjectNode();
                    tmpJson.put("key", entry.getKeyAsString());
                    tmpJson.put("count", entry.getDocCount());
                    arrayJson.add(tmpJson);
                    if (entry.getAggregations() != null) {
                        createAggs(tmpJson, entry.getAggregations().asList());
                    }
                }
                aggregations.put(agg.getName(), arrayJson);
            } else if (agg instanceof Stats) {
                Stats tmp = (Stats) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("min", verifyValue(tmp.getMin()));
                tmpJson.put("max", verifyValue(tmp.getMax()));
                tmpJson.put("avg", verifyValue(tmp.getAvg()));
                tmpJson.put("sum", verifyValue(tmp.getSum()));
                tmpJson.put("count", tmp.getCount());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof InternalNested) {
                InternalNested tmp = (InternalNested) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", tmp.getName());
                tmpJson.put("count", tmp.getDocCount());
                aggregations.put(agg.getName(), tmpJson);
                createAggs(tmpJson, tmp.getAggregations().asList());
            } else if (agg instanceof Cardinality) {
                Cardinality tmp = (Cardinality) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", tmp.getName());
                tmpJson.put("count", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof InternalSum) {
                InternalSum tmp = (InternalSum) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof InternalMax) {
                InternalMax tmp = (InternalMax) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof InternalMin) {
                InternalMin tmp = (InternalMin) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof InternalAvg) {
                InternalAvg tmp = (InternalAvg) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof InternalSimpleValue) {
                InternalSimpleValue tmp = (InternalSimpleValue) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "cumulative");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof InternalTopHits) {
                InternalTopHits tmp = (InternalTopHits) agg;
                try {
                    JsonNode tmpJson = mapper.readTree(tmp.getHits().getHits()[0].getSourceAsString());
                    aggregations.put(agg.getName(), tmpJson);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (agg instanceof ParsedSum) {
                ParsedSum tmp = (ParsedSum) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof ParsedMax) {
                ParsedMax tmp = (ParsedMax) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof ParsedMin) {
                ParsedMin tmp = (ParsedMin) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof ParsedAvg) {
                ParsedAvg tmp = (ParsedAvg) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", "original");
                tmpJson.put("value", tmp.getValue());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof ParsedDerivative) {
                ParsedDerivative tmp = (ParsedDerivative) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", tmp.getName());
                tmpJson.put("value", tmp.value());
                aggregations.put(agg.getName(), tmpJson);
            } else if (agg instanceof ParsedSimpleValue) {
                ParsedSimpleValue tmp = (ParsedSimpleValue) agg;
                ObjectNode tmpJson = mapper.createObjectNode();
                tmpJson.put("key", tmp.getName());
                tmpJson.put("value", tmp.value());
                aggregations.put(agg.getName(), tmpJson);
            }
        }
        if (parent != null) {
            parent.set("subAggregations", mapper.valueToTree(aggregations));
        }
        return aggregations;
    }

    private Double verifyValue(Double target) {
        Double result = target;
        if (Double.isInfinite(target) || Double.isNaN(target)) {
            result = null;
        }
        return result;
    }

    public void save(byte[] data, String index) {
        esOperation.save(data, index);
    }

    @Deprecated
    public void save(byte[] data, String index, String type) {
        esOperation.save(data, index, type);
    }

    @Deprecated
    public void save(byte[] data, String index, String type, String id) {
        esOperation.save(data, index, type, id);
    }

    public void save(List<Map<String, Object>> datas, String index) {
        esOperation.save(datas, index);
    }

    @Deprecated
    public void save(List<Map<String, Object>> datas, String index, String type) {
        esOperation.save(datas, index, type);
    }

    public void upsert(List<Map<String, Object>> datas, String index) {
        esOperation.upsert(datas, index);
    }

    @Deprecated
    public void upsert(List<Map<String, Object>> datas, String index, String type) {
        esOperation.upsert(datas, index, type);
    }

    public Response rawQuery(String method, String endPoint, String jsonData) {
        assert factory.getClient() != null;
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new NStringEntity(jsonData, ContentType.APPLICATION_JSON);
        try {
            Request request = new Request(method, endPoint);
            request.setEntity(entity);
            request.addParameters(params);
            return factory.getClient().getLowLevelClient().performRequest(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public long queryTotalSize(String index) {
        try {
            String queryDsl = BaseDSLBuilder.instance().pageable(1, 1).build();
            CommResult result = queryByCondition(queryDsl, index);
            if (result != null) {
                return result.getTotal();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;

    }

    public CommResult queryByPage(int page, int pageSize, String index) throws Exception {
        String queryDsl = BaseDSLBuilder.instance().pageable(page, pageSize).build();
        CommResult result = queryByCondition(queryDsl, index);
        return result;
    }

    public ScrollResult scrollIndex(String keyField, Object keyValue, String timeField, long from, long to,
        String index) {
        ScrollResult result = new ScrollResult();
        try {
            int size = 1000;
            SearchResponse resp = null;
            // 默认是查询所有
            TermQueryBuilder idBuilder = QueryBuilders.termQuery(keyField, keyValue);
            RangeQueryBuilder timeBuilder = QueryBuilders.rangeQuery(timeField).gte(from).lt(to);
            QueryBuilder builder = QueryBuilders.boolQuery().must(idBuilder).must(timeBuilder);
            // 指定一个index和type
            // 使用原生排序优化性能
            // 设置每批读取的数据量
            SearchRequest request = new SearchRequest(index + "*");
            SearchSourceBuilder source =
                new SearchSourceBuilder().trackTotalHits(true).sort("_doc", SortOrder.ASC).size(size).query(builder);
            request.source(source);
            // 设置 search context 维护1分钟的有效期
            request.scroll(TimeValue.timeValueMinutes(1));
            // 获得首次的查询结果
            resp = esSearch.searchByRequest(request);
            long total = resp.getHits().getTotalHits().value;
            result.getData().setTotal(total);
            do {
                // 读取结果集数据
                for (SearchHit hit : resp.getHits().getHits()) {
                    result.getData().addRecord(hit.getSourceAsString());
                }
                // 将scorllId循环传递
                String scorllId = resp.getScrollId();
                if (scorllId != null && scorllId.length() > 0) {
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scorllId);
                    scrollRequest.scroll(TimeValue.timeValueMinutes(1));
                    resp = esSearch.searchByScrollRequest(scrollRequest);
                } else {
                    break;
                }
                // 当searchHits的数组为空的时候结束循环，至此数据全部读取完毕
            } while (resp.getHits().getHits().length != 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public ScrollResult scrollIndex(String index) {
        ScrollResult result = new ScrollResult();
        try {
            int size = 1000;
            SearchResponse resp = null;
            // 指定一个index和type
            // 使用原生排序优化性能
            // 设置每批读取的数据量
            // 默认是查询所有
            SearchRequest request = new SearchRequest(index + "*");
            SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(true).sort("_doc", SortOrder.ASC)
                .size(size).query(QueryBuilders.queryStringQuery("*:*"));
            request.source(source);
            // 设置 search context 维护1分钟的有效期
            request.scroll(TimeValue.timeValueMinutes(1));
            // 获得首次的查询结果
            resp = esSearch.searchByRequest(request);
            do {
                // 读取结果集数据
                for (SearchHit hit : resp.getHits().getHits()) {
                    result.getData().addRecord(hit.getSourceAsString());
                }
                // 将scorllId循环传递
                String scorllId = resp.getScrollId();
                if (scorllId != null && scorllId.length() > 0) {
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scorllId);
                    scrollRequest.scroll(TimeValue.timeValueMinutes(1));
                    resp = esSearch.searchByScrollRequest(scrollRequest);
                } else {
                    break;
                }
                // 当searchHits的数组为空的时候结束循环，至此数据全部读取完毕
            } while (resp.getHits().getHits().length != 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public ScrollResult scrollWithSort(String index, String conditionDsl, int size) throws Exception {
        ScrollResult result = new ScrollResult();
        // 指定一个index和type
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("*:*");
        SearchRequest request = new SearchRequest(index + "*");
        SearchSourceBuilder source = new SearchSourceBuilder().size(size);
        request.source(source);
        if (conditionDsl != null && conditionDsl.length() > 0) {
            ESFilterBuilder filterBuilder = new ESFilterBuilder() {
            };
            filterBuilder.prepare(conditionDsl);
            queryBuilder = filterBuilder.buildFilters();
            filterBuilder.orderBy(request);
        }
        source.query(queryBuilder);
        // 设置每批读取的数据量
        // 默认是查询所有
        // 设置 search context 维护5分钟的有效期
        request.scroll(TimeValue.timeValueMinutes(5));
        // 获得首次的查询结果
        SearchResponse scrollResp = esSearch.searchByRequest(request);
        // long t1 = System.currentTimeMillis();
        if (scrollResp != null) {
            result.setScrollId(scrollResp.getScrollId());
            result.getData().setUsed(scrollResp.getTook().getMillis());
            result.getData().setTotal(scrollResp.getHits().getTotalHits().value);
            scrollResp.getHits().forEach(hit -> {
                result.getData().addRecord(hit.getSourceAsString());
            });
        }
        return result;
    }

    public ScrollResult scroll(String index, String conditionDsl, int size) throws Exception {
        // long t = System.currentTimeMillis();
        ScrollResult result = new ScrollResult();

        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("*:*");
        if (conditionDsl != null && conditionDsl.length() > 0) {
            ESFilterBuilder filterBuilder = new ESFilterBuilder() {
            };
            filterBuilder.prepare(conditionDsl);
            queryBuilder = filterBuilder.buildFilters();
        }

        // 指定一个index和type
        // 使用原生排序优化性能
        // 设置每批读取的数据量
        // 默认是查询所有
        SearchRequest request = new SearchRequest(index + "*");
        SearchSourceBuilder source =
            new SearchSourceBuilder().sort("_doc", SortOrder.ASC).size(size).query(queryBuilder);
        request.source(source);
        // 设置 search context 维护5分钟的有效期
        request.scroll(TimeValue.timeValueMinutes(5));
        // 获得首次的查询结果
        SearchResponse scrollResp = esSearch.searchByRequest(request);

        // long t1 = System.currentTimeMillis();
        if (scrollResp != null) {
            result.setScrollId(scrollResp.getScrollId());
            result.getData().setUsed(scrollResp.getTook().getMillis());
            result.getData().setTotal(scrollResp.getHits().getTotalHits().value);
            scrollResp.getHits().forEach(hit -> {
                result.getData().addRecord(hit.getSourceAsString());
            });
        }
        return result;
    }

    public ScrollResult scroll(String index, int size) {
        // long t = System.currentTimeMillis();
        ScrollResult result = new ScrollResult();

        // 指定一个index和type
        // 使用原生排序优化性能
        // 设置每批读取的数据量
        // 默认是查询所有
        SearchRequest request = new SearchRequest(index + "_*");
        SearchSourceBuilder source = new SearchSourceBuilder().sort("_doc", SortOrder.ASC).size(size)
            .query(QueryBuilders.queryStringQuery("*:*"));
        request.source(source);
        // 设置 search context 维护1分钟的有效期
        request.scroll(TimeValue.timeValueMinutes(1));
        // 获得首次的查询结果
        SearchResponse scrollResp = esSearch.searchByRequest(request);

        // long t1 = System.currentTimeMillis();
        if (scrollResp != null) {
            result.setScrollId(scrollResp.getScrollId());
            result.getData().setUsed(scrollResp.getTook().getMillis());
            result.getData().setTotal(scrollResp.getHits().getTotalHits().value);
            scrollResp.getHits().forEach(hit -> {
                result.getData().addRecord(hit.getSourceAsString());
            });
        }
        return result;
    }

    public ScrollResult scroll(String scrollId) {
        ScrollResult result = new ScrollResult();
        SearchScrollRequest request = new SearchScrollRequest(scrollId);
        request.scroll(TimeValue.timeValueMinutes(1));
        SearchResponse scrollResp = esSearch.searchByScrollRequest(request);
        if (scrollResp != null) {
            result.setScrollId(scrollResp.getScrollId());
            result.getData().setUsed(scrollResp.getTook().getMillis());
            result.getData().setTotal(scrollResp.getHits().getTotalHits().value);
            scrollResp.getHits().forEach(hit -> {
                result.getData().addRecord(hit.getSourceAsString());
            });
        }
        return result;
    }

}
