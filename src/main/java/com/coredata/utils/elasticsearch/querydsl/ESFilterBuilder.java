package com.coredata.utils.elasticsearch.querydsl;

import static com.coredata.utils.elasticsearch.querydsl.AggregationType.Derivative;
import static com.coredata.utils.elasticsearch.querydsl.AggregationType.SerialDiff;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.SerialDiffPipelineAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public abstract class ESFilterBuilder {

    private ObjectMapper mapper = new ObjectMapper();

    protected String aggName = "agg";

    protected String nestedAggName = "nestedAgg";

    protected Sort sort;

    protected Pagination pagination;

    protected TimeRangeFilter timeRangeFilter;

    protected JsonNode aggregations;

    protected JsonNode filter;

    protected QueryBuilder filterToBuilder(Filter filter) throws Exception {

        QueryBuilder builder = null;

        String field = filter.getField();

        switch (filter.getOps()) {
            case lt:
                builder = QueryBuilders.rangeQuery(field).lt(filter.getValue());
                break;
            case lte:
                builder = QueryBuilders.rangeQuery(field).lte(filter.getValue());
                break;
            case gt:
                builder = QueryBuilders.rangeQuery(field).gt(filter.getValue());
                break;
            case gte:
                builder = QueryBuilders.rangeQuery(field).gte(filter.getValue());
                break;
            case eq:
                builder = QueryBuilders.termQuery(field, filter.getValue());
                break;
            case neq:
                builder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(field, filter.getValue()));
                break;
            case isnull:
                builder = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field));
                break;
            case notnull:
                builder = QueryBuilders.existsQuery(field);
                break;
            case contains:
                builder =
                    QueryBuilders.queryStringQuery("*" + filter.getValue() + "*").field(field).analyzeWildcard(true);
                break;
            case notcontains:
                builder = QueryBuilders.queryStringQuery("NOT *" + filter.getValue() + "*").field(field)
                    .analyzeWildcard(true);
                break;
            case like:
                builder = QueryBuilders.wildcardQuery(field, filter.getValue().toString());
                break;
            case prefix:
                if (filter.getValue() instanceof String) {
                    builder = QueryBuilders.prefixQuery(field, filter.getValue().toString());
                } else {
                    builder = QueryBuilders.matchQuery(field, filter.getValue());
                }
                break;
            default:
                throw new Exception("Unsupported operator " + filter.getOps().toString() + ".");
        }

        if (field.contains(".")) {
            String path = field.substring(0, field.lastIndexOf("."));
            return QueryBuilders.nestedQuery(path, builder, ScoreMode.None);
        }

        return builder;
    }

    public void orderBy(SearchRequest request) throws Exception {
        if (sort != null) {
            for (String field : sort.getFields()) {
                SortOrder sortOrder = sort.getDirection() == Direction.ASC ? SortOrder.ASC : SortOrder.DESC;
                request.source().sort(field, sortOrder);
            }
        }
    }

    public void orderBy(SearchRequestBuilder search) {
        if (sort != null) {
            for (String field : sort.getFields()) {

                search.addSort(field, sort.getDirection() == Direction.ASC ? SortOrder.ASC : SortOrder.DESC);
            }
        }
    }

    public void pagination(SearchRequest request) throws Exception {
        if (pagination != null) {
            request.source().from(pagination.from()).size(pagination.getSize());
        } else {
            if (aggregations != null) {
                request.source().from(0).size(0);
            } else {
                request.source().from(0).size(50);
            }
        }
    }

    protected List<AggregationBuilder> createAggregation(String[] subAggs) {

        List<AggregationBuilder> aggs = new ArrayList<AggregationBuilder>();
        for (String prop : subAggs) {
            String aggName = UUID.randomUUID().toString();
            aggs.add(AggregationBuilders.terms(aggName).field(prop));
        }
        return aggs;
    }

    protected void createAggregation(AggregationBuilder aggBuilder, String[] subAggs) {
        String prop = subAggs[0];
        String aggName = UUID.randomUUID().toString();
        AggregationBuilder tmp = AggregationBuilders.terms(aggName).field(prop);
        if (aggBuilder == null) {
            aggBuilder = tmp;
        } else {
            aggBuilder.subAggregation(tmp);
        }
        String[] subs = Arrays.copyOfRange(subAggs, 1, subAggs.length);
        if (subs.length > 0) {
            createAggregation(tmp, subs);
        }
    }

    protected AggregationBuilder createAggregation(String field) {
        return AggregationBuilders.terms(aggName).field(field);
    }

    public void prepare(String query) throws Exception {

        try {
            JsonNode condition = mapper.readTree(query);

            if (condition.has("filter")) {
                filter = condition.get("filter");
            }

            if (condition.has("aggregations")) {
                aggregations = condition.get("aggregations");
            }

            if (condition.has("sort")) {
                sort = mapper.readValue(condition.get("sort").toString(), Sort.class);
            }

            if (condition.has("pagination")) {
                pagination = mapper.readValue(condition.get("pagination").toString(), Pagination.class);
            }

            if (condition.has("timeRange")) {
                timeRangeFilter = mapper.readValue(condition.get("timeRange").toString(), TimeRangeFilter.class);
            }

        } catch (IOException e) {
            throw new Exception("Parse query dsl error.", e);
        }
    }

    private QueryBuilder buildFilters(Ops ops, JsonNode filter) throws Exception {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (filter instanceof ArrayNode) {
            ArrayNode filters = (ArrayNode) filter;
            Iterator<JsonNode> ite = filters.iterator();
            while (ite.hasNext()) {
                JsonNode tmp = ite.next();
                try {
                    Filter f = mapper.readValue(mapper.writeValueAsString(tmp), Filter.class);
                    if (ops == Ops.and) {
                        boolQueryBuilder.must(filterToBuilder(f));
                    } else if (ops == Ops.or) {
                        boolQueryBuilder.should(filterToBuilder(f));
                    }
                } catch (Exception e) {
                    Entry<String, JsonNode> entry = tmp.fields().next();
                    try {
                        if (ops == Ops.and) {
                            boolQueryBuilder.must(buildFilters(Ops.valueOf(entry.getKey()), entry.getValue()));
                        } else if (ops == Ops.or) {
                            boolQueryBuilder.should(buildFilters(Ops.valueOf(entry.getKey()), entry.getValue()));
                        }
                    } catch (Exception e1) {
                        try {
                            System.err.println(mapper.writeValueAsString(entry)
                                + "====================No enum constant ..querydsl.Ops.field");
                        } catch (JsonProcessingException ex) {
                            // do Nothing
                        }
                        e1.printStackTrace();
                    }
                }
            }

        } else {
            try {
                Filter f = mapper.readValue(mapper.writeValueAsString(filter), Filter.class);
                if (ops != null) {
                    if (ops == Ops.and) {
                        boolQueryBuilder.must().add(filterToBuilder(f));
                    } else if (ops == Ops.or) {
                        boolQueryBuilder.should().add(filterToBuilder(f));
                    }
                } else {
                    boolQueryBuilder.must(filterToBuilder(f));
                }
            } catch (Exception e) {
                Iterator<Entry<String, JsonNode>> ite = filter.fields();
                while (ite.hasNext()) {
                    Entry<String, JsonNode> entry = ite.next();
                    if (ops == Ops.and) {
                        boolQueryBuilder.must(buildFilters(Ops.valueOf(entry.getKey()), entry.getValue()));
                    } else if (ops == Ops.or) {
                        boolQueryBuilder.should(buildFilters(Ops.valueOf(entry.getKey()), entry.getValue()));
                    } else {
                        return buildFilters(Ops.valueOf(entry.getKey()), entry.getValue());
                    }
                }
            }
        }

        return boolQueryBuilder;

    }

    public QueryBuilder timeRangeQuery() {
        if (timeRangeFilter != null) {
            return QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(timeRangeFilter.getField())
                .gte(timeRangeFilter.getStartMs()).lte(timeRangeFilter.getEndMs()));
        }
        return null;
    }

    public AggregationBuilder buildAggregation(AggregationBuilder parent, JsonNode aggregationDef) {

        String aggName =
            aggregationDef.has("name") ? aggregationDef.get("name").asText() : UUID.randomUUID().toString();
        String field = aggregationDef.get("field") != null ? aggregationDef.get("field").asText() : null;
        String calc = aggregationDef.get("calc") != null ? aggregationDef.get("calc").asText() : null;
        String lag = aggregationDef.get("lag") != null ? aggregationDef.get("lag").asText() : "1";
        String count = aggregationDef.get("count") != null ? aggregationDef.get("count").asText() : null;
        int aggSize = aggregationDef.get("size") != null ? aggregationDef.get("size").asInt() : 50;
        String orderAggrName =
            aggregationDef.get("orderAggr") != null ? aggregationDef.get("orderAggr").asText() : null;
        String orderMetricName =
            aggregationDef.get("orderMetric") != null ? aggregationDef.get("orderMetric").asText() : null;
        String orderDirect =
            aggregationDef.get("orderDirect") != null ? aggregationDef.get("orderDirect").asText() : null;
        String countOrder = aggregationDef.get("countOrder") != null ? aggregationDef.get("countOrder").asText() : null;
        String keyOrder = aggregationDef.get("keyOrder") != null ? aggregationDef.get("keyOrder").asText() : null;

        AggregationBuilder tmpBuilder = null;
        AggregationType aggType = AggregationType.valueOf(aggregationDef.get("type").asText());
        switch (aggType) {
            case Range:
                RangeAggregationBuilder rangeBuilder = AggregationBuilders.range(aggName).field(field);
                ArrayNode ranges = (ArrayNode) aggregationDef.get("ranges");
                if (ranges != null) {
                    ranges.forEach(range -> {
                        if (range.has("from") && range.has("to")) {
                            rangeBuilder.addRange(range.get("from").asDouble(), range.get("to").doubleValue());
                        } else if (range.has("from") && !range.has("to")) {
                            rangeBuilder.addUnboundedFrom(range.get("from").asDouble());
                        } else if (!range.has("from") && range.has("to")) {
                            rangeBuilder.addUnboundedFrom(range.get("to").asDouble());
                        }
                    });
                }
                tmpBuilder = rangeBuilder;
                break;
            case Distinct:
                tmpBuilder = AggregationBuilders.cardinality(aggName).field(field);
                break;
            case Stats:
                tmpBuilder = AggregationBuilders.stats(aggName).field(field);
                break;
            case Min:
                tmpBuilder = AggregationBuilders.min(aggName).field(field);
                break;
            case Max:
                tmpBuilder = AggregationBuilders.max(aggName).field(field);
                break;
            case Avg:
                tmpBuilder = AggregationBuilders.avg(aggName).field(field);
                break;
            case Count:
                tmpBuilder = AggregationBuilders.count(aggName).field(field);
                break;
            case Sum:
                tmpBuilder = AggregationBuilders.sum(aggName).field(field);
                break;
            case Term:
                TermsAggregationBuilder addBuilder = AggregationBuilders.terms(aggName).field(field).size(aggSize);

                if (count != null) {
                    Map<String, String> bucketsPathsMap = new HashMap<>();
                    bucketsPathsMap.put("doc_count", "_count");
                    Script script = new Script("params.doc_count" + count);

                    BucketSelectorPipelineAggregationBuilder bs =
                        PipelineAggregatorBuilders.bucketSelector("bucket_filter", bucketsPathsMap, script);
                    addBuilder.subAggregation(bs);
                }

                if (countOrder != null) {
                    BucketOrder order = InternalOrder.count(Direction.ASC.toString().equals(countOrder));
                    addBuilder = addBuilder.order(order);
                }
                if (keyOrder != null) {
                    BucketOrder order = InternalOrder.key(Direction.ASC.toString().equals(keyOrder));
                    addBuilder = addBuilder.order(order);
                }
                if (orderAggrName != null && orderMetricName != null) {
                    boolean asc = Direction.ASC.toString().equals(orderDirect);
                    BucketOrder order = InternalOrder.aggregation(orderAggrName, orderMetricName, asc);
                    addBuilder = addBuilder.order(order);
                }
                tmpBuilder = addBuilder;
                break;
            case DateHistogram:
                if (field != null && field.length() > 0) {
                    field = timeRangeFilter.getField();
                }
                DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram(aggName).field(field)
                    .minDocCount(0).timeZone(ZoneId.of("Asia/Shanghai"));
                tmpBuilder = aggBuilder;
                String interval = aggregationDef.get("interval").asText();
                String unit = interval.substring(interval.length() - 1);
                int val = Integer.valueOf(interval.substring(0, interval.length() - 1));
                switch (unit) {
                    case "y":
                        aggBuilder.calendarInterval(DateHistogramInterval.YEAR);
                        break;
                    case "M":
                        aggBuilder.calendarInterval(DateHistogramInterval.MONTH);
                        break;
                    case "w":
                        if (val == 1) {
                            aggBuilder.calendarInterval(DateHistogramInterval.WEEK);
                        } else {
                            aggBuilder.fixedInterval(DateHistogramInterval.weeks(val));
                        }
                        break;
                    case "d":
                        if (val == 1) {
                            aggBuilder.calendarInterval(DateHistogramInterval.DAY);
                        } else {
                            aggBuilder.fixedInterval(DateHistogramInterval.days(val));
                        }
                        break;
                    case "h":
                        if (val == 1) {
                            aggBuilder.calendarInterval(DateHistogramInterval.HOUR);
                        } else {
                            aggBuilder.fixedInterval(DateHistogramInterval.hours(val));
                        }
                        break;
                    case "m":
                        if (val == 1) {
                            aggBuilder.calendarInterval(DateHistogramInterval.MINUTE);
                        } else {
                            aggBuilder.fixedInterval(DateHistogramInterval.minutes(val));
                        }
                        break;
                    case "s":
                        if (val == 1) {
                            aggBuilder.calendarInterval(DateHistogramInterval.SECOND);
                        } else {
                            aggBuilder.fixedInterval(DateHistogramInterval.seconds(val));
                        }
                        break;
                    default:
                        break;
                }
                break;

            case IPRange:
                IpRangeAggregationBuilder ipRangeBuilder = AggregationBuilders.ipRange(aggName).field(field);
                tmpBuilder = ipRangeBuilder;
                ArrayNode masks = (ArrayNode) aggregationDef.get("masks");
                if (masks != null) {
                    masks.forEach(mask -> {
                        ipRangeBuilder.addMaskRange(mask.asText());
                    });
                }
                ranges = (ArrayNode) aggregationDef.get("ranges");
                if (ranges != null) {
                    ranges.forEach(range -> {
                        String from = range.get("from") != null ? range.get("from").asText() : null;
                        String to = range.get("to") != null ? range.get("to").asText() : null;
                        if (from != null && to != null) {
                            ipRangeBuilder.addRange(from, to);
                        } else if (from != null && to == null) {
                            ipRangeBuilder.addUnboundedFrom(from);
                        } else if (from == null && to != null) {
                            ipRangeBuilder.addUnboundedTo(to);
                        }
                    });
                }
                break;
            case Nested:
                String path = aggregationDef.get("path").asText();
                tmpBuilder = AggregationBuilders.nested(aggName, path);
                break;
            case Cumulative:
                final SumAggregationBuilder aggregationBuilder = AggregationBuilders.sum(aggName).field(field);
                final CumulativeSumPipelineAggregationBuilder builder =
                    PipelineAggregatorBuilders.cumulativeSum("Cumulative_" + aggName, aggName);
                tmpBuilder = aggregationBuilder;
                parent.subAggregation(builder);
                break;
            case TopHit:
                final TopHitsAggregationBuilder topHitsAggregationBuilder = AggregationBuilders.topHits(aggName);
                topHitsAggregationBuilder.fetchSource(true).size(aggSize).sort(orderMetricName,
                    SortOrder.fromString(orderDirect));
                tmpBuilder = topHitsAggregationBuilder;
                break;
            case Derivative:
                DerivativePipelineAggregationBuilder derivativeBuilder =
                    new DerivativePipelineAggregationBuilder(Derivative + "_" + field, field);

                addCalcAggr(parent, field, calc);

                parent.subAggregation(derivativeBuilder);

                break;
            case SerialDiff:
                SerialDiffPipelineAggregationBuilder serialDiffBuilder =
                    new SerialDiffPipelineAggregationBuilder(SerialDiff + "_" + field, field);
                serialDiffBuilder.lag(Integer.valueOf(lag));

                addCalcAggr(parent, field, calc);

                parent.subAggregation(serialDiffBuilder);
                break;
            default:
                break;
        }

        if (aggregationDef.get("reverse") != null && aggregationDef.get("reverse").asBoolean()) {
            tmpBuilder = AggregationBuilders.reverseNested("reverse_nested").subAggregation(tmpBuilder);
        }

        if (aggregationDef.has("subAggs")) {
            JsonNode subAggs = aggregationDef.get("subAggs");
            if (subAggs instanceof ArrayNode) {
                for (JsonNode subAgg : (ArrayNode) subAggs) {
                    buildAggregation(tmpBuilder, subAgg);
                }
            } else {
                buildAggregation(tmpBuilder, subAggs);
            }

        }

        if (parent != null) {
            if (tmpBuilder != null) {
                parent.subAggregation(tmpBuilder);
            }
            return parent;
        }
        return tmpBuilder;

    }

    private void addCalcAggr(AggregationBuilder parent, String field, String calc) {
        if (calc == null || calc.equals("sum")) {
            SumAggregationBuilder sumBuilder = AggregationBuilders.sum(field).field(field);
            parent.subAggregation(sumBuilder);
        } else if (calc.equals("avg")) {
            AvgAggregationBuilder avgBuilder = AggregationBuilders.avg(field).field(field);
            parent.subAggregation(avgBuilder);
        } else if (calc.equals("min")) {
            MinAggregationBuilder minBuilder = AggregationBuilders.min(field).field(field);
            parent.subAggregation(minBuilder);
        } else if (calc.equals("max")) {
            MaxAggregationBuilder maxBuilder = AggregationBuilders.max(field).field(field);
            parent.subAggregation(maxBuilder);
        } else {
            SumAggregationBuilder sumBuilder = AggregationBuilders.sum(field).field(field);
            parent.subAggregation(sumBuilder);
        }
    }

    public List<AggregationBuilder> buildAggregation() throws Exception {
        List<AggregationBuilder> builders = new ArrayList<>();
        if (aggregations != null) {
            if (aggregations instanceof ArrayNode) {
                for (JsonNode agg : (ArrayNode) aggregations) {
                    builders.add(buildAggregation(null, agg));
                }
            } else {
                builders.add(buildAggregation(null, aggregations));
            }
        }
        return builders;
    }

    public QueryBuilder buildFilters() throws Exception {
        if (timeRangeFilter != null) {
            return QueryBuilders.boolQuery().must(buildFilters(null, filter)).must(timeRangeQuery());
        }
        if (filter != null) {
            return buildFilters(null, filter);
        }
        return null;
    }

}