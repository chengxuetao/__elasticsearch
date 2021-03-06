package com.coredata.utils.elasticsearch.querydsl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AggregationBuilder {

    private ObjectMapper mapper = new ObjectMapper();

    private ObjectNode aggregation = mapper.createObjectNode();

    public static AggregationBuilder instance() {
        return new AggregationBuilder();
    }

    public AggregationBuilder distinctAggregation(String name, String field, boolean reverse) {
        aggregation.put("type", AggregationType.Distinct.name());
        aggregation.put("field", field);
        aggregation.put("name", name);
        aggregation.put("reverse", reverse);
        return this;
    }

    public AggregationBuilder distinctAggregation(String name, String field) {
        return distinctAggregation(name, field, false);
    }

    public AggregationBuilder termAggregation(String name, String field, int size, boolean reverse) {
        aggregation.put("type", AggregationType.Term.name());
        aggregation.put("field", field);
        aggregation.put("name", name);
        aggregation.put("size", size);
        aggregation.put("reverse", reverse);
        return this;
    }

    public AggregationBuilder termAggregation(String name, String field) {
        return termAggregation(name, field, 10, false);
    }

    public AggregationBuilder termAggregation(String name, String field, int size) {
        return termAggregation(name, field, size, false);
    }

    public AggregationBuilder dateHistogramAggregation(String name, String field, String interval, boolean reverse) {
        aggregation.put("type", AggregationType.DateHistogram.name());
        aggregation.put("field", field);
        aggregation.put("interval", interval);
        aggregation.put("name", name);
        aggregation.put("reverse", reverse);
        return this;
    }

    public AggregationBuilder dateHistogramAggregation(String name, String field, String interval) {
        return dateHistogramAggregation(name, field, interval, false);
    }

    public AggregationBuilder statsAggregation(String name, String field, boolean reverse) {
        aggregation.put("type", AggregationType.Stats.name());
        aggregation.put("field", field);
        aggregation.put("name", name);
        aggregation.put("reverse", reverse);
        return this;
    }

    public AggregationBuilder statsAggregation(String name, String field) {
        return statsAggregation(name, field, false);
    }

    public AggregationBuilder nestedAggregation(String name, String field, String path) {
        aggregation.put("type", AggregationType.Nested.name());
        aggregation.put("field", field);
        aggregation.put("name", name);
        aggregation.put("path", path);
        return this;
    }

    public IPRangeAggregation ipRangeAggregation(String name, String field) {
        return ipRangeAggregation(name, field, false);
    }

    public IPRangeAggregation ipRangeAggregation(String name, String field, boolean reverse) {
        return new IPRangeAggregation(this, name, field, reverse);
    }

    public RangeAggregation rangeAggregation(String name, String field, boolean reverse) {
        aggregation.put("name", name);
        return new RangeAggregation(this, name, field, reverse);
    }

    public RangeAggregation rangeAggregation(String name, String field) {
        return rangeAggregation(name, field, false);
    }

    public AggregationBuilder subAggs(AggregationBuilder subAggBuilder) {
        ArrayNode subAggs = (ArrayNode) aggregation.get("subAggs");
        if (subAggs == null) {
            subAggs = mapper.createArrayNode();
            aggregation.set("subAggs", subAggs);
        }
        subAggs.add(subAggBuilder.build());
        return this;
    }

    public JsonNode build() {
        return aggregation;
    }

    public class RangeAggregation {

        private AggregationBuilder parent;

        public RangeAggregation(AggregationBuilder parent, String name, String field, boolean reverse) {
            this.parent = parent;
            aggregation.put("type", AggregationType.Range.name());
            aggregation.put("field", field);
            aggregation.put("name", name);
            aggregation.put("reverse", reverse);
        }

        public RangeAggregation range(Double from, Double to) {
            ArrayNode ranges = (ArrayNode) aggregation.get("ranges");
            if (ranges == null) {
                ranges = mapper.createArrayNode();
                aggregation.set("ranges", ranges);
            }
            ObjectNode range = mapper.createObjectNode();
            if (from != null) {
                range.put("from", from);
            }
            if (to != null) {
                range.put("to", to);
            }
            ranges.add(range);
            return this;
        }

        public AggregationBuilder build() {
            return parent;
        }

    }

    public class IPRangeAggregation {

        private AggregationBuilder parent;

        public IPRangeAggregation(AggregationBuilder parent, String name, String field, boolean reverse) {
            this.parent = parent;
            aggregation.put("type", AggregationType.IPRange.name());
            aggregation.put("field", field);
            aggregation.put("name", name);
            aggregation.put("reverse", reverse);
        }

        public IPRangeAggregation masks(String... masks) {
            ArrayNode masksNode = (ArrayNode) aggregation.get("masks");
            if (masksNode == null) {
                masksNode = mapper.createArrayNode();
                aggregation.set("marks", masksNode);
            }
            for (String mask : masks) {
                masksNode.add(mask);
            }
            return this;
        }

        public IPRangeAggregation ipRange(String from, String to) {
            ArrayNode ranges = (ArrayNode) aggregation.get("ranges");
            if (ranges == null) {
                ranges = mapper.createArrayNode();
                aggregation.set("ranges", ranges);
            }
            ObjectNode range = mapper.createObjectNode();
            if (from != null && from.length() > 0) {
                range.put("from", from);
            }
            if (to != null && to.length() > 0) {
                range.put("to", to);
            }
            ranges.add(range);
            return this;
        }

        public AggregationBuilder build() {
            return parent;
        }

    }

}