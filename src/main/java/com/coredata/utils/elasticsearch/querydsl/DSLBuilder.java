package com.coredata.utils.elasticsearch.querydsl;

import com.fasterxml.jackson.databind.node.ArrayNode;

public class DSLBuilder extends BaseDSLBuilder {

	private ArrayNode aggregations = mapper.createArrayNode();

	public static DSLBuilder instance() {
		return new DSLBuilder();
	}

	public DSLBuilder() {
		root.set("aggregations", aggregations);
	}

	public DSLBuilder aggregation(AggregationBuilder builder) {
		aggregations.add(builder.build());
		return this;
	}

}