package com.coredata.utils.elasticsearch.querydsl;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

public class TimeRange {

	private long startMs;

	private long endMs;

	private String timeField = "createdTime";

	private DateHistogramInterval interval;

	public TimeRange() {

	}

	public TimeRange(long startMs, long endMs) {
		this(startMs, endMs, null);
	}

	public TimeRange(long startMs, long endMs, DateHistogramInterval interval) {
		this.startMs = startMs;
		this.endMs = endMs;
		this.interval = interval;
	}

	public long getStartMs() {
		return startMs;
	}

	public void setStartMs(long startMs) {
		this.startMs = startMs;
	}

	public long getEndMs() {
		return endMs;
	}

	public void setEndMs(long endMs) {
		this.endMs = endMs;
	}

	public String getTimeField() {
		return timeField;
	}

	public DateHistogramInterval getInterval() {
		return interval;
	}

	public void setInterval(DateHistogramInterval interval) {
		this.interval = interval;
	}

	public void setTimeField(String timeField) {
		this.timeField = timeField;
	}

}
