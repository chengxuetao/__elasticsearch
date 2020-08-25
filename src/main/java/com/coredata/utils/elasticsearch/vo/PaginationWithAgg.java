package com.coredata.utils.elasticsearch.vo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PaginationWithAgg {

	private long usedMs;

	private long total;

	private int currentPage;

	private int pageSize;

	private List<Map<String, Object>> source = new ArrayList<>();
	private List<Map<String, Object>> agg = new LinkedList<Map<String, Object>>();

	public void appendData(Map<String, Object> data) {
		source.add(data);
	}

	public void appendAgg(Map<String, Object> data) {
		agg.add(data);
	}

	public long getUsedMs() {
		return usedMs;
	}

	public void setUsedMs(long usedMs) {
		this.usedMs = usedMs;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public int getCurrentPage() {
		return currentPage;
	}

	public void setCurrentPage(int currentPage) {
		this.currentPage = currentPage;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public List<Map<String, Object>> getSource() {
		return source;
	}

	public void setSource(List<Map<String, Object>> source) {
		this.source = source;
	}

	public List<Map<String, Object>> getAgg() {
		return agg;
	}

	public void setAgg(List<Map<String, Object>> agg) {
		this.agg = agg;
	}

}
