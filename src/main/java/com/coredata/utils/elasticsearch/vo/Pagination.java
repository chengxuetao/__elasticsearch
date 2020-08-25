package com.coredata.utils.elasticsearch.vo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Pagination {

	private long usedMs;

	private long total;

	private int currentPage;

	private int pageSize;

	private List<Map<String, Object>> source = new ArrayList<>();

	public void appendData(Map<String, Object> data) {
		source.add(data);
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

}
