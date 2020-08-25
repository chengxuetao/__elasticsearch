package com.coredata.utils.elasticsearch.vo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketResults<T> {

	private long usedMs;

	private long total;

	private List<Map<String, Object>> source = new ArrayList<>();

	private List<BucketResult<T>> result = new ArrayList<>();
	
	private Map<String, Object> pieResult = new HashMap<String, Object>();
	public BucketResults() {

	}

	public void append(Map<String, Object> result) {
		source.add(result);
	}

	public void append(BucketResult<T> result) {
		this.result.add(result);
	}

	public void append(String key, Object value) {
		this.pieResult.put(key, value);
	}

	public long getUsedMs() {
		return usedMs;
	}

	public void setUsedMs(long usedMs) {
		this.usedMs = usedMs;
	}

	public List<BucketResult<T>> getResult() {
		return result;
	}

	public void setResult(List<BucketResult<T>> result) {
		this.result = result;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public List<Map<String, Object>> getSource() {
		return source;
	}

	public void setSource(List<Map<String, Object>> source) {
		this.source = source;
	}

	public Map<String, Object> getPieResult() {
		return pieResult;
	}

}
