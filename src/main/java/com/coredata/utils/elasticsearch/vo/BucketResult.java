package com.coredata.utils.elasticsearch.vo;

import java.util.ArrayList;
import java.util.List;

public class BucketResult<T> {

	private T key;
	private String keyAsString;
	private long docCount;
	private double avg;

	@SuppressWarnings("rawtypes")
	private List<BucketResult> subBucket = new ArrayList<>();

	public BucketResult(T key, String keyAsString, long docCount) {
		this.key = key;
		this.keyAsString = keyAsString;
		this.docCount = docCount;
	}

	@SuppressWarnings("rawtypes")
	public void appendSubBucket(BucketResult result) {
		subBucket.add(result);
	}

	public T getKey() {
		return key;
	}

	public String getKeyAsString() {
		return keyAsString;
	}

	public long getDocCount() {
		return docCount;
	}

	@SuppressWarnings("rawtypes")
	public List<BucketResult> getSubBucket() {
		return subBucket;
	}

	@SuppressWarnings("rawtypes")
	public void setSubBucket(List<BucketResult> subBucket) {
		this.subBucket = subBucket;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

}
