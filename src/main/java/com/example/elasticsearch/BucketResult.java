package com.example.elasticsearch;

import java.util.ArrayList;
import java.util.List;

public class BucketResult<T> {

	private T key;
	private String keyAsString;
	private long docCount;
	private double avg;

	private List<BucketResult> subBucket = new ArrayList<BucketResult>();

	public BucketResult(T key, String keyAsString, long docCount) {
		this.key = key;
		this.keyAsString = keyAsString;
		this.docCount = docCount;
	}

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

	public List<BucketResult> getSubBucket() {
		return subBucket;
	}

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
