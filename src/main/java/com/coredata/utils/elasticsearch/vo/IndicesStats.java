package com.coredata.utils.elasticsearch.vo;

import java.io.Serializable;

/**
 * 索引状态对象，用于组装请求返回的索引状态
 * @author sushi
 *
 */
public class IndicesStats implements Serializable {

	private static final long serialVersionUID = 5408489616980168264L;

	/**
	 * 索引大小，单位byte
	 */
	private long size = 0L;

	public IndicesStats() {

	}

	public IndicesStats(long size) {
		this.size = size;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

}
