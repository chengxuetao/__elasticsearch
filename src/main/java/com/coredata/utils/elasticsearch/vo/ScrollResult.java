package com.coredata.utils.elasticsearch.vo;

public class ScrollResult {

	private String scrollId;

	private CommResult data = new CommResult();;

	public String getScrollId() {
		return scrollId;
	}

	public void setScrollId(String scrollId) {
		this.scrollId = scrollId;
	}

	public CommResult getData() {
		return data;
	}

	public void setData(CommResult data) {
		this.data = data;
	}

}
