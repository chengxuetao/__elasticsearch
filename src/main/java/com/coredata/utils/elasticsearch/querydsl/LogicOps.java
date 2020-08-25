package com.coredata.utils.elasticsearch.querydsl;

public enum LogicOps {

	and("&"), or("|");

	private final String name;

	private LogicOps(String s) {
		name = s;
	}

	public boolean equalsName(String otherName) { 
		return name.equals(otherName);
	}

	public String toString() {
		return this.name;
	}

}
