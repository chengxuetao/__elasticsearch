package com.coredata.utils.elasticsearch.vo;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({ "type", "displayIndex" })
public class TSDBMetric implements Serializable {

	private static final long serialVersionUID = 4121635058783267662L;

	private String metric;

	private String customerId;

	private String instId;

	private String instName;

	private String ip;

	private String nodeId;

	private String modelId;

	private Double val;

	private String stringVal;

	private Boolean boolVal;

	/**
	 * 保存JSON格式字符串
	 */
	private Object objVal;

	private long taskTime;

	private long collFinishTime;

	private long miningFinishTime;

	/**
	 * 指标类型 Realtime(实时采集，无须存储)
	 */
	private String type;

	/**
	 *  对应决策模型
	 */
	private String decisionModel;

	private String rollupId;

	private String resfullType;

	private String resfullTypeName;

	/**
	* 国家
	*/
	private String nation;

	/**
	 * 省
	 */
	private String province;

	/**
	 * 市
	 */
	private String city;

	/**
	 * 区
	 */
	private String district;

	/**
	 * 数据转实体模型id
	 */
	private String entityModelId;

	private String metricName;

	private String indexId;

	public TSDBMetric() {

	}

	public TSDBMetric(String customerId, String instId, String nodeId, String modelId, String metric, String stringVal, Double val, Boolean boolVal,
			Object objVal, long taskTime, long collFinishTime, long miningFinishTime, String type) {
		this.customerId = customerId;
		this.nodeId = nodeId;
		this.instId = instId;
		this.metric = metric;
		this.modelId = modelId;
		this.val = val;
		this.stringVal = stringVal;
		this.taskTime = taskTime;
		this.collFinishTime = collFinishTime;
		this.miningFinishTime = miningFinishTime;
		this.boolVal = boolVal;
		this.objVal = objVal;
		this.type = type;
		this.rollupId = this.customerId + "_" + this.instId + "_" + this.metric;
	}

	public TSDBMetric(String customerId, String instId, String nodeId, String modelId, String metric, String stringVal, Double val, Boolean boolVal,
			Object objVal, long taskTime, long collFinishTime, long miningFinishTime, String type, String metricName) {
		this.customerId = customerId;
		this.nodeId = nodeId;
		this.instId = instId;
		this.metric = metric;
		this.modelId = modelId;
		this.val = val;
		this.stringVal = stringVal;
		this.taskTime = taskTime;
		this.collFinishTime = collFinishTime;
		this.miningFinishTime = miningFinishTime;
		this.boolVal = boolVal;
		this.objVal = objVal;
		this.type = type;
		this.metricName = metricName;
		this.rollupId = this.customerId + "_" + this.instId + "_" + this.metric;
	}

	public Double getVal() {
		return val;
	}

	public void setVal(Double val) {
		this.val = val;
	}

	public String getStringVal() {
		return stringVal;
	}

	public void setStringVal(String stringVal) {
		this.stringVal = stringVal;
	}

	public long getTaskTime() {
		return taskTime;
	}

	public void setTaskTime(long taskTime) {
		this.taskTime = taskTime;
	}

	public long getCollFinishTime() {
		return collFinishTime;
	}

	public void setCollFinishTime(long collFinishTime) {
		this.collFinishTime = collFinishTime;
	}

	public long getMiningFinishTime() {
		return miningFinishTime;
	}

	public void setMiningFinishTime(long miningFinishTime) {
		this.miningFinishTime = miningFinishTime;
	}

	public Boolean getBoolVal() {
		return boolVal;
	}

	public void setBoolVal(Boolean boolVal) {
		this.boolVal = boolVal;
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getInstId() {
		return instId;
	}

	public void setInstId(String instId) {
		this.instId = instId;
	}

	public String getModelId() {
		return modelId;
	}

	public void setModelId(String modelId) {
		this.modelId = modelId;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public String toString() {
		return "TSDBMetric [metric=" + metric + ", instId=" + instId + ", nodeId=" + nodeId + ", modelId=" + modelId + ", val=" + val + ", stringVal="
				+ stringVal + ", boolVal=" + boolVal + ", taskTime=" + taskTime + ", collFinishTime=" + collFinishTime + ", miningFinishTime="
				+ miningFinishTime + ", rollupId=" + rollupId + "]";
	}

	public String getInstName() {
		return instName;
	}

	public void setInstName(String instName) {
		this.instName = instName;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Object getObjVal() {
		return objVal;
	}

	public void setObjVal(Object objVal) {
		this.objVal = objVal;
	}

	public String getDecisionModel() {
		return decisionModel;
	}

	public void setDecisionModel(String decisionModel) {
		this.decisionModel = decisionModel;
	}

	public String getRollupId() {
		return rollupId;
	}

	public void setRollupId(String rollupId) {
		this.rollupId = this.customerId + "_" + this.instId + "_" + this.metric;
	}

	public String getResfullType() {
		return resfullType;
	}

	public void setResfullType(String resfullType) {
		this.resfullType = resfullType;
	}

	public String getNation() {
		return nation;
	}

	public void setNation(String nation) {
		this.nation = nation;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public String getEntityModelId() {
		return entityModelId;
	}

	public void setEntityModelId(String entityModelId) {
		this.entityModelId = entityModelId;
	}

	public String getResfullTypeName() {
		return resfullTypeName;
	}

	public void setResfullTypeName(String resfullTypeName) {
		this.resfullTypeName = resfullTypeName;
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public String getIndexId() {
		return indexId;
	}

	public void setIndexId(String indexId) {
		this.indexId = indexId;
	}
}