package gov.nasa.jpl.memex.solr.model;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author karanjeets
 */
public class MergeCoreConfig {
	
	private String baseIndex;
	private String mergeIndex;
	private String query;
	private Integer start;
	private Integer rows;
	private Integer buffer;
	private Map<String, BaseIndexField> fieldMap;
	private List<Pattern> dynamicFields;
	private List<String> requiredFields;
	
	public String getBaseIndex() {
		return baseIndex;
	}
	public void setBaseIndex(String baseIndex) {
		this.baseIndex = baseIndex;
	}
	public String getMergeIndex() {
		return mergeIndex;
	}
	public void setMergeIndex(String mergeIndex) {
		this.mergeIndex = mergeIndex;
	}
	
	public Map<String, BaseIndexField> getFieldMap() {
		return fieldMap;
	}
	public void setFieldMap(Map<String, BaseIndexField> fieldMap) {
		this.fieldMap = fieldMap;
	}
	public List<Pattern> getDynamicFields() {
		return dynamicFields;
	}
	public void setDynamicFields(List<Pattern> dynamicFields) {
		this.dynamicFields = dynamicFields;
	}
	public List<String> getRequiredFields() {
		return requiredFields;
	}
	public void setRequiredFields(List<String> requiredFields) {
		this.requiredFields = requiredFields;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public Integer getRows() {
		return rows;
	}
	public void setRows(Integer rows) {
		this.rows = rows;
	}
	public Integer getBuffer() {
		return buffer;
	}
	public void setBuffer(Integer buffer) {
		this.buffer = buffer;
	}
	public Integer getStart() {
		return start;
	}
	public void setStart(Integer start) {
		this.start = start;
	}
	
	@Override
	public String toString() {
		return "MergeCoreConfig [baseIndex=" + baseIndex + ", mergeIndex=" + mergeIndex + ", query=" + query
				+ ", start=" + start + ", rows=" + rows + ", buffer=" + buffer + ", fieldMap=" + fieldMap
				+ ", dynamicFields=" + dynamicFields + ", requiredFields=" + requiredFields + "]";
	}

}
