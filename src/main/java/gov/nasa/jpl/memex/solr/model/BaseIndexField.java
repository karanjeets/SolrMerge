package gov.nasa.jpl.memex.solr.model;

import java.util.Arrays;

/**
 * @author karanjeets
 */
public class BaseIndexField {
	
	private String name;
	private String className;
	private String handler;
	private String[] params;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public String[] getParams() {
		return params;
	}
	public void setParams(String[] params) {
		this.params = params;
	}
	
	@Override
	public String toString() {
		return "BaseIndexField [name=" + name + ", className=" + className + ", handler=" + handler + ", params="
				+ Arrays.toString(params) + "]";
	}
	
}
