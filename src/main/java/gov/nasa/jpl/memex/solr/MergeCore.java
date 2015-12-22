package gov.nasa.jpl.memex.solr;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.ws.rs.NotSupportedException;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.SAXException;

import gov.nasa.jpl.memex.solr.model.BaseIndexField;
import gov.nasa.jpl.memex.solr.model.MergeCoreConfig;

/**
 * @author karanjeets
 */
public class MergeCore {

	private MergeCoreConfig config;
	private Map<Class<?>, Object> objectMap;
	private static final String CONFIG_FILE = "merge-core-config.xml";
	private static final String DEFAULT_QUERY = "*:*";
	private static final Integer DEFAULT_START = 0;
	private static final Integer DEFAULT_ROWS = 10;
	private static final Integer DEFAULT_BUFFER = 100;
	
	// Initializing Configuration
	public MergeCore(InputStream configStream) {
		ConfigParser parser = new ConfigParser();
		try {
			config = parser.parseConfig(configStream);
			if(config.getQuery() == null)
				config.setQuery(DEFAULT_QUERY);
			if(config.getStart() == null)
				config.setStart(DEFAULT_START);
			if(config.getRows() == null)
				config.setRows(DEFAULT_ROWS);
			if(config.getBuffer() == null)
				config.setBuffer(DEFAULT_BUFFER);
			objectMap = new HashMap<Class<?>, Object>();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 *  Calls custom handler without parameters to perform operation(s) on the field before passing it to the Solr
	 * @param classNameStr
	 * @param handlerName
	 * @return Object value to store in Solr
	 */
	public Object callHandler(String classNameStr, String handlerName)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
			SecurityException, IllegalArgumentException, InvocationTargetException {
		return callHandler(classNameStr, handlerName, null, null);
	}
	
	/**
	 * Calls custom handler with parameters to perform operation(s) on the field before passing it to the Solr
	 * @param classNameStr
	 * @param handlerName
	 * @param params
	 * @param doc
	 * @return Object value to store in Solr
	 */
	public Object callHandler(String classNameStr, String handlerName, String[] params, SolrDocument doc)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
			SecurityException, IllegalArgumentException, InvocationTargetException {
		
		Object[] paramValues = null;
		Class<?>[] objTypes = null;
		
		if(params != null) {
			paramValues = new Object[params.length];
			objTypes = new Class<?>[params.length];
			int j = 0;
			for(String param : params) {
				paramValues[j] = doc.get(param);
				objTypes[j] = Object.class;
				j++;
			}
		}
		
		Class<?> className = Class.forName(classNameStr);
		Object obj = null;
		if(objectMap.containsKey(className))
			obj = objectMap.get(className);
		else {
			obj = className.newInstance();
			objectMap.put(className, obj);
		}
		Method handler = obj.getClass().getMethod(handlerName, objTypes);
		return handler.invoke(obj, paramValues);
	}
	
	/**
	 * Merge the two cores subject to the defined mapping
	 */
	public void merge() {
		
		// Initialize Solr
		SolrServer baseIndex = new HttpSolrServer(config.getBaseIndex());
		SolrServer mergeIndex = new HttpSolrServer(config.getMergeIndex());
		Integer start = config.getStart();
		Integer counter = 0;
		SolrQuery query = new SolrQuery();
		query.setQuery(config.getQuery());
		
		// Iterating over documents
		while(true) {
			query.setStart(start);
			query.setRows(config.getBuffer());
			
			try {
				QueryResponse response = mergeIndex.query(query);
				SolrDocumentList docs = response.getResults();
				Map<String, BaseIndexField> fieldMap = config.getFieldMap();
				
				for(int i = 0; i < docs.size(); i++) {
					
					// Check if the rows to be processed is in range
					if(counter == config.getRows())
						break;
					
					SolrDocument doc = docs.get(i);
					SolrInputDocument input = new SolrInputDocument();
					
					// Iterating over each field of the document
					for(String field : doc.getFieldNames()) {
						if(fieldMap.containsKey(field)) {
							BaseIndexField baseIndexField = fieldMap.get(field);
							Object value = null;
							if(baseIndexField.getClassName() == null) {
								value = doc.get(field).toString().trim().length() == 0 ? null : doc.get(field);
							}
							else {
								if(baseIndexField.getParams() == null)
									value = callHandler(baseIndexField.getClassName(), baseIndexField.getHandler());
								else
									value = callHandler(baseIndexField.getClassName(), baseIndexField.getHandler(), baseIndexField.getParams(), doc);
							}
							
							if(value == null)
								continue;
							
							input.addField(baseIndexField.getName(), value);
						}
						else {
							// Processing dynamic fields
							for(Pattern dynamicField : config.getDynamicFields()) {
								if(dynamicField.matcher(field).matches())
									input.addField(field, doc.get(field));
							}
						}
					}
					
					// Verifying and processing the required fields 
					if(config.getRequiredFields() != null && config.getRequiredFields().size() > 0) {
						for(String required : config.getRequiredFields()) {
							if(!input.containsKey(required)) {
								BaseIndexField baseIndexField = fieldMap.get(required);
								Object value = null;
								if(baseIndexField.getClassName() != null) 
									value = callHandler(baseIndexField.getClassName(), baseIndexField.getHandler(), baseIndexField.getParams(), doc);
								else
									throw new NotSupportedException("Invaid Configuration File. No handler is defined for field '" + required + "' and is not present in the document.");
								input.addField(required, value);
							}
						}
					}
					
					// Ready to add it into Solr
					if(!input.isEmpty())
						baseIndex.add(input);
					counter++;
					System.out.println(counter + " documents processed");
					System.out.println("=================");
				}
				
				// Solr commit
				baseIndex.commit();
			}
			catch(SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} 
			
			// Check if the rows to be processed is in range
			if(counter >= config.getRows())
				break;
			start += config.getBuffer();
		}
	}
	
	public static void main(String []args) {
		MergeCore mergeIndex = new MergeCore(MergeCore.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
		mergeIndex.merge();
	}
	
}
