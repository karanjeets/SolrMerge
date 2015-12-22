package gov.nasa.jpl.memex.solr;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import gov.nasa.jpl.memex.solr.model.BaseIndexField;
import gov.nasa.jpl.memex.solr.model.MergeCoreConfig;

/**
 * @author karanjeets
 */
public class ConfigParser {

	// Define Tags and Attributes
	private static final String INIT_TAG = "init";
	private static final String BASE_INDEX_TAG = "base-index";
	private static final String MERGE_INDEX_TAG = "merge-index";
	private static final String QUERY_TAG = "query";
	private static final String START_TAG = "start";
	private static final String ROWS_TAG = "rows";
	private static final String BUFFER_TAG = "buffer";
	private static final String MAPPING_TAG = "mapping";
	private static final String FIELD_TAG = "field";
	private static final String DYNAMIC_FIELD_TAG = "dynamicField";
	private static final String REQUIRED_TAG = "required";
	private static final String BASE_ATTR = "base";
	private static final String MERGE_ATTR = "merge";
	private static final String HANDLER_ATTR = "handler";
	private static final String CLASS_ATTR = "class";
	private static final String PARAMS_ATTR = "params";
	private static final String ATTR_SEPARATOR = ",";
	
	/**
	 * Parses configuration file and records how fields are mapped between cores
	 * @param configStream
	 * @return An object of MergeCoreConfig which contains structured mapping of cores
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	public MergeCoreConfig parseConfig(InputStream configStream) throws ParserConfigurationException, SAXException, IOException {
		
		MergeCoreConfig config;
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = dbFactory.newDocumentBuilder();
		Document doc = docBuilder.parse(configStream);
		
		doc.getDocumentElement().normalize();
		
		// Parsing INIT_TAG
		config = new MergeCoreConfig();
		NodeList init = doc.getElementsByTagName(INIT_TAG);
		if(init == null || init.getLength() == 0)
			throw new ParserConfigurationException("Invaid Configuration File. Incorrect Initialization.");
		NodeList initValues = init.item(0).getChildNodes();
		for(int i = 0; i < initValues.getLength(); i++) {
			Node node = initValues.item(i);
			if(node.getNodeType() == Node.ELEMENT_NODE) {
				switch(node.getNodeName()) {
					case BASE_INDEX_TAG	:	config.setBaseIndex(node.getTextContent());
										 	break;
					case MERGE_INDEX_TAG:	config.setMergeIndex(node.getTextContent());
										 	break;
					case QUERY_TAG		:	config.setQuery(node.getTextContent());
				 							break;
					case START_TAG		:	config.setStart(Integer.parseInt(node.getTextContent()));
											break;
					case ROWS_TAG		:	config.setRows(Integer.parseInt(node.getTextContent()));
				 							break;
					case BUFFER_TAG		:	config.setBuffer(Integer.parseInt(node.getTextContent()));
											break;
					default				:	throw new ParserConfigurationException("Invaid Configuration File. Incorrect Initialization.");
				}
			}
		}
		
		// Parsing MAPPING_TAG
		NodeList mapping = doc.getElementsByTagName(MAPPING_TAG);
		if(init == null || init.getLength() == 0 || init.item(0).getChildNodes().getLength() < 1)
			throw new ParserConfigurationException("Invaid Configuration File. Incorrect Mapping.");

		// Parsing FIELD_TAG
		Map<String, BaseIndexField> fieldMapping = new HashMap<String, BaseIndexField>();
		NodeList fieldValues = ((Element)mapping.item(0)).getElementsByTagName(FIELD_TAG);
		for(int i = 0; i < fieldValues.getLength(); i++) {
			NamedNodeMap attr = fieldValues.item(i).getAttributes();
			if(attr.getNamedItem(MERGE_ATTR) == null)
				continue;
			BaseIndexField field = new BaseIndexField();
			if(attr.getNamedItem(BASE_ATTR) != null)
				field.setName(attr.getNamedItem(BASE_ATTR).getNodeValue());
			if(attr.getNamedItem(CLASS_ATTR) != null && attr.getNamedItem(HANDLER_ATTR) != null) {
				field.setClassName(attr.getNamedItem(CLASS_ATTR).getNodeValue());
				field.setHandler(attr.getNamedItem(HANDLER_ATTR).getNodeValue());
				if(attr.getNamedItem(PARAMS_ATTR) != null)
					field.setParams(attr.getNamedItem(PARAMS_ATTR).getNodeValue().split(ATTR_SEPARATOR));
			}
			fieldMapping.put(attr.getNamedItem(MERGE_ATTR).getNodeValue(), field);
		}
		config.setFieldMap(fieldMapping);
		
		// Parsing Dynamic Fields - DYNAMIC_FIELD_TAG
		List<Pattern> dynamicFields = new ArrayList<Pattern>();
		NodeList dynamicFieldValues = ((Element)mapping.item(0)).getElementsByTagName(DYNAMIC_FIELD_TAG);
		for(int i = 0; i < dynamicFieldValues.getLength(); i++) {
			NamedNodeMap attr = dynamicFieldValues.item(i).getAttributes();
			if(attr.getNamedItem(MERGE_ATTR) != null)
				dynamicFields.add(Pattern.compile(attr.getNamedItem(MERGE_ATTR).getNodeValue()));
		}
		config.setDynamicFields(dynamicFields);
		
		// Parsing REQUIRED_TAG - List of required fields
		List<String> requiredFields = new ArrayList<String>();
		NodeList requiredFieldValues = ((Element)mapping.item(0)).getElementsByTagName(REQUIRED_TAG);
		for(int i = 0; i < requiredFieldValues.getLength(); i++) {
			NamedNodeMap attr = requiredFieldValues.item(i).getAttributes();
			if(attr.getNamedItem(MERGE_ATTR) != null) {
				String field = attr.getNamedItem(MERGE_ATTR).getNodeValue();
				if(field.trim().length() == 0 || !fieldMapping.containsKey(field))
					throw new ParserConfigurationException("Invaid Configuration File. Incorrect Mapping. Required field '" + field + "' is not defined.");
				requiredFields.add(attr.getNamedItem(MERGE_ATTR).getNodeValue());
			}
		}
		config.setRequiredFields(requiredFields);
		
		return config;
	}
	
}
