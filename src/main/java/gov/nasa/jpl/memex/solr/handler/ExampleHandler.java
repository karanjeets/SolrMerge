package gov.nasa.jpl.memex.solr.handler;

/**
 * @author karanjeets
 */
public class ExampleHandler {

	/**
	 * An example handler method which takes one parameter
	 * @param id
	 * @return
	 */
	public Object extractContentType(Object id) {
		// Do some operations and return value
		return id;
	}
	
	/**
	 * An example handler method which takes two parameter
	 * @param id
	 * @param host
	 * @return
	 */
	public Object extractImageLinks(Object id, Object host) {
		// Do some operations and return value
		return id.toString() + "-----" + host.toString();
	}
	
}
