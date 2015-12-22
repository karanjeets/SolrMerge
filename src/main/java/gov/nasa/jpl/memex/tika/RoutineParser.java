package gov.nasa.jpl.memex.tika;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.xml.sax.SAXException;

/**
 * Routine parser to extract information using Apache Tika;
 * Some example methods are defined already
 * @author karanjeets
 *
 */
public class RoutineParser {
	
	Parser parser;
	
	public RoutineParser() {
		
	}
	
	/**
	 * To extract image links form a URL. Needs Improvement
	 * @param url
	 * @return
	 * @throws IOException
	 * @throws SAXException
	 * @throws TikaException
	 */
	public Object extractImageLinks(String url) throws IOException, SAXException, TikaException {
		Set<String> imageLinks = new HashSet<String>();
		InputStream is = null;
		try {
			is = TikaInputStream.get(new URL(url).openStream());
			Metadata metadata = new Metadata();
			LinkContentHandler handler = new LinkContentHandler();
			AutoDetectParser parser = new AutoDetectParser();
			parser.parse(is, handler, metadata);
			List<Link> links = handler.getLinks();
			Iterator<Link> iter = links.iterator();
			while(iter.hasNext()) {
				Link link = iter.next();
				if(link.isImage())
					imageLinks.add(link.getUri());
			}
		}
		finally {
			is.close();
		}
		return imageLinks.toArray();
	}
	
	/**
	 * To extract Metadata form a URL. Needs Improvement
	 * @param url
	 * @return
	 * @throws IOException
	 * @throws SAXException
	 * @throws TikaException
	 */
	public Metadata extractMetadata(String url) throws IOException, SAXException, TikaException {
		InputStream is = null;
		Metadata metadata = null;
		try {
			is = TikaInputStream.get(new URL(url).openStream());
			metadata = new Metadata();
			BodyContentHandler handler = new BodyContentHandler();
			AutoDetectParser parser = new AutoDetectParser();
			parser.parse(is, handler, metadata);
		}
		finally {
			is.close();
		}
		return metadata;
	}
	
	public static void main(String []args) throws IOException, SAXException, TikaException {
		RoutineParser p = new RoutineParser();
		System.out.println(p.extractMetadata("http://www.google.com/"));
	}
	
}
