package KMeans;

import java.util.HashMap;
import java.util.Map;

public class KMeansUtils {
	
	private static Map<String, String> xmlMap;
	private static String[] tokens;
	private static String key, val;
	
	public static Map<String, String> convertFromXMLtoString(String xmlData) {
		xmlMap = new HashMap<String, String>();
		try {
			tokens = xmlData.trim().substring(5, xmlData.trim().length() - 3)
					.split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				key = tokens[i].trim();
				val = tokens[i + 1];
				xmlMap.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xmlData);
		}

		return xmlMap;
	}
}
