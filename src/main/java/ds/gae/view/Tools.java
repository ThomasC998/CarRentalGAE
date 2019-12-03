package ds.gae.view;

import java.text.SimpleDateFormat;

public final class Tools {
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy");
	
	/**
	 * Converts plain text to HTML-encoded text
	 * 
	 * @param plainText Input string
	 * @return HTML-encoded text
	 */
	public static String encodeHTML(String plainText) {
	    StringBuffer out = new StringBuffer();
	    for(int i=0; i<plainText.length(); i++) {
	        char c = plainText.charAt(i);
	        if(c > 127 || c=='"' || c=='<' || c=='>') {
	           out.append("&#"+(int)c+";");
	        }
	        else {
	            out.append(c);
	        }
	    }
	    return out.toString().replaceAll("(\r\n|\n)", "<br />");
	}
}
