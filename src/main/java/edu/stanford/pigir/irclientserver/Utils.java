package edu.stanford.pigir.irclientserver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

public class Utils {
	
	public static String getPigResultAlias(String pigScriptPath) throws IOException {
		BufferedReader inReader = new BufferedReader(new FileReader(pigScriptPath));
		List<String> pigScript = IOUtils.readLines(inReader);
		Pattern pattern = Pattern.compile(".*#resultAlias[\\s]*([\\w]*)");
		for (int i=0; i<pigScript.size(); i++) {
			Matcher matcher = pattern.matcher(pigScript.get(i));
			if (matcher.find()) {
				String alias = matcher.group(1);
				return alias;
			}
		}
		return null;
	}
	
	/**
	 * Create a URI to the host that is running the current application.
	 * Append a context to that URI.
	 * Example, assuming host of currently running application is
	 * mono.stanford.edu: 
	 * 		getURI(8080, "responses" returns http://mono.stanford.edu:8080/responses
	 * @param port port to be used for the result URI
	 * @param contextStr Web context string that will be trailing the URI.
	 * @return finished URI
	 * @throws URISyntaxException
	 * @throws UnknownHostException
	 */
	public static URI getSelfURI(int port, String contextStr) throws URISyntaxException, UnknownHostException {
		String hostname = InetAddress.getLocalHost().getHostName();
		// Context string must have a leading slash:
		if ((contextStr != null) && (contextStr.length() != 0) && (! contextStr.startsWith("/"))) {
			contextStr = "/" + contextStr;
		}
		String hostWithDomain = InetAddress.getByName(hostname).getCanonicalHostName();
		URI res = new URI("http",
						  null, // no user into
						  hostWithDomain,
						  port,
						  contextStr,
						  null, // no query
						  null); // no fragment
		return res;
	}
}
