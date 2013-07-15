package edu.stanford.pigir.irclientserver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

public class Utils {
	
	public static String getPigResultAlias(String pigScriptPath) throws IOException {
		BufferedReader inReader = new BufferedReader(new FileReader(pigScriptPath));
		List<String> pigScript = IOUtils.readLines(inReader);
		//Pattern pattern = Pattern.compile("([^-]2)([a-zA-Z0-9]+)=");
		Pattern pattern = Pattern.compile("([a-zA-Z0-9]+)[\\s]*=");
		for (int i=pigScript.size()-1; i>=0; i--) {
			Matcher matcher = pattern.matcher(pigScript.get(i));
			if (matcher.find()) {
				String alias = matcher.group(1);
				return alias;
			}
		}
		return null;
	}
}
