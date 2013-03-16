package edu.stanford.scratch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TestRegex {

	public static void matches(String patternStr, String str) {
		Pattern pattern = Pattern.compile(patternStr);
		Matcher m = pattern.matcher(str);
		if (m.matches())
			System.out.println("Matches");
		else
			System.out.println("No match");
	}
	
	public static void find(String patternStr, String str) {
		Pattern pattern = Pattern.compile(patternStr);
		Matcher m = pattern.matcher(str);
		if (m.find())
			System.out.println("Matches");
		else
			System.out.println("No match");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//String pat = ".*\\.dmoz\\.";
		String pat = null;
		
/*		pat = ".*[.]{0,1}dmoz[.]{0,1}"; 
		TestRegex.matches(pat, "http://foo.bar");
		TestRegex.matches(pat, "http://foo.dmoz");
*/		
/*		pat = "[^xyz]*?the.*";
		TestRegex.matches(pat, "This is the content");
		TestRegex.matches(pat, "This x is the content");
*/
/*		pat = "(?s).*dmoz";
		TestRegex.matches(pat, "http://foo.dmoz.org/bar");
		TestRegex.matches(pat, "http://foo.bar.org/fum");
*/	
		//pat = "(?s)\\b(?!frankbeecostume).*";
		//pat = "(?s)\\b[/]*(?!frankbeecostume).*";
		//pat = "(?s)[/]*(?!frankbeecostume)[./]*";
		//pat = "(?s).*[./]{0,2}frankbeecostume[.]{0,1}.*";  // Works in reverse sense 
		pat = "(?s)(?!.*frankbeecostume).*"; 
		TestRegex.matches(pat, "http://foo.dmoz.org/bar");   // should match
		TestRegex.matches(pat, "frankbeecostume");           // should not match
		TestRegex.matches(pat, "GET /ystore/images/qty-input-bg.jpg HTTP/1.0\n");  // should match
		TestRegex.matches(pat, "GET /ystore/images/qty-input-bg.jpg HTTP/1.0\n" +
						   "User-Agent: Mozilla/5.0 (compatible; heritrix/3.1.0 +http://infolab.stanford.edu/~paepcke)\n" +
						   "Connection: close");             // should match

		TestRegex.matches(pat, "via: http://frankbeecostume.com.p10.hostingprod.com/ystore/css/style.css"); //should not match 
		
	}
}
