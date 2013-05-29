/**
 * 
 */
package edu.stanford.pigir;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * @author paepcke
 *
 */
public class TextualHTMLLinksRemover {
	
	public static String removeLinks (String str) {
		return Common.removeLinks(str);
	}
	
	public static void removeLinks (File inFile, File outFile) throws IOException {
		String input= FileUtils.readFileToString(inFile, "UTF-8");
		String newStr = Common.removeLinks(input);
		FileUtils.writeStringToFile(outFile, newStr, "UTF-8");
	}
	
	public static void main(String[] args) throws IOException {
		final String usage = "Usage: java -jar textualHTMLLinksRemover.jar edu.stanford.pigir.TextualHTMLLinksRemover <inFilePath> <outFilePath>";
		if (args.length != 3) {
			System.out.println(usage);
			System.exit(1);
		}
		File inFile = new File(args[1]);
		File outFile = new File(args[2]);
		TextualHTMLLinksRemover.removeLinks(inFile, outFile);
	}
}
