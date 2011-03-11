package htmlwrangling;

import java.io.IOException;
import java.io.FileReader;
import java.io.Reader;
import java.io.BufferedReader;
import org.jsoup.Jsoup;


public class HTMLTextExtractor {

	public HTMLTextExtractor() {}
	
	public static String extractText(Reader reader) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(reader);
		String line;
		while ( (line=br.readLine()) != null) {
			sb.append(line);
		}
		String textOnly = Jsoup.parse(sb.toString()).text();
		return textOnly;
	}

  public final static void main(String[] args) throws Exception{
	  //FileReader reader = new FileReader("E:/Users/Paepcke/tmp/stateDepartmente.htm");
	  FileReader reader = new FileReader("E:/Users/Paepcke/tmp/gov-12-2010-text.pages");
	  System.out.println(HTMLTextExtractor.extractText(reader));
	  System.out.println("Done.");
  }
}
