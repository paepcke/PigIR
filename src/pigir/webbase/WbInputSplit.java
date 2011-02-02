package pigir.webbase;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.mapreduce.InputSplit;

public class WbInputSplit extends InputSplit {
	
	int numPages; 
	String startSite;
	String endSite; 
	URL distribDemonURL;
	
	public WbInputSplit(String theStartSite, String theEndSite, int theNumPages, URL theDistribDemonURL) {
		startSite = theStartSite;
		endSite   = theEndSite;
		numPages  = theNumPages;
		distribDemonURL = theDistribDemonURL; 
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return numPages;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return null;
	}
	
	public int getNumPages() {
		return numPages;
	}

	public String getStartSite() {
		return startSite;
	}

	public String getEndSite() {
		return endSite;
	}
	
	public String toString() {
		return startSite + "-" + endSite + ":" + numPages;
	}
}

