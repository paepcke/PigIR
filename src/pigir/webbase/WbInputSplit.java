package pigir.webbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class WbInputSplit extends InputSplit implements Writable {
	
	private int numPages; 
	private String startSite;
	private String endSite; 
	private URL distribDemonURL;
	
	public WbInputSplit(String theStartSite, String theEndSite, int theNumPages, URL theDistribDemonURL) {
		startSite = theStartSite;
		endSite   = theEndSite;
		numPages  = theNumPages;
		distribDemonURL = theDistribDemonURL; 
	}
	
	public WbInputSplit() {
		// Needed for creating a split on the worker nodes
		// via serialization. The readFields() method will 
		// be called during this process to initialize the
		// instance variables.
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return numPages;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[]{};
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
	
	public URL getDistributorDemonURL () {
		return distribDemonURL;
	}
	
	public String toString() {
		return startSite + "-" + endSite + ":" + numPages;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, startSite);
		Text.writeString(out, endSite);
		Text.writeString(out, distribDemonURL.toString());
		out.writeInt(numPages);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		startSite = Text.readString(in);
		endSite = Text.readString(in);
		distribDemonURL = new URL(Text.readString(in));
		numPages = in.readInt();
	}
}

