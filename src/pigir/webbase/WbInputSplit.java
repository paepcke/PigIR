package pigir.webbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class WbInputSplit extends InputSplit implements Writable {
	
	private int numPages; 
	private String startSite;
	private String endSite; 
	private DistributorContact distribDemonContact;
	
	public WbInputSplit(String theStartSite, 
						String theEndSite, 
						int theNumPages, 
						DistributorContact theDistribDemonContact) {
		startSite = theStartSite;
		endSite   = theEndSite;
		numPages  = theNumPages;
		distribDemonContact = theDistribDemonContact; 
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
	
	public DistributorContact getDistributorDemonContact () {
		return distribDemonContact;
	}
	
	public String toString() {
		return "WbInputSplit<" + startSite + "->" + endSite + ":" + numPages + ">";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, startSite);
		Text.writeString(out, endSite);
		distribDemonContact.write(out);
		out.writeInt(numPages);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		startSite = Text.readString(in);
		endSite = Text.readString(in);
		distribDemonContact = new DistributorContact();
		distribDemonContact.readFields(in);
		numPages = in.readInt();
	}
}

