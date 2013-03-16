package edu.stanford.pigir.warc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * @author paepcke
 * 
 * Stores WARC tuples into files. The tuples are assumed to have
 * the schema:

 *    (WARC_RECORD_ID:chararray, 
 *     CONTENT_LENGTH:int, 
 *     WARC_DATE:chararray, 
 *     WARC_TYPE, 
 *     {(<headerFldName>, <headerFldVal>)*}, 
 *     CONTENT:chararray),
 *     
 *  which is what WarcLoader produces.
 *  
 *     WARC/1.0
 *     WARC-Type: response
 *     WARC-Target-URI: http://www.battelle.org/Sys.WebForms.PageRequestManagerServerErrorException
 *     WARC-Date: 2012-12-14T12:46:05Z
 *     WARC-Payload-Digest: sha1:ZEZQFDVBQ6VK373MPDN532NDABRWDJPN
 *     WARC-IP-Address: 96.45.114.74
 *     WARC-Record-ID: <urn:uuid:9170e03a-7374-4ef9-a46d-fc985a2a2ffe>
 *     Content-Type: application/http; msgtype=response
 *     Content-Length: 37445
 *   
 */
public class PigWarcStorage extends StoreFunc {

	protected RecordWriter<Object,Text> writer = null;

    private static final int BUFFER_SIZE = 1024;
    private static final String UTF8 = "UTF-8";
    private static final char CR = 0x0D;
    private static final char LF = 0x0A;
    private static final byte[] WARC_OUT_VERSION = "WARC/1.0".getBytes();
    private ByteArrayOutputStream mOut = new ByteArrayOutputStream(BUFFER_SIZE);

    @Override
    public void putNext(Tuple tuple) throws IOException {
    	
    	int numCols = tuple.size();
    	int i=0;
    	Object tupleField = null;
    	
    	// Need at least the required WARC header fields
    	// in the tuple to make a legal WARC record:
    	if (tuple.size() < PigWarcRecord.mandatoryHeaderFields.length) {
    		throw new IOException("Attempt to write tuple without all required WARC header field information to WARC file.");
    	}
    	
    	mOut.write(WARC_OUT_VERSION);
    	mOut.write(LF);
    	for (String headerFieldName : PigWarcRecord.mandatoryHeaderFields) {
    		tupleField = tuple.get(i);
    		if (i > 0)
    			mOut.write(LF);
    		String isoFieldName = PigWarcRecord.ISO_WARC_HEADER_FIELD_NAMES.get(headerFieldName);
    		mOut.write((isoFieldName + ": " + getFieldValue(tupleField)).getBytes(UTF8));
    		i++;
    	}
    	// Now the bag of optional warc headers, if they are part of the 
    	// tuple. They would be second to last (just before the HTML content):
    	if (numCols > PigWarcRecord.mandatoryHeaderFields.length + 1) {
    		tupleField = tuple.get(numCols - 2);
    		mOut.write(LF);
    		mOut.write((getFieldValue(tupleField)).getBytes(UTF8));
    	}
    	// Finally: the empty newline to terminate the WARC header,
    	// followed by the WARC content field and the closing dual CRLF:
    	mOut.write(CR);
    	mOut.write(LF);
    	
    	if (DataType.findType(tuple.get(numCols - 1)) == DataType.BYTEARRAY) {
    		byte[] theByteContent = ((DataByteArray) tuple.get(numCols - 1)).get();
    		if (theByteContent != null) {
    			mOut.write(theByteContent);
    		}
    	} else {
    		String theStringContent = getFieldValue(tuple.get(numCols - 1));
    		if (theStringContent != null)
    			mOut.write(theStringContent.getBytes(UTF8));
    	}
    	// The post-record CRLF CRLF:
    	mOut.write(CR);
    	mOut.write(LF);
    	mOut.write(CR);
    	// The following LF should be necessary,
    	// but somehow an addition LF slips in
    	// elsewhere, making this one superfluous:
    	//mOut.write(LF);

    	Text text = new Text(mOut.toByteArray());
        try {
            writer.write(null, text);
            mOut.reset();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    protected String getFieldValue(Object tupleField) throws ExecException {
    	switch (DataType.findType(tupleField)) {
    	case DataType.INTEGER:
            return ((Integer)tupleField).toString();
		case DataType.CHARARRAY:
            return ((String)tupleField);
        case DataType.BAG:
            Iterator<Tuple> tupleIter = ((DataBag)tupleField).iterator();
            String optionalHeaders = "";
            while(tupleIter.hasNext()) {
            	Tuple attrValueTuple = tupleIter.next();
            	optionalHeaders += attrValueTuple.get(0) + ": " + attrValueTuple.get(1) + '\n';
            }
            return optionalHeaders;
    	}
		return null;
    }

   	@Override
    public TextOutputFormat<WritableComparable<?>, Tuple> getOutputFormat() {
        return new TextOutputFormat<WritableComparable<?>, Tuple>();
    }

    @SuppressWarnings("unchecked")
	@Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) {
        this.writer = writer;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        job.getConfiguration().set("mapred.textoutputformat.separator", "");
        FileOutputFormat.setOutputPath(job, new Path(location));
        if (location.endsWith(".bz2")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job,  BZip2Codec.class);
        }  else if (location.endsWith(".gz")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        }
    }
}
