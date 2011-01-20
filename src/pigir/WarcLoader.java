package pigir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

/**
 * @author paepcke
 * 
 * A load function that parses a record of WARC input into fields of a tuple.
 * Given a slice, which is a WARC file, a start, and a stop position within that
 * file, return tuples of the schema:
 *    (WARC_RECORD_ID, CONTENT_LENGTH, WARC_DATE, WARC_TYPE, {(<headerFldName>, <headerFldVal>)*}, <contentStr>)
 */
public class WarcLoader extends FileInputLoadFunc implements LoadPushDown {
    @SuppressWarnings("rawtypes")
	protected RecordReader in = null;    
    protected final Log mLog = LogFactory.getLog(getClass());
    private String signature;
        
    private byte fieldDel = '\t';
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String loadLocation;
    
    public WarcLoader() {
    }
    
    private boolean[] mRequiredColumns = null;
    
    private boolean mRequiredColumnsInitialized = false;

    @Override
    public Tuple getNext() throws IOException {
        mProtoTuple = new ArrayList<Object>();
        BagFactory bagFact = DefaultBagFactory.getInstance();
        if (!mRequiredColumnsInitialized) {
            if (signature!=null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
            }
            mRequiredColumnsInitialized = true;
        }
        try {
            boolean done = ! in.nextKeyValue();
            if (done) {
                return null;
            }
            // Get one Warc record:
            WarcRecord warcRec = (WarcRecord) in.getCurrentValue();
            
            // Create separate columns from the header information,
            // followed by the content field.
            // First the required fields that should be present in
            // order:
            for (String headerKey : warcRec.mandatoryKeysHeader()) {
            	mProtoTuple.add(warcRec.get(headerKey));
            }
            // Now all the optional header keys as a bag of maps:
            DataBag optionalHeaderFieldBag = bagFact.newDefaultBag();
            for (String headerKey : warcRec.optionalKeysHeader()) {
            	Tuple headerOptionalAttrValPair = mTupleFactory.newTuple(2);
            	headerOptionalAttrValPair.set(0,headerKey);
            	headerOptionalAttrValPair.set(1,warcRec.get(headerKey));
            	optionalHeaderFieldBag.add(headerOptionalAttrValPair);
            }
            mProtoTuple.add(optionalHeaderFieldBag);
            mProtoTuple.add(warcRec.get(WarcRecord.CONTENT));
            
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode, 
                    PigException.REMOTE_ENVIRONMENT, e);
        }
      
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;
        if (requiredFieldList.getFields() != null)
        {
            int lastColumn = -1;
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()>lastColumn)
                {
                    lastColumn = rf.getIndex();
                }
            }
            mRequiredColumns = new boolean[lastColumn+1];
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()!=-1)
                    mRequiredColumns[rf.getIndex()] = true;
            }
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(signature, ObjectSerializer.serialize(mRequiredColumns));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }
        }
        return new RequiredFieldResponse(true);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof WarcLoader)
            return equals((WarcLoader)obj);
        else
            return false;
    }

    public boolean equals(WarcLoader other) {
        return this.fieldDel == other.fieldDel;
    }

    @SuppressWarnings("rawtypes")
	@Override
    public InputFormat getInputFormat() {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new WarcPigTextInputFormat();
        }
    }

    @SuppressWarnings("rawtypes")
	@Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public int hashCode() {
        return (int)fieldDel;
    }
    
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature; 
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }
}
