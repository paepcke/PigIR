/**
 * 
 */
package edu.stanford.pigir.pigudf;

/**
 * Given a tuple that represents an ngram, return true if any of the
 * constituent words is a stopword. Else return false. 
 * 
 * @author paepcke
 *
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import edu.stanford.pigir.pigudf.IsStopword;

public class HasStopwordNgram extends FilterFunc {
	
	// This constant is merely informational. The only limitation
	// to the ngram arity is the method getArgToFuncMapping(), which
	// must define an overload for each ngram length:
	static int MAX_ARITY = 5;

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 * @see edu.stanford.pigir.pigudf.IsStopword#exec(org.apache.pig.data.Tuple)
	 * Expecting a tuple of BYTEARRAY or CHARARRAY. The tuple represents
	 * an ngram. The arity of the ngram may be 1 to MAX_ARITY.
	 * Run through the input tuple of words. Return true if any of the words is a
	 * stopword. Else return false. This method uses the single-word stopword determinator
	 * IsStopword.isStopword(). See that method for the list of words that are considered
	 * stopwords.
	 * 
	 * The number of words in the ngram is actually not limited by the code in
	 * this method. The only limitation lies in the getArgToFuncMapping() method that
	 * defines overloads.
	 *  
	 * @param input The Pig tuple containing all the words.
	 * @return true if any of the words is a stopword.
	 * @throws IOException
	 */
	@Override
	public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
		for (int i=0; i<input.size(); i++) {
			try {
				if (IsStopword.isStopword((String) input.get(i))) {
					return true;
				}
			} catch (Exception e) {
				throw new IOException(String.format("Ngram tuple contains field that cannot be converted to a string. Field position: %d", i));
			}
		}
		return false;
	}

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     * This method establishes operator overloads for the exec
     * method. We enable exec() to be called with anywhere between 
     * a one-tuple and a five-tuple. For all five there is a version
     * for chararray and bytearray. The latter is needed when the
     * calling Pig script didn't know the ngram arity, and couldn't
     * establish a schema for the loaded ngrams. That schema would have
     * established the ngram words to be chararray. The absence of an
     * explicit schema during load will have declared the words to be
     * bytearrays.
     * 
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        
	Schema unigramChararray = new Schema();
	unigramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	funcList.add(new FuncSpec(this.getClass().getName(), unigramChararray));

	Schema bigramChararray = new Schema();
	bigramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	bigramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	funcList.add(new FuncSpec(this.getClass().getName(), bigramChararray));

	Schema trigramChararray = new Schema();
	trigramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	trigramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	trigramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	funcList.add(new FuncSpec(this.getClass().getName(), trigramChararray));
	
	Schema quadgramChararray = new Schema();
	quadgramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	quadgramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	quadgramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	quadgramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));	
	funcList.add(new FuncSpec(this.getClass().getName(), quadgramChararray));

	Schema quingramChararray = new Schema();
	quingramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	quingramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	quingramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
	quingramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));	
	quingramChararray.add(new Schema.FieldSchema(null, DataType.CHARARRAY));	
	funcList.add(new FuncSpec(this.getClass().getName(), quingramChararray));

	// Now the bytearray versions:
	
	Schema unigramBytearray = new Schema();
	unigramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	funcList.add(new FuncSpec(this.getClass().getName(), unigramBytearray));

	Schema bigramBytearray = new Schema();
	bigramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	bigramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	funcList.add(new FuncSpec(this.getClass().getName(), bigramBytearray));

	Schema trigramBytearray = new Schema();
	trigramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	trigramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	trigramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	funcList.add(new FuncSpec(this.getClass().getName(), trigramBytearray));
	
	Schema quadgramBytearray = new Schema();
	quadgramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	quadgramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	quadgramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	quadgramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));	
	funcList.add(new FuncSpec(this.getClass().getName(), quadgramBytearray));

	Schema quingramBytearray = new Schema();
	quingramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	quingramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	quingramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
	quingramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));	
	quingramBytearray.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));	
	funcList.add(new FuncSpec(this.getClass().getName(), quingramBytearray));
	

	return funcList;
    }
}
