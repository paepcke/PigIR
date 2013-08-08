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

public class HasStopwordNgram extends FilterFunc {
	
	// This constant is merely informational. The only limitation
	// to the ngram arity is the method getArgToFuncMapping(), which
	// must define an overload for each ngram length:
	static int MAX_ARITY = 5;

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 * @see edu.stanford.pigir.pigudf.IsStopword#exec(org.apache.pig.data.Tuple)
	 * Expecting a tuple of BYTEARRAY or CHARARRAY. The tuple represents
	 * an ngram in one of two ways:
	 * 		- Each word of the ngram may occupy one field of the input tuple, or
	 * 		- The one and only field may contain a comma-separated string that defines
	 * 	      the ngram.
	 * Run through the words. Return true if any of the words is a
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
        
        int ngramEndIndex   = input.size();
        String[] words = null;
        if (input.size() == 1) {
        	// Unigram or comma-separated string of a
        	// higher-arity ngram:
        	try {
        		words = ((String)input.get(0)).split(",");
        	} catch (Exception e) {
        		throw new IOException("Input to HasStopwordNgram.exec() is a one-field tuple whose field is not of type String.");
        	}
        	if (words.length > 1) {
        		// Have comma-separated ngrams; will use
        		// the words array to iterate over the words
        		// below, so we start from index 0:
        		ngramEndIndex   = words.length;
        	} else {
        		// Really have a unigram, not a comma-separated list:
        		words = null;
        	}
        }
        
		for (int i=0; i<ngramEndIndex; i++) {
			String word = null;
			try {
				if (words!=null) {
					word = words[i];
				} else {
					word = (String) input.get(i);
				}
				if (IsStopword.isStopword(word)) {
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
