 package pigir.pigudf;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

//public class TFs extends EvalFunc<Tuple> {
/**
 * Compute term frequencies of documents. 
 * Input:   tuple whose only element is a bag of word tuples taken from
 *          one document: ({(w1),(w2),(w3),...})
 * Output:  Bag of word/tf tuples: {(w1,tf1),(w2,tf2),...)
 *
 * Usage scenario: Tokenize a document as desired, then call this function.
 * @see RegexpTokenize
 * 
 * @author paepcke
 */
public class TFs extends EvalFunc<DataBag> {
	
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();
    Object firstArg = null;
    
    public DataBag exec(Tuple input) throws IOException {
    	DataBag output = mBagFactory.newDefaultBag();
    	DataBag wordsOfDoc = null;
    	
    	HashMap<String,Integer> wordCounts = new HashMap<String,Integer>();

    	if (input == null || input.size() == 0 || (firstArg = input.get(0)) == null) 
    		return null;
    		
    	if (! (firstArg instanceof DataBag))
    		throw new IOException("TFs function must be called with a bag of single-word tuples. Instead, object '" + 
    					   	       firstArg +
    						       "' was passed.");
    	
    	wordsOfDoc = (DataBag) firstArg;
    	
    	Iterator<Tuple> wordIterator = wordsOfDoc.iterator();
    	String oneWord;
    	Integer currentWordCount;
    	
    	while (wordIterator.hasNext()) {
    		oneWord = (String) wordIterator.next().get(0);
    		if (oneWord.isEmpty()) 
    			continue;
    		currentWordCount = wordCounts.get(oneWord);
    		if (currentWordCount == null)
    			wordCounts.put(oneWord, 1);
    		else
    			wordCounts.put(oneWord, currentWordCount + 1);
    	}
    	
    	long docLength= wordsOfDoc.size();
    	for (String word : wordCounts.keySet()) {
    		Tuple wordPlusTFTuple = TupleFactory.getInstance().newTuple(2);
    		wordPlusTFTuple.set(0, word);
    		wordPlusTFTuple.set(1, ((double)wordCounts.get(word))/((double)docLength));
    		output.add(wordPlusTFTuple);
    	}
    	return output;
    }
    
	public Schema outputSchema(Schema input) {
        try{
            Schema wordAndTfSchema = new Schema();
           	wordAndTfSchema.add(new Schema.FieldSchema("word", DataType.CHARARRAY));
           	wordAndTfSchema.add(new Schema.FieldSchema("tf", DataType.DOUBLE));
           	Schema wordTfCollSchema = new Schema();
           	/* The following seems to make no difference: */
           	//wordTfCollSchema.setTwoLevelAccessRequired(true);
           	//wordTfCollSchema.add(new Schema.FieldSchema("wordTfs", wordAndTfSchema, DataType.TUPLE));
           	wordTfCollSchema.add(new Schema.FieldSchema("wordTfs", wordAndTfSchema, DataType.BAG));
            return wordTfCollSchema;
        }catch (Exception e){
                return null;
        }
    }    
	
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FieldSchema> fields = new ArrayList<FieldSchema>(1);
        fields.add(new FieldSchema(null, DataType.BAG));
        FuncSpec funcSpec = new FuncSpec(this.getClass().getName(), new Schema(fields));
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(1);
        funcSpecs.add(funcSpec);
        return funcSpecs;
    }	
    /* ---------------------------------   T E S T I N G ------------------------------*/
	
	private static DataBag makeWordTuplesBag(String doc) {
		DataBag res = new DefaultDataBag();
		for (String word : doc.split(" ")) {
			Tuple singleWordTuple = new DefaultTuple();
			singleWordTuple.append(word);
			res.add(singleWordTuple);
		}
		return res;
	}
	
	public static void main (String [] args) throws IOException {
		TFs func = new TFs();
		Tuple input = new DefaultTuple();
		// Install one field, which we'll reuse:
		input.append(null);
		
		input.set(0, makeWordTuplesBag("This Test"));
		System.out.println(func.exec(input));
		
		input.set(0, makeWordTuplesBag("This and this plus that."));
		System.out.println(func.exec(input));
		
		input.set(0, makeWordTuplesBag("this and this plus that."));
		System.out.println(func.exec(input));
		
		input.set(0, makeWordTuplesBag(""));
		System.out.println(func.exec(input));
		
		DataBag emptyBag = new DefaultDataBag();
		input.set(0,emptyBag);
		System.out.println(func.exec(input));
		
		Schema outschema = func.outputSchema(new Schema());
		System.out.println(outschema);
	}

}
