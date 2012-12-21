package edu.stanford.pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class log10 extends EvalFunc<Double> {
	
	/**
	 * Provids Pig script access to Java Math.log10() for Double, Long, and Integer arguments.
	 * 
	 * Input: tuple(arg)
	 * Output: Double
	 * 
	 * @author paepcke
	 *
	 */
	
	public Double exec(Tuple input) throws IOException {
		
		Object rawArg;
		try {
			if (input == null || 
				input.size() < 1 ||
				(rawArg = input.get(0)) == null) 
				return null;
		} catch (ClassCastException e) {
			if (log.isWarnEnabled())
				log.warn("Log10 computation encountered a mal-formed input: " + input);
			return null;
		}
		
		if (rawArg instanceof java.lang.Double) 
			return Math.log10(((Double) rawArg).doubleValue());
		
		if (rawArg instanceof java.lang.Long) 
			return Math.log10(((Long) rawArg).doubleValue());

		if (rawArg instanceof java.lang.Integer) 
			return Math.log10(((Integer) rawArg).doubleValue());
		
		if (rawArg instanceof java.lang.String) 
			System.out.println("*********Log10: string.");
		
		throw new IOException("log10: passed argument that is not Double, Long, or Integer: " + rawArg);
	}
	
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		tupleSchema.add(new FieldSchema("logVal", DataType.DOUBLE));
		return tupleSchema;
	}
	
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		// Implementation for type Double parameter:
		List<FieldSchema> doubleSigField = new ArrayList<FieldSchema>(1);
		doubleSigField.add(new FieldSchema(null, DataType.DOUBLE));
		FuncSpec funcSpecDouble = new FuncSpec(this.getClass().getName(), new Schema(doubleSigField));
		
		// Implementation for type Long parameter:
		List<FieldSchema> longSigField = new ArrayList<FieldSchema>(1);
		longSigField.add(new FieldSchema(null, DataType.LONG));
		FuncSpec funcSpecLong = new FuncSpec(this.getClass().getName(), new Schema(longSigField));
		
		// Implementation for type Integer parameter:
		List<FieldSchema> intSigField = new ArrayList<FieldSchema>(1);
		intSigField.add(new FieldSchema(null, DataType.INTEGER));
		FuncSpec funcSpecInt= new FuncSpec(this.getClass().getName(), new Schema(intSigField));
		
		// Implementation for type String parameter:
		List<FieldSchema> strSigField = new ArrayList<FieldSchema>(1);
		strSigField.add(new FieldSchema(null, DataType.CHARARRAY));
		FuncSpec funcSpecStr = new FuncSpec(this.getClass().getName(), new Schema(strSigField));
		
		List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(4);
		funcSpecs.add(funcSpecDouble);
		funcSpecs.add(funcSpecLong);
		funcSpecs.add(funcSpecInt);
		funcSpecs.add(funcSpecStr);
		return funcSpecs;
	}
}

