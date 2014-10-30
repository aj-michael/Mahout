package edu.rosehulman;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class StripPOS extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		
		try {
			String str = (String)input.get(0);
			return (str.split("_")[0]);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
}
