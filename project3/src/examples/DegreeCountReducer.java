package examples;

import java.io.IOException;
import java.util.Iterator;

import mapredCommon.RecordWriter;
import mapredCommon.Reducer;

public class DegreeCountReducer implements Reducer {
	public void reduce(String key, Iterator<String> values, RecordWriter writer) {
		int degree = 0;
		while (values.hasNext()) {
			degree += Integer.parseInt(values.next());
		}
		try {
			writer.write(key, String.valueOf(degree));
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}