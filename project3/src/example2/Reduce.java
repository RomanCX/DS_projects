package example2;

import java.io.IOException;
import java.util.Iterator;

import mapredCommon.RecordWriter;
import mapredCommon.Reducer;

public class Reduce extends Reducer {

	@Override
	public void reduce(String key, Iterator<String> values, RecordWriter writer) {
		int count = 0;
		while (values.hasNext()) {
			count++;
			values.next();
		}
		try {
			writer.write(key, Integer.toString(count));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
