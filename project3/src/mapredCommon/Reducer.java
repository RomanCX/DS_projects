package mapredCommon;

import java.util.Iterator;

public abstract class Reducer {
	
	public void setup() {
		//Does nothing by default
	}
	
	public abstract void reduce(String key, Iterator<String> values, RecordWriter writer);
}
