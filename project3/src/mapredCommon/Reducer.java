package mapredCommon;

import java.util.Iterator;

public interface Reducer {
	public void reduce(String key, Iterator<String> values, RecordWriter writer);
}
