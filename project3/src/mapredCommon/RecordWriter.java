package mapredCommon;

import java.io.IOException;

public interface RecordWriter {
	public void write(String key, String value)throws IOException;
}
