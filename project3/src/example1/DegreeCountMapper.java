package example1;

import java.io.IOException;
import java.util.StringTokenizer;

import mapredCommon.Mapper;
import mapredCommon.RecordWriter;

public class DegreeCountMapper extends Mapper {
	public void map(int key, String value, RecordWriter writer) {
		try {
			StringTokenizer st = new StringTokenizer(value, ",");
			writer.write(st.nextToken(), "1");
			writer.write(st.nextToken(), "1");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
