package examples;

import java.io.IOException;

import mapredCommon.Mapper;
import mapredCommon.RecordWriter;

public class DegreeCountMapper implements Mapper {
	public void map(int key, String value, RecordWriter writer) {
		try {
			writer.write(value, "1");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
