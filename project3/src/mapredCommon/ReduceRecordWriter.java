package mapredCommon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ReduceRecordWriter implements RecordWriter {
	
	private Job job;
	private BufferedWriter writer;
	
	public ReduceRecordWriter(Job job, File outputFile) {
		this.job = job;
		try {
			writer = new BufferedWriter(new FileWriter(outputFile));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void write(String key, String value) throws IOException {
		writer.write(key);
		writer.write(job.getDelim());
		writer.write(value);
		writer.newLine();
	}
	
	public void flush() throws IOException {
		writer.flush();
	}

}
