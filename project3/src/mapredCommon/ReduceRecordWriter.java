package mapredCommon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * ReduceRecordWriter is a facility to write output of reduce task to dfs.
 * @author RomanC
 *
 */
public class ReduceRecordWriter implements RecordWriter {
	
	private Job job;
	private BufferedWriter writer;
	
	/**
	 * Constructor
	 * @param job currently executing job
	 * @param outputFile output file in dfs
	 */
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
	
	/**
	 * Flush buffered records to dfs
	 * @throws IOException
	 */
	public void flush() throws IOException {
		writer.flush();
	}

}
