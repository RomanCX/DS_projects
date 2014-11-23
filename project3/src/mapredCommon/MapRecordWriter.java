package mapredCommon;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import tasktracker.TaskTracker;

public class MapRecordWriter implements RecordWriter {
	private Job job;
	private List<BufferedWriter> outWriters;
	private TreeMap<String, List<String>> outResult;
	
	public MapRecordWriter(Job job, int taskId, String outputDir) throws IOException {
		this.job = job;
		this.outWriters = new ArrayList<BufferedWriter>();
		this.outResult = new TreeMap<String, List<String>>();
		for (int i = 0; i < job.getNumReduceTasks(); i++) {
			OutputPath mapOutputPath = new OutputPath(job, taskId, i);
			String outputPath = mapOutputPath.getMapPath(outputDir, TaskTracker.getInstance().getId());
			File outFile = new File(outputPath);
			if (outFile.exists()) {
				outFile.delete();
			}
			this.outWriters.add(new BufferedWriter(new FileWriter(outFile, true)));
		}

	}
	
	@Override
	public void write(String key, String value) throws IOException {
		List<String> values = outResult.get(key);
		if (values == null) {
			values = new ArrayList<String>();
			outResult.put(key, values);
		}
		values.add(value);
	}
	
	public void writeAll() throws IOException {
		for (Entry<String, List<String>> entry: outResult.entrySet()) {
			String key = entry.getKey();
			List<String> values = entry.getValue();
			int hashCode = key.hashCode();
			int reduceNum = hashCode % job.getNumReduceTasks();
			BufferedWriter writer = outWriters.get(reduceNum);
			for (String value : values) {
				writer.write(key);
				writer.write(job.getDelim());
				writer.write(value);
				writer.write("\n");
			}
		}
		
		for (int i = 0; i < job.getNumReduceTasks(); i++) {
			outWriters.get(i).flush();
			outWriters.get(i).close();

		}
	}
}
