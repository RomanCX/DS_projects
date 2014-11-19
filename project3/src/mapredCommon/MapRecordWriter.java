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

public class MapRecordWriter implements RecordWriter {
	private Job job;
	private List<BufferedWriter> outWriters;
	private HashMap<Integer, TreeMap<String, String>> outResults;
	
	public MapRecordWriter(Job job, int taskId, String outputDir) throws IOException {
		this.job = job;
		this.outWriters = new ArrayList<BufferedWriter>();
		this.outResults = new HashMap<Integer, TreeMap<String,String>>();
		for (int i = 0; i < job.getNumReduceTasks(); i++) {
			OutputPath mapOutputPath = new OutputPath(job, taskId, i);
			String outputPath = mapOutputPath.getMapPath(outputDir);
			File outFile = new File(outputPath);
			if (outFile.exists()) {
				outFile.delete();
			}
			this.outWriters.add(new BufferedWriter(new FileWriter(outFile, true)));
			
			outResults.put(i, new TreeMap<String, String>());
		}

	}
	
	@Override
	public void write(String key, String value) throws IOException {
		int hashCode = key.hashCode();
		int reduceNum = hashCode % job.getNumReduceTasks();
		TreeMap<String, String> outResult = outResults.get((Integer)reduceNum);
		outResult.put(key, value);
	}
	
	public void writeAll() throws IOException {
		for (int i = 0; i < job.getNumReduceTasks(); i++) {
			TreeMap<String, String> outResult = outResults.get((Integer)i);
			BufferedWriter writer = outWriters.get(i);
			for (Entry<String, String> entry : outResult.entrySet()) {
				writer.write(entry.getKey());
				writer.write(job.getDelim());
				writer.write(entry.getValue());
				writer.write("\n");
				System.out.println("key " + entry.getKey());
				System.out.println("value " + entry.getValue());
			}
			writer.flush();
			writer.close();
		}
	}
}
