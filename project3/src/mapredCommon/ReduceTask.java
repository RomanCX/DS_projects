package mapredCommon;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import dfsClient.DfsFileWriter;
import tasktracker.TaskTracker;
import jobtracker.TaskTrackerInfo;

public class ReduceTask extends Task {
	private List<TaskTrackerInfo> taskTrackers;
	private List<File> mapOutFiles;
	private File sortedFile;
	private File reduceOutputTmpFile;
	private OutputPath outputPath;
	
	private static final int FILE_BUFFER_SIZE = 1024 * 1024;
	
	public class ValueEntry {
		public int index;
		public String value;
		
		public ValueEntry(int index, String value) {
			this.index = index;
			this.value = value;
		}
	}
	
	public ReduceTask(int taskId, Job job,
				List<TaskTrackerInfo> taskTrackers) {
		super(taskId, job);
		this.taskTrackers = taskTrackers;
		this.outputPath = new OutputPath(job, taskId, taskId);
		mapOutFiles = new ArrayList<File>();

		sortedFile = new File(outputPath.getReduceSortedPath(tmpDir));
		if (sortedFile.exists()) {
			sortedFile.delete();
		}
		
		reduceOutputTmpFile = new File(outputPath.getReducePath(tmpDir));
		if (reduceOutputTmpFile.exists()) {
			reduceOutputTmpFile.delete();
		}
	}

	@Override
	public void run() {
		try {
			fetchFiles();
			sortFiles();
			
			Reducer reducer = MapReduceUtils.getReducer(job.getJarFile());
			ReduceRecordWriter writer = new ReduceRecordWriter(job, reduceOutputTmpFile);
			
			BufferedReader reader = new BufferedReader(new FileReader(sortedFile));
			String line = reader.readLine();
			int pos = line.indexOf(job.getDelim());
			
			/*
			 * The following loop does the trick to collect 
			 * all values with the same key
			 */
			String currKey = line.substring(0, pos);
			List<String> values = new ArrayList<String>();
			String value = line.substring(pos + 1);
			values.add(value);
			String lastKey = currKey;
			while ((line = reader.readLine()) != null) {
				pos = line.indexOf(job.getDelim());
				currKey = line.substring(0, pos);
				value = line.substring(pos + 1);
				if (currKey.equals(lastKey)) {
					values.add(value);
				} else {
					reducer.reduce(lastKey, values.iterator(), writer);
					lastKey = currKey;
					values = new ArrayList<String>();
					values.add(value);
				}
			}
			reducer.reduce(lastKey, values.iterator(), writer);

			uploadOutput();
			
			reader.close();
			TaskTracker.getInstance().reportFinished(taskId);
		} catch (Exception e) {
			e.printStackTrace();
			TaskTracker.getInstance().reportFailed(taskId);
		} finally {
			sortedFile.delete();
			for (File file : mapOutFiles) {
				file.delete();
			}
		}
		
		
	}
	
	private void fetchFiles() {
		try {

			for (int i = 0; i < taskTrackers.size(); i++) {
				TaskTrackerInfo taskTracker = taskTrackers.get(i);
				Socket socket = new Socket(taskTracker.getAddress(), taskTracker.getPort());
				ObjectInputStream inStream = new ObjectInputStream(socket.getInputStream());
				ObjectOutputStream outStream = new ObjectOutputStream(socket.getOutputStream());
				
				//Send the requesting map output path
				outStream.writeObject(outputPath);
				
				//Receive the file count
				Integer fileCount = inStream.readInt();
				
				for (int j = 0; i < fileCount; i++) {
					//Receive the file length
					Long fileSize = inStream.readLong();
					
					//Create the file and put to outputPath list
					File tmpFile = new File(outputPath.getReduceTmpPath(tmpDir, i, j));
					if (tmpFile.exists()) {
						tmpFile.delete();
					}
					mapOutFiles.add(tmpFile);
					
					//Read the file from socket and write to local disk
					FileOutputStream fileOutput = new FileOutputStream(tmpFile, true);
					byte[] buffer = new byte[FILE_BUFFER_SIZE];
					int bytesRead = 0;
					while (fileSize != 0) {
						if (fileSize >= buffer.length) {
							bytesRead = inStream.read(buffer, 0, buffer.length);
							fileOutput.write(buffer, 0, bytesRead);
							fileSize -= bytesRead;
						} else {
							bytesRead = inStream.read(buffer, 0, fileSize.intValue());
							fileOutput.write(buffer, 0, bytesRead);
							fileSize -= bytesRead;
						}
					}
					fileOutput.close();
					
				}
				
				
				inStream.close();
				outStream.close();
				socket.close();
			}
			
		} catch (Exception e) {
			TaskTracker.getInstance().reportFailed(taskId);
			e.printStackTrace();
		}
	}
	
	/*
	 * This function performs an external merge sort. Given a list of sorted files,
	 * it put the the first key-value pair into a heap(priority queue). Then in every
	 * iteration, it gets the smallest key-value pair from the heap, write it to output
	 * file, delete that pair from heap and read a new key-value pair from the file the
	 * just-deleted pair belonged.
	 */
	private void sortFiles() throws IOException {
		List<BufferedReader> readers = new ArrayList<BufferedReader>();
		for (int i = 0; i < mapOutFiles.size(); i++) {
			readers.add(new BufferedReader(new FileReader(mapOutFiles.get(i))));
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter(sortedFile));
		TreeMap<String, ValueEntry> heap = new TreeMap<String, ValueEntry>();
		for (int i = 0; i < readers.size(); i++) {
			readLinePutHeap(heap, readers, i);
		}
		while (!heap.isEmpty()) {
			int index = writeValueEntry(heap, writer);
			readLinePutHeap(heap, readers, index);
		}
			
	}
	
	/*
	 * Read a line from reader(i). This is the file that has the smallest record and 
	 * just pulled from heap into output file
	 */
	private void readLinePutHeap(TreeMap<String, ValueEntry> heap, List<BufferedReader> readers, int index) {
		try {
			BufferedReader reader = readers.get(index);
			String line = reader.readLine();
			if (line == null) {
				return;
			}
			int pos = line.indexOf(job.getDelim());
			String key = line.substring(0, pos);
			String value = line.substring(pos + 1);
			ValueEntry valueEntry = new ValueEntry(index, value);
			heap.put(key, valueEntry);
				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 * Write a key-value pair to  the file specified by writer
	 */
	private int writeValueEntry(TreeMap<String, ValueEntry> heap, BufferedWriter writer) {
		Entry<String, ValueEntry> entry = heap.pollFirstEntry();
		String key = entry.getKey();
		ValueEntry valueEntry = entry.getValue();
		try {
			writer.write(key);
			writer.write(job.getDelim());
			writer.write(valueEntry.value);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return valueEntry.index;
	}
		
	/*
	 * Upload the local reduce output file to HDFS
	 */
	private void uploadOutput() throws Exception  {
		String jobTrackerAddress = TaskTracker.getInstance().getJobTrackerAddress();
		int jobTrackerPort = TaskTracker.getInstance().getJobTrackerPort();
		DfsFileWriter writer = new DfsFileWriter(jobTrackerAddress, jobTrackerPort);
		if (writer.write(reduceOutputTmpFile.getAbsolutePath(), job.getOutputPath()) == false) {
			System.out.println("Failed to get the writer");
			throw new IOException();
		}
	}
}

