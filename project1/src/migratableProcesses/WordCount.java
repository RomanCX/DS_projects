package migratableProcesses;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import transactionalIO.*;

public class WordCount extends MigratableProcess {
	private TransactionalFileInputStream inFile;
	private TransactionalFileOutputStream outFile;
	private String inputFile;
	private String outputFile;
	private Map<String, Integer> count;
	
	private volatile boolean suspending;
	
	public WordCount(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: <inputFile> <outputFile");
			throw new Exception("Invalid Argument");
		}
		inputFile = args[0];
		outputFile = args[1];
		inFile = new TransactionalFileInputStream(inputFile);
		outFile = new TransactionalFileOutputStream(outputFile, false);
		count = new TreeMap<String, Integer>();
		suspending = false;
	}
	
	@Override
	public void run() {
		DataInputStream dis = new DataInputStream(inFile);
		PrintStream ps = new PrintStream(outFile);
		try {
			while (!suspending) {
				String line = dis.readLine();
				if (line == null) {
					Iterator it = count.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry entry = (Map.Entry)it.next();
						ps.println(entry.getKey() + "\t" + entry.getValue());
					}
					break;
				}
				String[] words = line.split(" ");
				for (String w : words) {
					if (count.containsKey(w)) {
						int oldCount = count.get(w);
						count.put(w, oldCount + 1);
					}
					else
						count.put(w, 1);
				}
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					
				}	
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		suspending = false;
	}
	
	@Override
	public void suspend() {
		suspending = true;
		while (suspending);
	}
	
	@Override
	public String toString() {
		return "WordCount " + inputFile + " " + outputFile;
	}
	
}