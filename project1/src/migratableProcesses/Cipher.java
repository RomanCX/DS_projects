package migratableProcesses;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;

import transactionalIO.*;

public class Cipher extends MigratableProcess {
	private TransactionalFileInputStream inFile;
	private TransactionalFileOutputStream outFile;
	private String inputFile;
	private String outputFile;
	private int offset;
	
	private volatile boolean suspending;
	
	public Cipher(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: <inputFile> <outputFile");
			throw new Exception("Invalid Argument");
		}
		inputFile = args[0];
		outputFile = args[1];
		inFile = new TransactionalFileInputStream(inputFile);
		outFile = new TransactionalFileOutputStream(outputFile, false);
		offset = 3;
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
					break;
				}
				for (int i = 0; i < line.length(); ++i) {
					char c = line.charAt(i);
					if (c >= 'A' && c <= 'Z') {
						c = (char)(((int)c - 65 + offset) % 26 + 65);
					}
					else if (c >= 'a' && c <= 'z') {
						c = (char)(((int)c - 97 + offset) % 26 + 97);
					}
					ps.print(c);
				}
				ps.print('\n');
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
		return "Cipher " + inputFile + " " + outputFile;
	}
	
}