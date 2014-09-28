package migratableProcesses;

import java.io.PrintStream;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.Thread;
import java.lang.InterruptedException;
import transactionalIO.*;

public class GrepProcess extends MigratableProcess
{
	private TransactionalFileInputStream  inFile;
	private TransactionalFileOutputStream outFile;
	private String inputFile;
	private String outputFile;
	private String query;
	
	private volatile boolean suspending;

	public GrepProcess(String args[]) throws Exception
	{
		if (args.length != 3) {
			System.out.println("usage: GrepProcess <queryString> <inputFile> <outputFile>");
			throw new Exception("Invalid Arguments");
		}
		
		query = args[0];
		inputFile = args[1];
		outputFile = args[2];
		inFile = new TransactionalFileInputStream(inputFile);
		outFile = new TransactionalFileOutputStream(outputFile, false);
	}

	public void run()
	{   
		PrintStream out = new PrintStream(outFile);
		DataInputStream in = new DataInputStream(inFile);
		try {
			while (!suspending) {
				String line = in.readLine();

				if (line == null) break;
				
				//System.out.println(line);
				if (line.contains(query)) {
					out.println(line);
				}
				
				// Make grep take longer so that we don't require extremely large files for interesting results
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					//Ignore it
				}
			}
		} catch (EOFException e) {
			//End of File
		} catch (IOException e) {
			System.out.println ("GrepProcess: Error: " + e);
		}


		suspending = false;
	}

	public void suspend()
	{
		suspending = true;
		while (suspending);
	}
	
	@Override
	public String toString() {
		return "GrepProcess " + query + " " + inputFile + " " + outputFile;
	}

}
