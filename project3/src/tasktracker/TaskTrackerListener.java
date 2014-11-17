package tasktracker;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import mapredCommon.OutputPath;

public class TaskTrackerListener implements Runnable{
	ServerSocket listenerSocket;
	
	//Buffer size for transferring file
	private static final int FILE_BUFFER_SIZE = 1024 * 1024;
	
	public TaskTrackerListener(ServerSocket listenerSocket) {
		this.listenerSocket = listenerSocket;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 * Accepts connection from another task tracker 
	 * Receive an MapOutputRequest object indicating job, reduce id
	 * Sends the corresponding map output file
	 */
	@Override
	public void run() {
		while (true) {
			try {
				Socket connectionSocket = listenerSocket.accept();
				System.out.println("Listener: accepted connection from " 
						+ connectionSocket.getInetAddress() + ":" 
						+ connectionSocket.getPort());
				ObjectInputStream inStream = new ObjectInputStream(connectionSocket.getInputStream());
				ObjectOutputStream outStream = new ObjectOutputStream(connectionSocket.getOutputStream());
				
				//Receive the object describing the map output path
				OutputPath mapOutputPath = (OutputPath)inStream.readObject();
				
				File folder = new File(TaskTracker.getInstance().getMapOutputDir());
				File[] filesArray = folder.listFiles();
				List<File> listOfFiles = new ArrayList<File>();
				String prefix = "map" + mapOutputPath.getJob().getJobId();
				String suffix = Integer.toString(mapOutputPath.getReduceId());
				for (File file : filesArray) {
					String fileName = file.getName();
					if (fileName.startsWith(prefix) && fileName.endsWith(suffix)) {
						listOfFiles.add(file);
					}
				}
				Integer fileCount = listOfFiles.size();
				
				//Send the file count
				outStream.writeObject(fileCount);
				
				
				for (int i = 0; i < listOfFiles.size(); i++) {
					//Send the file size
					File file = listOfFiles.get(i);
					Long fileSize = file.length();
					outStream.writeObject(fileSize);
					
					byte[] buffer = new byte[FILE_BUFFER_SIZE];
					BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(file));
					
					//Read and send the file
					int bytesRead = 0;
					while ((bytesRead = fileInput.read(buffer, 0, buffer.length)) != -1) {
						outStream.write(buffer, 0, bytesRead);
					}
					fileInput.close();
				}
				inStream.close();
				outStream.close();
				connectionSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
