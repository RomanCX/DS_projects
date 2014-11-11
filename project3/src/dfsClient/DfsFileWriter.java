package dfsClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import datanode.Block;
import namenode.DatanodeInfo;
import protocals.ClientProtocal;
import protocals.Command;
import protocals.Operation;

public class DfsFileWriter {
	
	// protocol used to communicate with namenode
	private ClientProtocal namenode;
	// block size in bytes
	private int blockSize;
	
	public DfsFileWriter(String address, int port) throws Exception {
		Registry registry = LocateRegistry.getRegistry(address, port);
		namenode = (ClientProtocal)registry.lookup("namenode");
		blockSize = namenode.getBlockSize();
	}
	
	/* 
	 *  Write local file to dfs
	 *  fileName: the name of local file
	 *  dfsPath: the path on dfs, starting with dfs://
	 */
	public boolean write(String fileName, String dfsPath) {
		int splitNum = split(fileName);
		int pos = dfsPath.indexOf("//");
		dfsPath = "/" + dfsPath.substring(pos + 1);
		// blockId : list of datanode to be written
		// tree map?
		TreeMap<Integer, List<DatanodeInfo>> blockToDn = namenode.write(dfsPath, splitNum);
		
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String line = null;
		String prevLine = null;
		Block block = null;
		for (int blockId : blockToDn.keySet()) {
			StringBuilder sb = new StringBuilder();
			if (prevLine != null) {
				sb.append(prevLine);
				sb.append("\n");
			}
			while ((line = br.readLine()) != null) {
				if (sb.length() + line.length() > blockSize) {
					prevLine = line;
					break;
				}
				sb.append(line);
				sb.append("\n");
			}
			block = new Block(blockId, sb.toString());
			for (DatanodeInfo datanode : blockToDn.get(blockId)) {
				if (!sendBlock(block, datanode)) {
					System.out.println("fail to write block " + blockId
							+ "to datanode" + datanode.getAddress());
					return false;
				}
			}
		}
		return true;
	}
	
	/* Send a block to datanode */
	private boolean sendBlock(Block block, DatanodeInfo datanode) {
		Command command = new Command(Operation.WRITE_DATA, null);
		try (Socket socket = new Socket(datanode.getAddress(), datanode.getPort());
			 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			 ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
			oos.writeObject(command);
			oos.writeObject(block);
			oos.flush();
			String ack = (String)ois.readObject();
			if (ack.equals("succeed"))
				return true;
			else
				return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/* Decide how many splits this file should have
	 * The calculation is based on the metadata of this file
	 * It won't actually read the whole file
	 */
	private int split(String fileName) {
		File f = new File(fileName);
		long fileSize = f.length();
		if (fileSize / blockSize == 0)
			return (int)(fileSize / blockSize);
		else
			return (int)(fileSize / blockSize + 1);
	}
}
