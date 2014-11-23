package dfsClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.TreeMap;

import datanode.Block;
import namenode.DatanodeInfo;
import protocols.Command;
import protocols.NamenodeProtocol;
import protocols.DatanodeOperation;

/**
 * DfsFileWriter is a facility to write files in local disk to dfs
 * @author RomanC
 *
 */
public class DfsFileWriter {
	
	// protocol used to communicate with namenode
	private NamenodeProtocol namenode;
	// block size in bytes
	private int blockSize;
	
	/**
	 * Constructor
	 * @param address ip address of namenode
	 * @param port port number of namenode
	 * @throws Exception
	 */
	public DfsFileWriter(String address, int port) throws Exception {
		Registry registry = LocateRegistry.getRegistry(address, port);
		namenode = (NamenodeProtocol)registry.lookup("namenode");
		blockSize = namenode.getBlockSize();
	}
	
	/**
	 * Write local file to dfs
	 * @param fileName path of local file
	 * @param dfsPath path on dfs
	 * @return true if successfully writes local file to dfs
	 * <p> false if write fails
	 */
	public boolean write(String fileName, String dfsPath) {
		int splitNum = split(fileName);
		int pos = dfsPath.indexOf("//");
		dfsPath = "/" + dfsPath.substring(pos + 2);
		// blockId : list of datanode to be written
		
		
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			TreeMap<Integer, List<DatanodeInfo>> blockToDn = namenode.write(dfsPath, splitNum);
			
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
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/* Send a block to datanode */
	private boolean sendBlock(Block block, DatanodeInfo datanode) {
		Command command = new Command(DatanodeOperation.WRITE_DATA, 0, null);
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
		if (fileSize % blockSize == 0)
			return (int)(fileSize / blockSize);
		else
			return (int)(fileSize / blockSize + 1);
	}
}
