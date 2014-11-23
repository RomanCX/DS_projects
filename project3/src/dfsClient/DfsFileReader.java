package dfsClient;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;

import datanode.Block;
import namenode.DatanodeInfo;
import protocols.Command;
import protocols.NamenodeProtocol;
import protocols.DatanodeOperation;

/**
 * DfsFileReader is a facility to read file from dfs
 * @author RomanC
 *
 */
public class DfsFileReader {
	
	// protocol used to communicate with namenode
	NamenodeProtocol namenode;
	
	/**
	 * Constructor
	 * @param address ip address of namenode
	 * @param port port number of namenode
	 * @throws Exception
	 */
	public DfsFileReader(String address, int port) throws Exception {
		
		Registry registry = LocateRegistry.getRegistry(address, port);
		namenode = (NamenodeProtocol)registry.lookup("namenode");
	}
	
	/**
	 * Read file from dfs
	 * @param dfsPath file path in dfs, starting with dfs://
	 * @param fileName name on local disk
	 * @return true if successfully read file from dfs
	 * <p> false if the read fails
	 */ 
	public boolean read(String dfsPath, String fileName) {
		int pos = dfsPath.indexOf("//");
		dfsPath = "/" + dfsPath.substring(pos + 2);
		String address = null;
		try {
			address = InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))) {
			TreeMap<Integer, DatanodeInfo> blockToDn = namenode.read(dfsPath, address);
			for (int blockId : blockToDn.keySet()) {
				Block b = getBlock(blockId, blockToDn.get(blockId));
				if (b == null) {
					System.out.println("fail to get block" + blockId);
					return false;
				}
				bw.write(b.getData());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private Block getBlock(int blockId, DatanodeInfo datanode) {
		Block block = null;
		
		try (Socket socket = new Socket(datanode.getAddress(), datanode.getPort());
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
			
			oos.writeObject(new Command(DatanodeOperation.READ_DATA, blockId, null));
			oos.flush();
			block = (Block)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return block;
	}
}
