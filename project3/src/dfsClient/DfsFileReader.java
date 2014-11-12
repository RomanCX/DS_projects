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
import protocals.ClientProtocal;
import protocals.Command;
import protocals.Operation;

public class DfsFileReader {
	
	// protocol used to communicate with namenode
	ClientProtocal namenode;
	
	public DfsFileReader(String address, int port) throws Exception {
		
		Registry registry = LocateRegistry.getRegistry(address, port);
		namenode = (ClientProtocal)registry.lookup("namenode");
	}
	
	/*
	 *  Read file from dfs
	 *  dfsPath: path in dfs, starting with dfs://
	 *  fileName: name of local file
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
		
		TreeMap<Integer, DatanodeInfo> blockToDn = namenode.read(dfsPath, address);
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))) {
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
			
			oos.writeObject(new Command(Operation.READ_DATA, blockId, null));
			oos.flush();
			block = (Block)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return block;
	}
}
