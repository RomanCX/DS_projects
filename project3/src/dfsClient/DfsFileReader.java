package dfsClient;

import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import protocals.ClientProtocal;

public class DfsFileReader {
	
	// protocol used to communicate with namenode
	ClientProtocal namenode;
	
	public DfsFileReader(String address, int port) {
		try {
			Registry registry = LocateRegistry.getRegistry(address, port);
			namenode = (ClientProtocal)registry.lookup("namenode");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 *  Read file from dfs
	 *  dfsPath: path in dfs, starting with dfs://
	 *  fileName: name of local file
	 */
	public boolean read(String dfsPath, String fileName) {
		int pos = dfsPath.indexOf("//");
		dfsPath = "/" + dfsPath.substring(pos + 1);
		String address = null;
		try {
			address = InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			e.printStackTrace();
		}
		namenode.read(dfsPath, address);
	}
}
