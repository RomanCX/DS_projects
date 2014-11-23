package dfsClient;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import namenode.DatanodeInfo;
import datanode.Block;
import protocals.Command;
import protocals.NamenodeProtocal;
import protocals.Operation;

public class Client {
	private static final String prompt = ">>";
	// address of namenode
	private static String address;
	// port number of namenode
	private static int port;
	// address of client
	private static String myAddress;
	private static NamenodeProtocal namenode;
	
	
	/*
	 *  possible commands:
	 *  (1) get dfs://path path
	 *  (2) put path dfs://path
	 *  (3) ls dfs://path
	 *  (4) delete dfs://path
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: <namenode address> <namenode port>");
			System.exit(1);
		}
		
		address = args[0];
		port = Integer.parseInt(args[1]);
		
		try {
			myAddress = InetAddress.getLocalHost().getHostName();
			Registry registry = LocateRegistry.getRegistry(address, port);
			namenode = (NamenodeProtocal)registry.lookup("namenode");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Scanner scanner = new Scanner(System.in);
		String command = null;
		
		while (true) {
			System.out.print(prompt);
			if (scanner.hasNext()) {
				command = scanner.nextLine();
			}
			StringTokenizer st = new StringTokenizer(command);
			String [] splits = new String[3];
			int idx = 0;
			while (st.hasMoreTokens()) {
				splits[idx] = st.nextToken();
				idx++;
			}
			
			if (idx == 0) {
				System.out.println("Empty command");
				continue;
			}
			
			if (splits[0].equals("get")) {
				executeGet(splits[1], splits[2]);
			}
			else if (splits[0].equals("put")) {
				executePut(splits[1], splits[2]);
			}
			else if (splits[0].equals("ls")) {
				executeLs(splits[1]);
			}
			else if (splits[0].equals("delete")) {
				executeDel(splits[1]);
			}
			else if (splits[0].equals("quit")) {
				System.exit(0);
			} 
			else if (splits[0].equals("shutdown")) {
				executeShutDown();
				System.exit(0);
			}
			else {
				System.out.println("Unknow command");
			}
		}
	}
	
	private static void executeGet(String dfsPath, String localPath) {
		try {
			DfsFileReader reader = new DfsFileReader(address, port);
			if (reader.read(dfsPath, localPath) == true) {
				System.out.println("Got " + dfsPath + " from dfs");
			}
			else {
				System.out.println("fail to get" + dfsPath);
			}
		} catch (Exception e) {
			System.out.println("fail to get reader");
		}
	}
	
	private static void executePut(String localPath, String dfsPath) {
		try {
			DfsFileWriter writer = new DfsFileWriter(address, port);
			if (writer.write(localPath, dfsPath) == true) {
				System.out.println(localPath + " has been put to " + dfsPath);
			}
			else {
				System.out.println("fail to write " + localPath + " to " + dfsPath);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("fail to get writer");
		}
		
	}
	
	private static void executeLs(String dfsPath) {
		try {
			List<String> files = namenode.ls();
			for (String file : files) {
				System.out.println(file);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void executeDel(String dfsPath) {
		int pos = dfsPath.indexOf("//");
		dfsPath = "/" + dfsPath.substring(pos + 2);
		try {
			namenode.delete(dfsPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void executeShutDown() {
		try {
			namenode.shutDown();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
}
