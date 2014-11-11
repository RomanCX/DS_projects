package dfsClient;

import java.net.InetAddress;
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
import protocals.ClientProtocal;
import protocals.Command;
import protocals.Operation;

public class Client {
	private static final String prompt = ">>";
	private static String address;
	private static ClientProtocal namenode;
	
	
	/*
	 *  possible commands:
	 *  (1) get dfs://path path
	 *  (2) put path dfs://path
	 *  (3) ls dfs://path
	 */
	public static void main(String[] args) {
		
		try {
			address = InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			Registry registry = LocateRegistry.getRegistry();
			namenode = (ClientProtocal)registry.lookup("namenode");
		} catch (Exception e) {
			System.out.println("fail to get namenode");
			System.exit(1);
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
			else {
				System.out.println("Unknow command");
			}
		}
	}
	
	private static void executeGet(String dfsPath, String localPath) {
		int pos = dfsPath.indexOf("//");
		dfsPath = "/" + dfsPath.substring(pos + 1);
		Map<DatanodeInfo, List<Integer>> blockInfo = namenode.read(dfsPath, address);
		
		List<Block> blocks = new ArrayList<Block>();
		Iterator iter = blockInfo.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry)(iter.next());
			int datanodeId = (int)entry.getKey();
			List<Integer> blockIds = (List<Integer>)entry.getValue();
			
		}
	}
	
	private static void executePut(String localPath, String dfsPath) {
		
	}
	
	private static void executeLs(String dfsPath) {
		
	}
}
