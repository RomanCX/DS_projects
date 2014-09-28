import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

class Master {
	private int port;
	private Map<String, Integer> slaves;
	
	public Master(int port) {
		this.port = port;
		slaves = new HashMap<String, Integer>();
	}

	/* Provide service of registering slaver's IP address and port number 
	 * and looking up specific slaver's port number
	 */
	public void startService() {
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
			System.out.println("Fail to establish listening socket");
			return;
		}
		try {
			String name = InetAddress.getLocalHost().getHostName();
			System.out.println("Master name: " + name);
		} catch (Exception e) {
			/* Ignore it */
		}
		System.out.println("Starting service...");
		while (true) {
			try (Socket client = ss.accept();
				Scanner sc = new Scanner(client.getInputStream());
				DataOutputStream dos = new DataOutputStream(
												client.getOutputStream())) {
				String request = null;
				if (sc.hasNext())
					request = sc.nextLine();
				String[] splits = request.split(" ");
				/* Register slaver's IP address and port number */
				if (splits[0].equals("Register")) {
					slaves.put(splits[1], Integer.parseInt(splits[2]));
					System.out.println("Add slave " + splits[1] + " " + 
									splits[2]);
					dos.writeChar('Y');
					dos.flush();
				}
				/* Lookup port number */
				else if (splits[0].equals("Lookup")) {
					/* When address is on the slaver list, 
					 * return its port number
					 */
					if (slaves.containsKey(splits[1]) == true) {
						dos.writeInt(slaves.get(splits[1]));
						dos.flush();
					}
					/* Otherwise return -1 to tell client 
					 * the address is wrong
					 */
					else {
						dos.writeInt(-1);
						dos.flush();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
