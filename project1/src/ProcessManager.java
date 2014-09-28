import java.util.Scanner;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessManager {
	private static final long frequency = 500;
	private static final String prompt = ">>> ";
	private static Lock locker;
	
	public static void main(String []args) {
		/* Run as a slaver by launching with arguments 
		 * "-s <masterName> <masterPort>"
		 */
		if (args[0].equals("-s")) {
			int masterPort = Integer.parseInt(args[2]);
			locker = new ReentrantLock();
			Slave slaver = new Slave(args[1], masterPort, locker);
			Listener listener = new Listener(slaver, locker);
			Poller poller = new Poller(slaver, frequency, locker);
			new Thread(listener).start();
			new Thread(poller).start();
			
			/* Sleep one second to let listener register IP address and 
			 * port number in master. 
			 */
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				/* ignore it */
			}
			
			Scanner scanner = new Scanner(System.in);
			System.out.print(prompt);
			while (scanner.hasNext()) {
				String command = scanner.nextLine();
				String [] splits = command.split(" ");
				/* List all the running processes */
				if (splits[0].equals("ps")) {
					slaver.listProcess();
				}
				/* Migrate a specific process to another machine */
				else if (splits[0].equals("migrate")) {
					if (splits.length != 3) {
						System.out.println("Usage: migrate <process id>" + 
							"<target machine>");
					}
					else {
						slaver.migrateProcess(Integer.parseInt(splits[1]), 
											splits[2]);
					}
				}
				/* Launch a new process */
				else if (splits[0].equals("launch")) {
					if (splits.length < 2) {
						System.out.println("Usage: launch <process name> "
								+ "[arglist]");
					}
					else {
						String [] arguments = 
							 Arrays.copyOfRange(splits, 2, splits.length);
					 	slaver.launchProcess(splits[1], arguments);
					}
				}
				/* Quit ProcessManager */
				else if (splits[0].equals("quit")) {
					System.exit(0);
				}
				/* Other commands are not supported */
				else {
					System.out.println("Unknown command");
				}
				
				System.out.print(prompt);
			}
		}
		
		/* Run as a master by launching with arguments
		 * "-m <port>"
		 */
		else if (args[0].equals("-m")) {
			Master master = new Master(Integer.parseInt(args[1]));
			master.startService();
		}
		/* Print help information */
		else if (args[0].equals("-h")) {
			System.out.println("Run as a slaver: ./ProcessManager -s "
					+ " <master's name> <master's port>");
			System.out.println("Run as a master: ./ProcessManger -m <port>");
		}
		else {
			System.out.println("You can only run as a slaver or master!");
		}
	}
}


