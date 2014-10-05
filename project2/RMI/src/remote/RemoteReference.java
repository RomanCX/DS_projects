package remote;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

import messages.*;

/* All stubs should extend this class */
public class RemoteReference implements Serializable {
	private static final long serialVersionUID = -1227441569718798946L;
	
	protected String interfaceName;
	private String host;
	private int port;
	
	public RemoteReference() {
		
	}
	
	public RemoteReference(String interfaceName, String host, int port) {
		this.interfaceName = interfaceName;
		this.host = host;
		this.port = port;
	}
	
	/* Receive a message from client which encapsulates all the information about 
	 * a remote method invocation. And then send this message to server, receive the 
	 * response message which includes the return value of this method (if any) or 
	 * any exceptions if the remote method fails in server.
	 */
	protected RMIServerMessage invokeRemoteMethod(RMIClientMessage msg) {
		RMIServerMessage ret = null;
		
		try (Socket socket = new Socket(host, port);
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
			oos.writeObject(msg);
			ret = (RMIServerMessage)ois.readObject();
		} catch (Exception e) {
			ret = new RMIServerMessage();
			ret.setException(e);
		}
		return ret;
	}
}
