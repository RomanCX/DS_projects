package helloworld;

import remote.RemoteException;
import remote.RemoteReference;
import messages.*;

public class Hello_Stub extends RemoteReference implements HelloInterface {
	public Hello_Stub() {
		
	}
	
	public Hello_Stub(String interfaceName, String host, int port) {
		super(interfaceName, host, port);
	}
	
	public String sayHello(String s) throws RemoteException {
		RMIClientMessage msg = 
				new RMIClientMessage(interfaceName, "sayHello", new Object[]{s});
		RMIServerMessage returnValue = invokeRemoteMethod(msg);
		Exception e = returnValue.getException();
		if (e != null)
			throw new RemoteException("remote method invocation fails", e);
		return (String)returnValue.getReturnValue();
	}
}
