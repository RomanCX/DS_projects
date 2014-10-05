package helloworld;

import remote.Remote;
import remote.RemoteException;

public interface HelloInterface extends Remote{
	public String sayHello(String name) throws RemoteException;
}
