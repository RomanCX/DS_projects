import java.rmi.Remote;
import java.rmi.RemoteException;


public interface Test extends Remote {
	public void kao(Class clazz) throws RemoteException;

}
