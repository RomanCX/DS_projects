package registry;


import remote.Remote;
import remote.RemoteException;


public interface Registry extends Remote {
	public static final int REGISTRY_PORT = 1099;
	public static final String REGISTRY_OBJECT_NAME = "registry";
    public Remote lookup(String name)
    	throws RemoteException;
    public void bind(String name, Remote obj)
    	throws RemoteException;
    public void unbind(String name)
    	throws RemoteException;
	public void rebind(String name, Remote obj)
    	throws RemoteException;

    public String[] list() 
    		throws RemoteException;
}
