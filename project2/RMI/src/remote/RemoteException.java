package remote;

public class RemoteException extends Exception {
	private static final long serialVersionUID = -4086974204435875229L;
	
	public RemoteException() {
		
	}
	
	public RemoteException(String message) {
		super(message);
	}
	
	public RemoteException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
