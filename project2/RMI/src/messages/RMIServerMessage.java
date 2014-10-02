package messages;

import java.io.Serializable;

public class RMIServerMessage implements Serializable{
	private Object returnValue;
	private Exception exception;
	
	public void setReturnValue(Object returnValue) {
		this.returnValue = returnValue;
	}
	
	public void setException(Exception exception) {
		this.exception = exception;
	}
	
	public Object getReturnValue() {
		return returnValue;
	}
	
	public Exception getException() {
		return exception;
	}
}
