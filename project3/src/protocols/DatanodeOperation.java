package protocols;

import java.io.Serializable;

public enum DatanodeOperation implements Serializable {
	FETCH_DATA,
	DELETE_DATA,
	READ_DATA,
	WRITE_DATA,
	SHUT_DOWN,
	NOOP
}
