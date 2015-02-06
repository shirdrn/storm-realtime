package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Callback handler invoked when persisting data to the specified storage.
 * 
 * @author Yanjun
 *
 * @param <C> Connection object.
 */
public interface CallbackHandler<C> extends Serializable {

	/**
	 * Invoke the callback logic.
	 * @param connection
	 * @throws Exception
	 */
	void callback(C connection) throws Exception;
	
}
