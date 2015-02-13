package org.shirdrn.storm.api;

import java.io.Serializable;

import org.apache.log4j.Level;

/**
 * Manage connection for a used storage engine.
 * 
 * @author Yanjun
 *
 * @param <C> Connection object
 */
public interface ConnectionManager<C> extends Serializable, LifecycleAware {

	/**
	 * Obtain a available connection object
	 * @return
	 */
	C getConnection();
	
	/**
	 * Release connection
	 * @param connection
	 */
	void releaseConnection(C connection);
	
	/**
	 * Get storage engine related command log level
	 * @return
	 */
	Level getCmdLogLevel();
}
