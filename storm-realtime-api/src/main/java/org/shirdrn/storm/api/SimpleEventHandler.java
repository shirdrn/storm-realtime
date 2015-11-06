package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Simple event handler which handles event.
 * 
 * @author Yanjun
 *
 * @param <RESULT> Computed {@link Result}
 * @param <EVENT> Event data object
 */
public interface SimpleEventHandler<RESULT, EVENT> extends Serializable {

	/**
	 * Execute statistics computation, and result stat result.
	 * @param event
	 * @return
	 */
	RESULT handle(EVENT event) throws Exception;
	
	
	/**
	 * Get event code to identify a event.
	 * @return
	 */
	String getEventCode();

}
