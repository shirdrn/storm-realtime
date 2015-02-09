package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Indicator computation interface.
 * 
 * @author Yanjun
 *
 * @param <R> Computed {@link Result}
 * @param <C> Connection object
 * @param <E> Event data object
 */
public interface IndicatorCalculator<R, C, E> extends Serializable {

	/**
	 * Compute for the indicator.
	 * @param client
	 * @param event
	 * @return
	 */
	R calculate(final C connection, E event);
	
	int getIndicator();
}
