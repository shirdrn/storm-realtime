package org.shirdrn.storm.analytics.common;

import org.apache.commons.logging.Log;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.IndicatorCalculator;

/**
 * A generic indicator calculator who is holding some basic information
 * about a {@link IndicatorCalculator} instance.
 * 
 * @author Yanjun
 *
 * @param <R>
 * @param <C>
 * @param <E>
 */
public abstract class GenericIndicatorCalculator<R, C, E> implements IndicatorCalculator<R, C, E>, Loggingable {

	private static final long serialVersionUID = 1L;
	private Level logLevel;
	protected final int indicator;
	
	public GenericIndicatorCalculator(int indicator) {
		super();
		this.indicator = indicator;
	}
	
	@Override
	public int getIndicator() {
		return indicator;
	}

	@Override
	public void setPrintRedisCmdLogLevel(Level logLevel) {
		this.logLevel = logLevel;
	}
	
	protected void logRedisCmd(Log log, String cmd) {
		RealtimeUtils.printRedisCmd(log, logLevel, cmd);
	}

}
