package org.shirdrn.storm.analytics.common;

import org.apache.commons.logging.Log;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;

public abstract class AbstractIndicatorCalculator<R> implements IndicatorCalculator<R> {

	private static final long serialVersionUID = 1L;
	private Level logLevel;
	protected final int indicator;
	
	public AbstractIndicatorCalculator(int indicator) {
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
