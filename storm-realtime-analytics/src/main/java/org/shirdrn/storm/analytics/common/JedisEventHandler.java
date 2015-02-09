package org.shirdrn.storm.analytics.common;

import java.util.NoSuchElementException;

import net.sf.json.JSONObject;

import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.api.IndicatorCalculator;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.common.GenericEventHandler;
import org.shirdrn.storm.api.utils.IndicatorCalculatorFactory;

import redis.clients.jedis.Jedis;

/**
 * Event handler abstract implementation base on <code>Redis</code> storage engine.
 * 
 * @author Yanjun
 */
public abstract class JedisEventHandler extends GenericEventHandler<Result, Jedis, JSONObject> {

	private static final long serialVersionUID = 1L;
	protected transient ConnectionManager<Jedis> connectionManager;
	
	public JedisEventHandler(String eventCode) {
		super(eventCode);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected IndicatorCalculator<Result, Jedis, JSONObject> getIndicatorCalculator(int indicator) {
		return (IndicatorCalculator<Result, Jedis, JSONObject>) 
					IndicatorCalculatorFactory.newIndicatorCalculator(indicator);
	}
	
	@Override
	protected Result processEvent(int indicator, JSONObject event) {
		Jedis connection = connectionManager.getConnection();
		IndicatorCalculator<Result, Jedis, JSONObject> calculator = selectCalculator(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for indicator: " + indicator);
		}
		
		// set writing Redis command log level
		if(calculator instanceof Loggingable) {
			((Loggingable) calculator).setPrintRedisCmdLogLevel(connectionManager.getCmdLogLevel());
		}
		Result result = calculator.calculate(connection, event);
		connectionManager.releaseConnection(connection);
		return result;
	}
	
	@Override
	public void setConnectionManager(ConnectionManager<Jedis> connectionManager) {
		this.connectionManager = connectionManager;	
	}
}
