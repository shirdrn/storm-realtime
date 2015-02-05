package org.shirdrn.storm.analytics.common;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

import com.google.common.collect.Sets;

public abstract class AbstractJedisEventHandler extends
		JedisEventHandler<TreeSet<AbstractResult>, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(AbstractJedisEventHandler.class);
	
	public AbstractJedisEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
	}
	
	@Override
	public TreeSet<AbstractResult> handle(JSONObject event) throws Exception {
		Collection<Integer> mappedIndicators = getMappedIndicators();
		LOG.info(this.getClass().getSimpleName() + ": indicators=" + mappedIndicators);
		TreeSet<AbstractResult> results = Sets.newTreeSet();
		for(int indicator : mappedIndicators) {
			compute(results, indicator, event);
		}
		LOG.info(this.getClass().getSimpleName() + ": results=" + results);
		return results;
	}
	
	protected void compute(TreeSet<AbstractResult> results, int indicator, JSONObject event) {
		Jedis jedis = jedisBolt.getJedis();
		IndicatorCalculator<? extends AbstractResult> calculator = selectCalculator(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for indicator: " + indicator);
		}
		calculator.setPrintRedisCmdLogLevel(jedisBolt.getRedisCmdLogLevel());
		AbstractResult result = calculator.calculate(jedis, event);
		if(result != null) {
			results.add(result);
		}
		jedisBolt.returnResource(jedis);
	}
	
}
