package org.shirdrn.storm.analytics.bolts;

import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.analytics.common.KeyedObject;
import org.shirdrn.storm.analytics.common.LazyCallback;
import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.analytics.constants.StatIndicators;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class EventStatResultPersistBolt extends JedisRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventStatResultPersistBolt.class);
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	}
	
	@Override
	public void execute(Tuple input) {
		int indicator = input.getInteger(0);
		AbstractResult obj = (AbstractResult) input.getValue(1);
		LOG.debug("INPUT: indicator=" + indicator + ", obj=" + obj);
		processOne(input, indicator, obj);
	}

	@SuppressWarnings("unchecked")
	private void processOne(Tuple input, int indicator, AbstractResult obj) {
		switch(indicator) {
			case StatIndicators.AU:
			case StatIndicators.NU:
			case StatIndicators.PLAY_AU:
			case StatIndicators.PLAY_NU:
			case StatIndicators.LAUNCH_TIMES:
			case StatIndicators.PLAY_TIMES:
			case StatIndicators.PLAY_NU_DURATION:
			case StatIndicators.PLAY_AU_DURATION:
				StatResult statResult = (StatResult) obj;
				// <key, field, value> like: 
				// <2015011520, 11::0::A-Baidu::3.1.0, 43997>
				// explanation: hour->2015011520, NU->11, os type->0, channel->A-Baidu, version->3.1.0, counter->43997
				String key = statResult.getStrHour();
				String field = statResult.toField();
				LazyCallback<Jedis> callback = statResult.getCallback();
				if(callback != null) {
					try {
						callback.call(super.getJedis());
						collector.ack(input);
					} catch (Exception e) {
						LOG.error("Fail to update value for: key=" + key + ", field=" + field, e);
						collector.fail(input);
					}
				}
				break;
				
			case StatIndicators.USER_DEVICE_INFO:
			case StatIndicators.USER_DYNAMIC_INFO:
				KeyedObject<JSONObject> keyedObject = (KeyedObject<JSONObject>) obj;
				String k = keyedObject.getKey();
				JSONObject v = keyedObject.getObject();
				// user device information:
				// <key, value> like: 
				// key  -> us::9d11f3ee0242a15026e51d1b3efba454
				// value-> {"aid": "0", "dt":"0", "ch":"A-Baidu", "v":"1.2.7"}
				
				// user dynamic information:
				// <key, value> like:
				// key  -> ud::9d11f3ee0242a15026e51d1b3efba454
				// field-> fod  fpd
				// value-> 2015-01-15
				callback = keyedObject.getCallback();
				if(callback != null) {
					try {
						callback.call(super.getJedis());
						collector.ack(input);
					} catch (Exception e) {
						LOG.error("Fail to update value for: k=" + k + ", v=" + v, e);
						collector.fail(input);
					}
				}
				break;
				
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
