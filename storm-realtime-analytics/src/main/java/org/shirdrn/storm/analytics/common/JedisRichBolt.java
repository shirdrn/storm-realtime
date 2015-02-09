//package org.shirdrn.storm.analytics.common;
//
//import java.util.Map;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.log4j.Level;
//import org.shirdrn.storm.analytics.constants.Constants;
//import org.shirdrn.storm.analytics.utils.RealtimeUtils;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.base.BaseRichBolt;
//
//public abstract class JedisRichBolt extends BaseRichBolt {
//
//	private static final long serialVersionUID = 1L;
//	private static final Log LOG = LogFactory.getLog(JedisRichBolt.class);
//	protected OutputCollector collector;
//	
//	
//	@SuppressWarnings("rawtypes")
//	@Override
//	public void prepare(Map stormConf, TopologyContext context,
//			OutputCollector collector) {
//		this.collector = collector;
//		
//		// set print Redis cmd log level
//		Object level = stormConf.get(Constants.REALTIME_REDIS_CMD_LOG_LEVEL);
//		if(level != null) {
//			redisCmdLogLevel = RealtimeUtils.parseLevel((String) level);
//		}
//	}
//	
//	@Override
//	public void cleanup() {
//		super.cleanup();
//	}
//
//	public Level getRedisCmdLogLevel() {
//		return redisCmdLogLevel;
//	}
//
//}
