package org.shirdrn.storm.live.bolts;

import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.shirdrn.storm.live.common.HeartbeatEvent;
import org.shirdrn.storm.live.common.JedisRichBolt;
import org.shirdrn.storm.live.constants.Constants;
import org.shirdrn.storm.live.constants.EventCode;
import org.shirdrn.storm.live.constants.EventKeys;
import org.shirdrn.storm.live.handler.AbstractHeartbeatEventHandler;
import org.shirdrn.storm.live.handler.HeartbeatEventHandler;
import org.shirdrn.storm.live.handler.JedisEventHandler;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Maps;

public class RealtimeStatisticsBolt extends JedisRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(RealtimeStatisticsBolt.class);
	private final Map<String, JedisEventHandler<?, ?, ?>> statHandlers = Maps.newHashMap();
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		statHandlers.put(EventCode.HEART_BEAT, new HeartbeatEventHandler(this, EventCode.HEART_BEAT));
	}

	@Override
	public void execute(Tuple input) {
		boolean processed = false;
		try {
			String room = input.getString(0);
			JSONObject data = (JSONObject) input.getValue(1);
			String eventCode = data.getString(EventKeys.EVENT_CODE);
			
			if(eventCode.equals(EventCode.ENTER) || eventCode.equals(EventCode.HEART_BEAT)) {
				// Add heartbeat packet to queue, and a background thread to check
				// the heartbeat timestamp
				if(data.containsKey(EventKeys.UDID) 
						&& data.containsKey(EventKeys.EVENT_TIME) 
						&& data.containsKey(EventKeys.FRAGMENT_ID)) {
					HeartbeatEvent hbEvent = new HeartbeatEvent();
					hbEvent.setEventCode(eventCode);
					hbEvent.setRoom(room);
					hbEvent.setUdid(data.getString(EventKeys.UDID));
					String eventTime = data.getString(EventKeys.EVENT_TIME);
					hbEvent.setEventTime(eventTime);
					hbEvent.setEventTimestamp(DateTimeUtils.getTimestamp(eventTime, Constants.DT_EVENT_PATTERN));
					hbEvent.setFragmentId(data.getString(EventKeys.FRAGMENT_ID));
					hbEvent.setInput(input);
					hbEvent.setCollector(collector);
					AbstractHeartbeatEventHandler handler = (AbstractHeartbeatEventHandler) statHandlers.get(EventCode.HEART_BEAT);
					handler.handle(hbEvent);
					processed = true;
					// we will ack this tuple in our {@link HeartbeatEventHandler}
				}
			}
			
		} catch (Exception e) {
			LOG.warn("Abnormal JSON format event data: ", e);
		} finally {
			// here, send back a ack to avoid upstream component waiting this bolt's
			if(!processed) {
				collector.ack(input);
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	@Override
	public void cleanup() {
		super.cleanup();
	}

}
