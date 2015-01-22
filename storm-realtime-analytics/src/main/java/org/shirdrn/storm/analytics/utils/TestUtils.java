package org.shirdrn.storm.analytics.utils;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.constants.StatFields;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestUtils {

	public static BaseRichSpout getTestSpout() {
		return new TestSpout();
	}
	
	public static class TestSpout extends BaseRichSpout {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(TestSpout.class);
		private String[] events;
		int pointer = 0;
		SpoutOutputCollector collector;
		
		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
			events = new String[] {
					// install
					"{\"event_code\":\"400000\",\"install_id\":\"000000HX\",\"udid\":\"AQQQQQQQQQQQQQQQQQQQQQQQ\",\"event_time\":\"2055-11-11 00:51:12\",\"app_key\":\"1\",\"os_type\":0,\"version\":\"3.1.2\",\"channel\":\"A-xop\"}",
					// open
					"{\"event_code\":\"100010\",\"install_id\":\"000000HX\",\"udid\":\"AQQQQQQQQQQQQQQQQQQQQQQQ\",\"event_time\":\"2055-11-11 11:11:48\"}",
					// play start
					"{\"event_code\":\"101013\",\"install_id\":\"000000HX\",\"udid\":\"AQQQQQQQQQQQQQQQQQQQQQQQ\",\"event_time\":\"2055-11-11 22:00:15\"}",
					// play end
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"AQQQQQQQQQQQQQQQQQQQQQQQ\",\"event_time\":\"2055-11-11 22:42:09\",\"duration\":\"9999\"}"
			};
			
		}

		@Override
		public void nextTuple() {
			if(pointer < events.length) {
				String data = events[pointer];
				collector.emit(new Values(data));
				LOG.info("Spout emitted: " + data);
				++pointer;
				Utils.sleep(3000);
			} else {
				Utils.sleep(100000);
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields(StatFields.EVENT_DATA));			
		}
		
	}
}
