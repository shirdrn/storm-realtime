package org.shirdrn.storm.live;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.commons.constants.Keys;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.shirdrn.storm.commons.utils.TopologyUtils;
import org.shirdrn.storm.live.bolts.EventFilterBolt;
import org.shirdrn.storm.live.bolts.RealtimeStatisticsBolt;
import org.shirdrn.storm.live.constants.Constants;
import org.shirdrn.storm.live.utils.LiveRealtimeUtils;
import org.shirdrn.storm.live.utils.LiveSimulator;

import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * Online live analytics, the topology definition stream graph is depict as follows:
 * <pre>
 * +------------+     +-----------------+     +----------------+ 
 * | KafkaSpout | --> | EventFilterBolt | --> | OnlineStatBolt | 
 * +------------+     +-----------------+     +----------------+ 
 * </pre>
 * <ol>
 * 		<li>{@link KafkaSpout}           </li> : Read data from Kafka MQ(topic: json_basis_event) 
 * 		<li>{@link EventFilterBolt}      </li> : Filter and distribute events
 * 		<li>{@link RealtimeStatisticsBolt}       </li> : Online statistics
 * </ol>
 * 
 * @author yanjun
 */
public class LiveRealtimeAnalyticsTopology {

	private static final Log LOG = LogFactory.getLog(LiveRealtimeAnalyticsTopology.class);
	
	private static TopologyBuilder buildTopology(Configuration conf, String topic) {
		return buildTopology(false, conf, topic);
	}
	/**
	 * Configure a topology based on known Storm components(Spout, Bolt).
	 * @param conf
	 * @return
	 */
	private static TopologyBuilder buildTopology(boolean isDebug, Configuration conf, String topic) {
		LOG.info("Building topology...");
		TopologyBuilder builder = new TopologyBuilder();
		// naming Storm components
		String kafkaJsonReader = "kafka-reader";
		String eventFilter = "event-filter";
		String eventStat = "event-stat";
		
		// configure Kafka spout
		BaseRichSpout kafkaSpout = null;
		int spoutParallelism = 2;
		if(isDebug) {
			kafkaSpout = LiveSimulator.getSimulatedSpout();
			builder.setSpout(kafkaJsonReader, kafkaSpout, spoutParallelism);
			
			// configure event filter bolt
			builder
			.setBolt(eventFilter, new EventFilterBolt())
			.shuffleGrouping(kafkaJsonReader);
			
			// configure statistics bolt
			builder
			.setBolt(eventStat, new RealtimeStatisticsBolt())
			.fieldsGrouping(eventFilter, new Fields(Constants.FIELD_LIVE_ROOM_ID));
		} else {
			kafkaSpout = LiveRealtimeUtils.newKafkaSpout(topic, conf);
			builder.setSpout(kafkaJsonReader, kafkaSpout, spoutParallelism);
			
			// configure event filter bolt
			builder
			.setBolt(eventFilter, new EventFilterBolt(), 2)
			.shuffleGrouping(kafkaJsonReader)
			.setNumTasks(2);
			
			// configure statistics bolt
			builder
			.setBolt(eventStat, new RealtimeStatisticsBolt(), 4)
			.fieldsGrouping(eventFilter, new Fields(Constants.FIELD_LIVE_ROOM_ID))
			.setNumTasks(4);
		}

		LOG.info("Topology built: " + TopologyUtils.toString(
				LiveRealtimeAnalyticsTopology.class.getSimpleName(), 
					kafkaJsonReader, eventFilter, eventStat));
		return builder;
	}
	
	private static void checkArgs(String[] args) {
		if(!(args.length == 0 || args.length >= 1)) {
			LOG.error("");
			StringBuffer sb = new StringBuffer();
			sb
				.append("------------------------------------------------------------")
				.append("USAGE: \n")
				.append("    bin/storm jar /path/to/storm-realtime-live-<VERSION>.jar " + LiveRealtimeAnalyticsTopology.class.getName() + " <TOPOLOGY_NAME>[ <KAFKA_CLIENT_ID>]\n")
				.append("OPTIONS: \n")
				.append("    TOPOLOGY_NAME     Required. Topology name string.\n")
				.append("    KAFKA_CLIENT_ID   Optional. Kafka live client id.\n")
				.append("EXAMPLE: \n")
				.append("    bin/storm jar /home/storm/storm-realtime-live-0.2.0.jar " + LiveRealtimeAnalyticsTopology.class.getName() + " live_topology live_client_1")
				.append("------------------------------------------------------------");
			LOG.info(sb.toString());
			System.exit(1);
		}
	}
	
	public static void main(String[] args) throws Exception {
		checkArgs(args);
		
		Configuration externalConf = new PropertiesConfiguration("config.properties");
		String topic = externalConf.getString(Keys.KAFKA_BROKER_TOPICS);
		
		String kafkaLiveClientId = "LIVE_CLIENT_" + DateTimeUtils.format(System.currentTimeMillis(), "yyyyMMddHHmmssSSS");
		if(args.length > 1) {
			kafkaLiveClientId = args[1].trim();
		}
		LOG.info("Kakfa live client id: " + kafkaLiveClientId);
		externalConf.setProperty(Keys.KAFKA_CLIENT_ID, kafkaLiveClientId);
		
		// configure topology
		TopologyBuilder builder = null;
		if(args.length == 0) {
			builder = buildTopology(true, externalConf, topic);
		} else {
			builder = buildTopology(externalConf, topic);
		}
		
		// submit topology
		String nimbus = externalConf.getString(Keys.STORM_NIMBUS_HOST);
		Config stormConf = new Config();
		String name = LiveRealtimeAnalyticsTopology.class.getSimpleName();
		
		// add external configurations
		Iterator<String> iter = externalConf.getKeys();
		while(iter.hasNext()) {
			String key = iter.next();
			stormConf.put(key, externalConf.getProperty(key));
		}
		
		// production use
		if (args != null && args.length > 0) {
			name = args[0];
			stormConf.put(Config.NIMBUS_HOST, nimbus);
			stormConf.setNumWorkers(4);
			StormSubmitter.submitTopologyWithProgressBar(name, stormConf, builder.createTopology());
		} else {
			// debug using local cluster
			stormConf.setDebug(true);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, stormConf, builder.createTopology());
			int sleep = 60 * 60 * 1000;
			Thread.sleep(sleep);
			cluster.shutdown();
		}
	
	}
	
}
