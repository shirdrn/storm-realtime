package org.shirdrn.storm.analytics;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.bolts.EventDistributionBolt;
import org.shirdrn.storm.analytics.bolts.EventStatResultPersistBolt;
import org.shirdrn.storm.analytics.bolts.EventStatisticsBolt;
import org.shirdrn.storm.analytics.constants.StatFields;
import org.shirdrn.storm.analytics.utils.StormComponentFactory;
import org.shirdrn.storm.analytics.utils.TestUtils;
import org.shirdrn.storm.analytics.utils.TopologyUtils;
import org.shirdrn.storm.commons.constants.Keys;

import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * Real-time event analytics, the topology definition stream graph is depict as follows:
 * <pre>
 * +------------+     +-----------------------+     +---------------------+     +----------------------------+
 * | KafkaSpout | --> | EventDistributionBolt | --> | EventStatisticsBolt | --> | EventStatResultPersistBolt |
 * +------------+     +-----------------------+     +---------------------+     +----------------------------+
 * </pre>
 * <ol>
 * 		<li>{@link KafkaSpout}                 </li> : Read data from Kafka MQ(topic: json_basis_event) 
 * 		<li>{@link EventDistributionBolt}      </li> : Distribute events
 * 		<li>{@link EventStatisticsBolt}        </li> : Event statistics
 * 		<li>{@link EventStatResultPersistBolt} </li> : Persist result to Redis, and insert into MySQL from Redis.
 * </ol>
 * 
 * @author yanjun
 */
public class RealtimeAnalyticsTopology {

	private static final Log LOG = LogFactory.getLog(RealtimeAnalyticsTopology.class);
	
	
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
		String kafkaEventReader = "kafka-event-reader";
		String eventDistributor = "event-distributor";
		String eventStatistics = "event-statistics";
		String eventStatResultPersist = "event-stat-result-persist";
		
		// configure Kafka spout
		BaseRichSpout kafkaSpout = null;
		if(isDebug) {
			kafkaSpout = TestUtils.getTestSpout();
		} else {
			kafkaSpout = StormComponentFactory.newKafkaSpout(topic, conf);
		}
		builder.setSpout(kafkaEventReader, kafkaSpout, 1);
		
		// configure distributor bolt
		builder
			.setBolt(eventDistributor, new EventDistributionBolt(), 1)
			.shuffleGrouping(kafkaEventReader)
			.setNumTasks(1);
		
		// configure statistics bolt
		builder
			.setBolt(eventStatistics, new EventStatisticsBolt(), 2)
			.shuffleGrouping(eventDistributor)
			.setNumTasks(2);
		
		// configure persistence bolt
		builder
			.setBolt(eventStatResultPersist, new EventStatResultPersistBolt(), 2)
			.fieldsGrouping(eventStatistics, new Fields(StatFields.STAT_INDICATOR))
			.setNumTasks(2);
		
		LOG.info("Topology built: " + TopologyUtils.toString(
				RealtimeAnalyticsTopology.class.getSimpleName(), 
				kafkaEventReader, eventDistributor, eventStatistics, eventStatResultPersist));
		return builder;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration externalConf = new PropertiesConfiguration("config.properties");
		String topic = externalConf.getString(Keys.KAFKA_BROKER_TOPICS);
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
		String name = RealtimeAnalyticsTopology.class.getSimpleName();
		
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
