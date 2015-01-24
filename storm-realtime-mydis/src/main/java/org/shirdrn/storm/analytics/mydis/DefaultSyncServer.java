package org.shirdrn.storm.analytics.mydis;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncServer;
import org.shirdrn.storm.analytics.mydis.common.SyncServer;
import org.shirdrn.storm.analytics.mydis.common.SyncWorker;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.analytics.mydis.constants.StatIndicators;
import org.shirdrn.storm.analytics.mydis.workers.StatSyncWorker;
import org.shirdrn.storm.analytics.mydis.workers.UserSyncWorker;

public class DefaultSyncServer extends RedisSyncServer {

	private static final Log LOG = LogFactory.getLog(DefaultSyncServer.class);
	
	public DefaultSyncServer(Configuration conf) {
		super(conf);
	}
	
	static void register(SyncServer syncServer, SyncWorker<?, ?> syncWorker) {
		syncServer.registerSyncWorkers(syncWorker);
		LOG.info("SyncWorker registed: " + syncWorker);
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new PropertiesConfiguration("config.properties");
		RedisSyncServer syncServer = new DefaultSyncServer(conf);
		// add SyncWorker
		register(syncServer, new StatSyncWorker(syncServer));
		register(syncServer,
				new UserSyncWorker(syncServer, StatIndicators.PLAY_NU_DURATION, Constants.NS_PLAY_NU_DURATION_USER));
		register(syncServer,
				new UserSyncWorker(syncServer, StatIndicators.PLAY_AU_DURATION, Constants.NS_PLAY_AU_DURATION_USER));
		// start server
		syncServer.start();
		syncServer.join();
	}

}
