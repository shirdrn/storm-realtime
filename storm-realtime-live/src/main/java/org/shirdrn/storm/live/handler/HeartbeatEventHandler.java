package org.shirdrn.storm.live.handler;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.shirdrn.storm.live.bolts.EventFilterBolt;
import org.shirdrn.storm.live.common.HeartbeatEvent;
import org.shirdrn.storm.live.common.JedisRichBolt;
import org.shirdrn.storm.live.common.LiveConfiguration;
import org.shirdrn.storm.live.common.QueuedManager;
import org.shirdrn.storm.live.constants.Constants;
import org.shirdrn.storm.live.constants.EventCode;
import org.shirdrn.storm.live.utils.LiveRealtimeUtils;

import redis.clients.jedis.Jedis;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

/**
 * Process live heart beat messages, and compute the following 
 * statistical indicators for each live room:
 * <ol>
 * 	<li>Online user count</li>
 * 	<li>Maximum online user count</li>
 * 	<li>Accumulated user count</li>
 * </ol>
 * 
 * @author yanjun
 */
public class HeartbeatEventHandler extends AbstractHeartbeatEventHandler {

	private static final long serialVersionUID = -6240487773785663422L;
	private static final Log LOG = LogFactory.getLog(HeartbeatEventHandler.class);
	
	private static final String STR_ZERO = "0";
	private static final String ROOMID_FRAGMENTID_SEPARATOR = "_";
	private static final String DATETIME_READABLE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final String DATE_READABLE_FORMAT = "yyyyMMdd";
	
	private final LiveConfiguration config;
	private final ExecutorService executorService;
	private final ConcurrentMap<String, Long> timestampedRooms = Maps.newConcurrentMap();
	// Map<roomid, roomid_fragmentid>
	private final ConcurrentMap<String, String> roomKeyedFragments = Maps.newConcurrentMap();
	private final Cache<RoomFragmentKey, String> roomFragmentPairCache;
	
	private final CounterRefresher counterRefresher;
	private final MaximumOnlineUserCountCalculator maximumOnlineUserCountCalculator;
	private final AckedWorker ackedWorker;
	private final HeartbeatRenewer heartbeatRenewer;
	private final AtomicBoolean executingPurgeOperation = new AtomicBoolean(false);
	private long lastPurgeTimestamp = System.currentTimeMillis();
	
	public HeartbeatEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
		
		config = new LiveConfiguration(jedisBolt.getStormConf());
		
		// create room fragment pair cache
		roomFragmentPairCache = CacheBuilder.newBuilder().maximumSize(config.getRoomFragmentPairCacheCapacity()).build();
		
		// create a cached thread pool
		executorService = Executors.newCachedThreadPool();
		LOG.info("Cached thread pool created: " + executorService);
		
		ackedWorker = new AckedWorker();
		heartbeatRenewer = new HeartbeatRenewer();
		maximumOnlineUserCountCalculator = new MaximumOnlineUserCountCalculator();
		counterRefresher = new CounterRefresher();
		
		startComputationWorkers();
	}

	private void startComputationWorkers() {
		// start ACK worker
		executorService.execute(ackedWorker);
		LOG.info("ACK worker started: " + ackedWorker);
		
		// start heart beat renewer thread
		executorService.execute(heartbeatRenewer);
		LOG.info("Heartbeat renewer started: " + heartbeatRenewer);
		
		// calculate maximum online user count
		executorService.execute(maximumOnlineUserCountCalculator);
		LOG.info("Maximum online user count calculator started: " + maximumOnlineUserCountCalculator);
		
		executorService.execute(counterRefresher);
		LOG.info("Room counter refresher started: " + counterRefresher);
	}

	@Override
	public TreeSet<Void> handle(HeartbeatEvent event) {
		String roomId = event.getRoomId();
		String fragmentId = event.getFragmentId();
		String eventCode = event.getEventCode();
		if(eventCode.equals(EventCode.ENTER) || eventCode.equals(EventCode.HEART_BEAT)) {
			// check whether a new room appears, if true then add room to the shared set
			checkRoomCache(roomId, fragmentId);

			// renewer heart beat
			heartbeatRenewer.addLast(event);
		} 
		return null;
	}

	private void checkRoomCache(String roomId, String fragmentId) {
		if(!timestampedRooms.containsKey(roomId)) {
			long timestamp = System.currentTimeMillis();
			timestampedRooms.putIfAbsent(roomId, timestamp);
			String roomFragmentPair = new RoomFragmentKey(roomId, fragmentId).toKey();
			roomKeyedFragments.put(roomId, roomFragmentPair);
			LOG.info("Room added: room=" + roomId + ", fragment=" + fragmentId +
					", timestamp=" + timestamp + 
					"(" + DateTimeUtils.format(timestamp, DATETIME_READABLE_FORMAT) + ")");
			
			// check room cache, kick off expired rooms
			if(timestampedRooms.size() > config.getRoomCacheCapacity()
					&& System.currentTimeMillis() - lastPurgeTimestamp > config.getRoomCachePurgeInterval()
					&& executingPurgeOperation.compareAndSet(false, true)) {
				long current = System.currentTimeMillis();
				LOG.info("Before purging: " + 
						"roomCacheSize=" + timestampedRooms.size() + 
						", fragmentCacheSize=" + roomKeyedFragments.size() + 
						", lastPurgeTime=" + DateTimeUtils.format(lastPurgeTimestamp, DATETIME_READABLE_FORMAT) + 
						", currentTime=" + DateTimeUtils.format(current, DATETIME_READABLE_FORMAT));
				purgeExpirations();
				lastPurgeTimestamp = System.currentTimeMillis();
				// unlock
				executingPurgeOperation.set(false);
			}
		}
	}
	
	private void purgeExpirations() {
		executorService.execute(new Runnable() {

			@Override
			public void run() {
				LOG.info("Reached capacity(" + config.getRoomCacheCapacity() + "), try to check & kick off from cache: ");
				Iterator<Entry<String, Long>> iter = timestampedRooms.entrySet().iterator();
				int removed = 0;
				Map<String, String> resetRoomIds = Maps.newHashMap();
				while(iter.hasNext()) {
					Entry<String, Long> entry = iter.next();
					String cachedRoomId = entry.getKey();
					Long ts = entry.getValue();
					if(System.currentTimeMillis() - ts >= config.getRoomCacheExpireTime()) {
						iter.remove();
						// remove room_fragmentid pair
						roomKeyedFragments.remove(cachedRoomId);
						removed += 1;
						LOG.info("Kicked off: key=" + cachedRoomId + ", ts=" + ts + 
								"(" + DateTimeUtils.format(ts, DATETIME_READABLE_FORMAT) + ")");
						// room counter need to be reset to 0
						resetRoomIds.put(cachedRoomId, STR_ZERO);
					}
				}
				LOG.info(removed + " expirations have been kicked off.");
				LOG.info("After purged: roomCacheSize=" + timestampedRooms.size() + ", fragmentCacheSize=" + roomKeyedFragments.size());
				
				// update counter for the removed room to 0
				resetRoomCounters(resetRoomIds);
			}

			private void resetRoomCounters(Map<String, String> resetRoomIds) {
				Jedis connection = null;
				try {
					if(!resetRoomIds.isEmpty()) {
						String liveRoomUserPlayCounterKey = Constants.KEY_LIVE_ROOM_USER_PLAY_COUNT;
						connection = jedisBolt.getJedis();
						connection.hmset(liveRoomUserPlayCounterKey, resetRoomIds);
						LiveRealtimeUtils.printRedisCmd(LOG, "HMSET " + liveRoomUserPlayCounterKey + " " + resetRoomIds);
					} 
				} catch(Exception e) {
				} finally {
					LiveRealtimeUtils.closeQuietly(connection);
				}
			}
			
		});
	}
	
	/**
	 * Refresh room user counter periodically. We'll try to
	 * get heart beat messages stored in Redis, and count
	 * the users belonging to a given room.
	 * 
	 * @author yanjun
	 */
	private class CounterRefresher extends Thread {
		
		private final Log logger = LogFactory.getLog(CounterRefresher.class);
		private final String heartbeatKeyPrefix;
		private final String liveRoomUserPlayCounterKey = Constants.KEY_LIVE_ROOM_USER_PLAY_COUNT;
		
		public CounterRefresher() {
			super();
			heartbeatKeyPrefix = Constants.KEY_LIVE_HEART_BEAT + Constants.KEY_REDIS_NS_SEPARATOR;
		}
		
		@Override
		public void run() {
			// enter to check and compute user count for each room registered
			while(true) {
				Jedis connection = null;
				try {
					if(!timestampedRooms.isEmpty()) {
						connection = jedisBolt.getJedis();
						// Map<room, count>
						Map<String, String> currentRoomCounterMap = Maps.newHashMap();
						// Map<RoomFragmentKey, count>
						Map<RoomFragmentKey, String> currentRoomFragmentidCounterMap = Maps.newHashMap();
						String nowDate = DateTimeUtils.format(System.currentTimeMillis(), DATE_READABLE_FORMAT);
						for(String roomId : timestampedRooms.keySet()) {
							// key like: lhb::1000000000::2000000000::ad09dad86caa399497ca9a53f0b9abaf
							String keyPattern = 
									heartbeatKeyPrefix + roomId + Constants.KEY_REDIS_NS_SEPARATOR + "*";
							Set<String> keys = connection.keys(keyPattern);
							LiveRealtimeUtils.printRedisCmd(logger, Level.DEBUG, "KEYS " + keyPattern);
							
							if(keys != null && !keys.isEmpty()) {
								String count = String.valueOf(keys.size());
								currentRoomCounterMap.put(roomId, count);
								String key = keys.iterator().next();
								String[] a = key.split(Constants.KEY_REDIS_NS_SEPARATOR);
								
								// no need to store 0 counter for computing maximum online user count
								if(!STR_ZERO.equals(count) && a.length == 4) {
									String fragmentId = a[2];
									currentRoomFragmentidCounterMap.put(new RoomFragmentKey(roomId, fragmentId), count);
								}
							} else {
								currentRoomCounterMap.put(roomId, STR_ZERO);
							}
						}
						
						// HMSET
						if(!currentRoomCounterMap.isEmpty()) {
							// single record example:
							// HSET l::play::u::cnt fragmentid usercount
							// HSET l::play::u::cnt 1000000000 29
							connection.hmset(liveRoomUserPlayCounterKey, currentRoomCounterMap);
							LiveRealtimeUtils.printRedisCmd(logger, Level.DEBUG, "HMSET " + liveRoomUserPlayCounterKey + " " + currentRoomCounterMap);
							
							// print log information
							Map<String, String> map = removeZeroCounters(currentRoomCounterMap);
							if(!map.isEmpty()) {
								logger.info("ROOM: " + map);
							}
						}
						
						if(!currentRoomFragmentidCounterMap.isEmpty()) {
							// collect data to compute max user count of each room on daily basis
							maximumOnlineUserCountCalculator.addLast(new CountersHolder(nowDate, currentRoomFragmentidCounterMap));
						}
						
						// compute accumulated users for each fragment contained in a room
						computeAccumulatedUsers(nowDate, connection);
					}
				} catch (Exception e) {
					logger.warn(this.getClass().getSimpleName() + ": ", e);
				} finally {
					try {
						LiveRealtimeUtils.closeQuietly(connection);
						Thread.sleep(config.getRoomedUserCounterRefreshInterval());
					} catch (InterruptedException e) { }
				}
			}
			
		}

		private Map<String, String> removeZeroCounters(Map<String, String> map) {
			Iterator<Entry<String, String>> iter = map.entrySet().iterator();
			while(iter.hasNext()) {
				if(STR_ZERO.equals(iter.next().getValue())) {
					iter.remove();
				}
			}
			return map;
		}

		/**
		 * Compute accumulated users for every fragment
		 * @param nowDate
		 * @param connection
		 */
		private void computeAccumulatedUsers(String nowDate, Jedis connection) {
			Map<String, String> accumulatedUsers = Maps.newHashMap();
			for(String roomIdFragmentId : roomKeyedFragments.values()) {
				// SCARD 1000000000_2000000000
				Long accuUserCnt = connection.scard(roomIdFragmentId);
				if(accuUserCnt != null) {
					accumulatedUsers.put(roomIdFragmentId, accuUserCnt.toString());
				}
			}
			if(!accumulatedUsers.isEmpty()) {
				connection.hmset(nowDate, accumulatedUsers);
			}
		}
		
	}
	
	/**
	 * Hold a room id and fragment id pair, such as:
	 * <pre>
	 * if:
	 * 		roomId = 1000000000, fragmentId = 2000000000
	 * then:
	 * 		pair = 1000000000_2000000000
	 * </pre>
	 *  
	 * @author yanjun
	 */
	private class RoomFragmentKey {
		
		private final String roomId;
		private final String fragmentId;
		
		public RoomFragmentKey(String roomId, String fragmentId) {
			super();
			this.roomId = roomId;
			this.fragmentId = fragmentId;
		}
		
		@Override
		public int hashCode() {
			return 31 * roomId.hashCode() + 31 * fragmentId.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			RoomFragmentKey other = (RoomFragmentKey) obj;
			return other.hashCode() == this.hashCode();
		}

		public String toKey() {
			// cache composite key, if necessary
			String roomfragmentStrKey = roomFragmentPairCache.getIfPresent(this);
			if(roomfragmentStrKey == null) {
				roomfragmentStrKey = roomId + ROOMID_FRAGMENTID_SEPARATOR + fragmentId;
				roomFragmentPairCache.put(this, roomfragmentStrKey);
			}
			return roomfragmentStrKey;
		}
	}
	
	/**
	 * Heartbeat updater for registered rooms. A updater
	 * can fetch heartbeat message from deque, and renew the
	 * heartbeat timestamp by resetting expire time.
	 * 
	 * @author yanjun
	 */
	public class HeartbeatRenewer extends QueuedManager<HeartbeatEvent> {
		
		private final Log log = LogFactory.getLog(HeartbeatRenewer.class);
		protected final String liveHeartbeatKeyPrefix;
		
		public HeartbeatRenewer() {
			super();
			liveHeartbeatKeyPrefix = Constants.KEY_LIVE_HEART_BEAT + Constants.KEY_REDIS_NS_SEPARATOR;
		}
		
		@Override
		public void run() {
			while(true) {
				Jedis connection = null;
				HeartbeatEvent hb = super.getQueue().pollFirst();
				boolean caughtException = false;
				try {
					if(hb == null) {
						Thread.sleep(200);
						continue;
					}
					
					connection = jedisBolt.getJedis();
					// process heart beat event
					processHeartbeat(connection, hb);
				} catch (Exception e) {
					// save this heartbeat's life
					if(hb != null) {
						super.getQueue().addFirst(hb);
					}
					caughtException = true;
					log.error(this.getClass().getSimpleName() + ": ", e);
				} finally {
					LiveRealtimeUtils.closeQuietly(connection);
					// put being acked input tuple to a separated thread
					if(!caughtException && hb != null && hb.getInput() != null) {
						ackedWorker.addLast(hb);
					}
				}
			}
		}
		
		private void processHeartbeat(final Jedis connection, final HeartbeatEvent hb) {
			String udid = hb.getUdid();
			String roomId = hb.getRoomId();
			String fragmentId = hb.getFragmentId();
			// lhb::1000000000::2000000000::a90eaa54aba319bd7c4bb0df7bdadae 1426479011000
			String heartbeatKey = 
					liveHeartbeatKeyPrefix + 
					roomId + Constants.KEY_REDIS_NS_SEPARATOR + 
					fragmentId + Constants.KEY_REDIS_NS_SEPARATOR + udid;
			
			// update heartbeat's timestamp for this user in the room
			connection.setex(heartbeatKey, config.getMaxHeartbeatExpireInterval(), Long.toString(hb.getEventTimestamp()));
			LiveRealtimeUtils.printRedisCmd(log, "SETEX " + heartbeatKey + " " + config.getMaxHeartbeatExpireInterval() + " " + hb.getEventTimestamp());
			
			// store to compute accumulated users
			// SADD 1000000000_2000000000 c90eaa542fe319bd7c4bb0df7bd11ae
			// SADD 1000000000_2000000000 b1986a92f5fa0355bd57de3d1742795b
			String compositeKey = new RoomFragmentKey(roomId, fragmentId).toKey();
			connection.sadd(compositeKey, udid);
		}
		
	}
	
	private final class CountersHolder {
		
		private final String nowDate;
		private final Map<RoomFragmentKey, String> namedCounters;
		
		public CountersHolder(String nowDate, Map<RoomFragmentKey, String> counters) {
			super();
			this.nowDate = nowDate;
			this.namedCounters = counters;
		}
		
	}
	
	/**
	 * Compute maximum online user count on daily basis. Here we compute
	 * user count of a room occupied by a fragment. Because each room can arrange
	 * multiple fragments for different time range, we should record the maximum 
	 * user count for each fragment.</br>
	 *
	 * @author yanjun
	 */
	private final class MaximumOnlineUserCountCalculator extends QueuedManager<CountersHolder> {
		
		private final Log logger = LogFactory.getLog(MaximumOnlineUserCountCalculator.class);
		private final String keyPrefix = Constants.KEY_LIVE_MAX_USER_COUNT_PREFIX;
		
		public MaximumOnlineUserCountCalculator() {
			super();
		}
		
		@Override
		public void run() {
			while(true) {
				Jedis connection = null;
				CountersHolder countersHolder = super.getQueue().pollFirst();
				try {
					if(countersHolder == null) {
						Thread.sleep(300);
						continue;
					}
					connection = jedisBolt.getJedis();
					computeMaxOnlineUsers(connection, countersHolder);
				} catch (Exception e) {
					// push back to the head of the queue
					if(countersHolder != null) {
						super.getQueue().addFirst(countersHolder);
					}
					logger.error(this.getClass().getSimpleName() + ": ", e);
				} finally {
					LiveRealtimeUtils.closeQuietly(connection);
				}
			}
		}
		
		/**
		 * Compute maximum online user count.
		 * @param connection
		 * @param counterHolder
		 */
		private void computeMaxOnlineUsers(Jedis connection, CountersHolder counterHolder) {
			if(!counterHolder.namedCounters.isEmpty()) {
				// HGETALL l::max::cnt::20151101
				String key = keyPrefix + Constants.KEY_REDIS_NS_SEPARATOR + counterHolder.nowDate;
				Map<String, String> data = connection.hgetAll(key);
				Map<String, String> changedData = Maps.newHashMap();
				LiveRealtimeUtils.printRedisCmd(logger, "HGETALL " + key);
				
				if(data == null) {
					data = Maps.newHashMap();
				}
				Iterator<Entry<RoomFragmentKey, String>> iter = counterHolder.namedCounters.entrySet().iterator();
				while(iter.hasNext()) {
					Map.Entry<RoomFragmentKey, String> entry = iter.next();
					RoomFragmentKey roomFragmentKey = entry.getKey();
					String currentCount = entry.getValue();
					// room_fragmentid
					// 1000000000_2000000000
					String compositeKey = roomFragmentKey.toKey();
					String currentMaxCount = data.get(compositeKey);
					if(currentMaxCount != null) {
						try {
							if(Integer.parseInt(currentCount) > Integer.parseInt(currentMaxCount)) {
								changedData.put(compositeKey, currentCount);
							}
						} catch (Exception e) {
							LOG.warn("Ignore it: realtimeCount=" + currentCount + ", maxCount=" + currentMaxCount, e);
							// ignore it
						}
					} else {
						// first time to save max user counter data
						changedData.put(compositeKey, currentCount);
					}
				}
				
				if(!changedData.isEmpty()) {
					// HSET l::max::cnt::20151101 1000000000_2000000000 106
					connection.hmset(key, changedData);
					LiveRealtimeUtils.printRedisCmd(logger, "HMSET " + key + " " + changedData);
				}
			}
		}

	}
	
	/**
	 * After finishing to process heart beat messages, we should acknowledge 
	 * the upstream component {@link EventFilterBolt}.
	 *
	 * @author yanjun
	 */
	private final class AckedWorker extends QueuedManager<HeartbeatEvent> {
		
		public AckedWorker() {
			super();
		}
		
		@Override
		public void run() {
			while(true) {
				HeartbeatEvent hb = null;
				try {
					hb = super.getQueue().pollFirst();
					if(hb != null) {
						hb.getCollector().ack(hb.getInput());
					} else {
						Thread.sleep(200);
					}
				} catch (Exception e) {
					if(hb != null) {
						super.getQueue().addFirst(hb);
					}
				}
			}
		}
		
	}

}
