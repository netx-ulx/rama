package net.floodlightcontroller.core.rama;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFConnection;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.util.OFBundle;
import net.floodlightcontroller.util.OFMessageUtils;
import static net.floodlightcontroller.core.rama.ContextKeys.getEventId;

import org.projectfloodlight.openflow.protocol.OFBundleCtrlMsg;
import org.projectfloodlight.openflow.protocol.OFBundleFailedCode;
import org.projectfloodlight.openflow.protocol.OFErrorMsg;
import org.projectfloodlight.openflow.protocol.OFErrorType;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.errormsg.OFBundleFailedErrorMsg;
import org.projectfloodlight.openflow.types.DatapathId;
import org.python.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;

public class BundleManager implements IBundleService, IFloodlightModule {

	private static final Set<OFBundleFailedCode> REASONS_TO_REPEAT_BUNDLE = Sets
			.immutableEnumSet(OFBundleFailedCode.MSG_CONFLICT,
					OFBundleFailedCode.MSG_FAILED,
					OFBundleFailedCode.MSG_TOO_MANY);

	static final long BUNDLE_TIMEOUT_MILIS = 500L;

	private final static ScheduledExecutorService scheduler = Executors
			.newScheduledThreadPool(1);

	/**
	 * The map of current event bundles in each thread for each switch.
	 */
	private Map<Long, Map<String, OFBundle>> currentBundles;

	/**
	 * The map of non event bundles in each thread for each switch.
	 */
	private Map<Long, Map<String, OFBundle>> nonEventBundles;
	
	private Set<Integer> sentBundles;

	private Logger log;

	/**
	 * ID of the event being processed in each thread
	 */
	private ThreadLocal<Long> currentEventId;

	private IReplicationService replicationService = null;

	private IFloodlightProviderService floodlightProvider;

	/**
	 * Sets the id of the current event being processed in the current thread
	 * 
	 * @param eventId
	 */
	@Override
	public void setCurrentEvent(long eventId) {
		currentEventId.set(eventId);
	}

	/**
	 * 
	 * @return the event id being processed in the current thread
	 * 
	 * @requires previous call to setCurrentEvent(long eventId)
	 */
	private long getCurrentEventId() {
		return currentEventId.get();
	}

	@Override
	public Collection<OFMessage> add(List<OFMessage> msgs, IOFSwitch sw,
			IOFConnection conn) {

		if (msgs.isEmpty()) {
			return Collections.emptyList();
		}

		long threadId = getCurrentThreadId();
		String swId = getSwitchId(sw);

		if (replicationService == null) {
			return getCurrentSwitchBundle(threadId, sw, swId).add(msgs);
		}

		if (!replicationService.isConnected(swId)) {
			log.info("Not sending msgs in bundle because switch not fully connected yet");
			return conn.write(msgs);
		}

		long eventId = getCurrentEventId();

		// add affected switch even in slave mode.
		// we need to know from which switches to expect commit replies
		if (eventId != -1) {
			addAffectedSwitchToCurrentEvent(swId, eventId);
		}

		if (!replicationService.isMaster(swId)) {
			log.info("BundleManager filtering outgoing messages from slave: "
					+ OFBundle.getMsgTypes(msgs));
			return Collections.emptyList();
		}

		OFBundle currentBundle;

		if (eventId != -1) {
			currentBundle = getCurrentSwitchBundle(threadId, sw, swId);
		} else {
			currentBundle = getSwitchNonEventBundle(threadId, sw);
		}

		if (currentBundle.isClosed() || currentBundle.isCommited()) {
			log.warn("Closed or committed bundle? " + currentBundle);
			return add(msgs, sw, conn);
		}

		// send messages inside BUNDLE_ADD
		Collection<OFMessage> unsent = currentBundle.add(msgs);
		if (unsent != null && unsent.isEmpty()) {
			// everything ok case
			// add event id if not there already
			currentBundle.setEventId(eventId);
		} else if (unsent == null) {
			// bundle was closed in the meanwhile??
			log.warn(
					"Bundle {} was closed in the meanwhile, adding these messages to a new bundle",
					currentBundle.getBundleId());
			return add(msgs, sw, conn);
		} else {
			// TODO: should not happen but...
			log.error("Error sending messages to switch " + getSwitchId(sw)
					+ ": " + unsent);
		}

		log.debug("BundleManager: added messages to bundle {}", currentBundle);
		return unsent;
	}

	/**
	 * Called after all modules process the OFMessage msg from the switch sw by
	 * Controller.java at the end of handleMessage(...) (can be initiated by the
	 * replicationService)
	 * 
	 * @param sw
	 * @param msg
	 * @param cntx
	 * @return
	 * @requires cntx != null && cntx.getStorage() != null
	 */
	@Override
	public void onMessageConsumed(final IOFSwitch sw, OFMessage msg,
			FloodlightContext cntx) {

		//log.debug("onMessageConsumed: sw={} msg={}", sw, msg);

		/*
		 * if (!isValidSwitch(sw)) { return; }
		 */
		long threadId = getCurrentThreadId();
		if (replicationService == null) {
			// if we are not using replicationService, commit bundles only
			finishBundles(getBundles(threadId), threadId);
			return;
		}

		long eventId = -1;

		try {
			eventId = getEventId(cntx);
		} catch (ClassCastException e) {
			log.error("Not a long in context storage for key "
					+ "BundleManager.CONTEXT_KEY. Assuming no event in current thread");
		}

		String swId = getSwitchId(sw);

		log.debug("onMessageConsumed: consumed event {} from switch {}: {}",
				new Object[] { eventId, swId, OFMessageUtils.msgToString(msg) });

		// if switch connected and we are slave
		if (replicationService.isConnected(swId)
				&& !replicationService.isMaster(swId)) {
			log.trace("Im slave, ignoring...");
			return;
		}

		// after here: we are master of the switch sw; the event is not from
		// replicationService
		// or it is but is unprocessed. or the switch is diconnected but we
		// still need to try and commit the bundles in other switches.

		// need to commit all bundles from all switches affected by the event
		OFBundle[] affectedBundles = getBundles(threadId);
		if (affectedBundles == null || affectedBundles.length == 0) {
			handleNullBundle(sw, swId, threadId, eventId);
		} else {
			finishBundles(affectedBundles, threadId);
		}
	}

	/**
	 * 
	 * @param sw
	 * @return true if this switch is able to use OFBundles
	 */
	@SuppressWarnings("unused")
	private boolean isValidSwitch(IOFSwitch sw) {
		return sw.getOFFactory().getVersion().compareTo(OFVersion.OF_14) >= 0;
	}

	private boolean finishBundles(OFBundle[] bundles, long threadId) {
		boolean result = true;
		for (OFBundle b : bundles) {
			if (!finishBundle(b, threadId)) {
				log.warn("Error finishing bundle: " + b);
				result = false;
			}
		}
		return result;
	}

	/**
	 * Checks the context to see if other modules instructed us to send or not
	 * the commands in the current bundle to switches. Modules do this by
	 * specifying a boolean in the context storage for the key
	 * BundleManager.CONTEXT_KEY
	 * 
	 * @param cntx
	 * 
	 * @return true if the controller should not send commands to the switches
	 * 
	 * @throws ClassCastException
	 *             if for some reason the value put for the key
	 *             BundleManager.CONTEXT_KEY is not a boolean
	 * 
	 * @requires cntx != null && cntx.getStorage() != null
	 */
	// private boolean stopCommandSubmission(FloodlightContext cntx)
	// throws ClassCastException {
	// return (cntx.getStorage().containsKey(ContextKeys.STOP_BUNDLE)) ?
	// (boolean) cntx
	// .getStorage().get(ContextKeys.STOP_BUNDLE) : false;
	// }

	/**
	 * An event may send commands to multiple switches. Therefore, BundleManager
	 * may have one bundle for each switch in a given thread. This method
	 * returns all current bundles opened by modules in the thread threadId that
	 * were created when sending commands to some switch.
	 * 
	 * @param threadId
	 *            the id of the thread
	 * @return all current bundles opened by the thread
	 */
	private OFBundle[] getBundles(long threadId) {
		Map<String, OFBundle> bundles = getThreadMap(currentBundles, threadId);
		if (bundles == null) {
			return null;
		} else if (bundles.size() == 0) {
			return new OFBundle[0];
		} else {
			// new OFBundle[0] needed to cast directly
			return (OFBundle[]) bundles.values().toArray(new OFBundle[0]);
		}
	}

	/**
	 * Notifies replicationService that a switch cannot send a commit reply so
	 * we should consider that it was sent and process it.
	 * 
	 * @param swId
	 *            the id of the switch for the bundle that cannot be committed
	 * @param eventIds
	 *            the ids of the events in the bundle
	 * @requires replicationService != null
	 */
	private void commitBundleFromDisconnectedSwitch(String sw, long eventId) {
		replicationService.processReceivedCommitReply(eventId, sw);
	}

	/**
	 * Handles the case of when an event does not generate any commands to a
	 * switch
	 * 
	 * @param swId
	 * @param threadId
	 * @return
	 */
	private boolean handleNullBundle(IOFSwitch sw, String swId, long threadId,
			long eventId) {
		if (sw == null || !sw.isConnected()
				|| sw instanceof OFDisconnectedSwitch) {
			log.debug("handleNullBundle but switch is disconnected");
			return true;
		}
		if (eventId != -1L) {
			// create new bundle and commit (only with PacketOut)
			OFBundle b = new OFBundle(sw);
			b.setEventId(eventId);
			log.debug("Opening and committing new bundle for event with no commands to switches: "
					+ b.toString());
			boolean result = doBundleCommit(b);
			return result;
		} else {
			log.warn(
					"No event being processed in thread {} when trying to handle empty bundle for switch {}",
					threadId, swId);
			return false;
		}
	}

	/**
	 * 
	 * @param b
	 * @param sw
	 * @param swId
	 * @param threadId
	 * @return
	 * @requires !b.isClosed() && !b.isCommitted()
	 */
	private boolean finishBundle(@Nonnull final OFBundle b, long threadId) {
		boolean result = true;

		IOFSwitch sw = b.getSwitch();

		// only commit bundle if switch is active
		if (sw != null && !(sw instanceof OFDisconnectedSwitch)) {
			result = doBundleCommit(b);
		} else if (replicationService != null) {
			log.info(
					"Will notify replicationService that switch {} cannot send commit reply for events {}",
					b.getSwitchId(), b.getEventId());
			commitBundleFromDisconnectedSwitch(b.getSwitchId(), b.getEventId());
		}
		/*
		 * Remove the bundle from the map after sending a close+commit request.
		 * We dont need to get it later. The bundle object will still exist but
		 * it is not associated with the map.
		 * 
		 * Removing scheduled closed/committed bundles from the map is important
		 * because next events may not send commands (that means no new bundle
		 * is opened) and those events need to be handled by handleNullBundle
		 */
		getCurrentBundlesMap(threadId).remove(getSwitchId(b.getSwitch()));
		return result;
	}

	/**
	 * After receiving the bundle commit from the switch, notifies
	 * replicationService to store the commit
	 * 
	 * @param bundleId
	 *            the id of the bundle
	 * @param eventId
	 *            the set of events completed
	 * @param swId
	 *            the id of the switch
	 * @param swConnected
	 *            if this switch is known to be connected or not
	 * @return
	 * @requires replicationService != null
	 */
	private void finishEventLoopProcessment(int bundleId, long eventId,
			String swId) {
		if (replicationService != null) {
			replicationService.storeCommit(bundleId, eventId, swId);
		} else {
			sentBundles.add(bundleId);
		}
	}

	/**
	 * When all modules processed the switch update notification, we may need to
	 * close and commit an empty bundle because maybe no modules sent any
	 * messages (cant send messages to disconnected switches...)
	 * 
	 * If some module sent messages to the switch as part of the event generated
	 * by the switch update
	 * 
	 * @param swId
	 * @param port
	 * @param changeType
	 */
	public void onSwitchUpdateFinish(DatapathId swDpId, IOFSwitch sw,
			OFPortDesc port, PortChangeType changeType, FloodlightContext cntx) {

		long threadId = getCurrentThreadId();
		long eventId = getCurrentEventId();
		if (cntx != null) {
			assert (eventId == cntx.getId());
		}
		String swId = getSwitchId(swDpId);

		log.debug(
				"onSwitchUpdateFinish(swId={}, sw={}, changeType={}) eventId={}",
				new Object[] { swId, sw, changeType, eventId });

		if (replicationService == null) {
			OFBundle b = getSwitchBundle(currentBundles, threadId,
					getSwitchId(sw));
			if (b != null) {
				doBundleCommit(b);
			} else {
				log.debug("No bundle for onSwitchUpdateFinish");
			}
			return;
		}

		// if ZKManager initialized this sw as a DCed switch
		if (sw == null || swId == null || sw instanceof OFDisconnectedSwitch
				|| sw.getId() == null) {
			log.info(
					"Will notify replicationService that event {} from the disconnected switch {} is processed.",
					eventId, swId);
			replicationService.onSwitchUpdateFinish(sw, cntx);
			return;
		}

		if (!replicationService.isConnected(swId)) {
			log.debug("Ignoring onSwitchUpdateFinish, sw not connected yet");
			return;
		}

		if (!replicationService.isMaster(swId)) {
			// if the switch is connected and we are slave, we wait commit reply
			log.debug("Im not switch master, will wait commit reply");
			return;
		}

		if (eventId == -1) {
			log.warn("No event being processed for onSwitchUpdateFinish.");
			return;
		}

		// if (cntx.getStorage().containsKey(ContextKeys.EVENT_ID)) {
		// assert (((long) cntx.getStorage().get(ContextKeys.EVENT_ID)) ==
		// eventId);
		// } else {
		// cntx.getStorage().put(ContextKeys.EVENT_ID, eventId);
		// }

		// we are master and all modules processed the switch update
		// or slave but switch not connected

		// make sure that this bundle was not committed because it also
		// included other event and this event was completed
		// (onMessageConsumed)
		log.debug("onSwitchUpdateFinish thread {}", threadId);
		OFBundle b = getSwitchBundle(currentBundles, threadId, getSwitchId(sw));
		if (b == null) {
			log.warn("No bundle was opened for switch {} in thread {}", swId,
					threadId);
		} else if (!b.isClosed() && !b.isCommited()) {
			log.debug("onSwitchUpdateFinish: committing bundle {}",
					b.toString());
			doBundleCommit(b);
		} else {
			log.debug("Bundle was already closed: " + b.toString());
		}

		// call replicationService here because SwitchManager does not know
		// replicationService
		replicationService.onSwitchUpdateFinish(sw, cntx);
	}

	/**
	 * 
	 * @param sw
	 * @requires previous call to setEvent
	 */
	@Override
	public void openEmptyBundle(IOFSwitch sw) {
		long eventId = getCurrentEventId();
		long threadId = getCurrentThreadId();
		String swId = getSwitchId(sw);
		log.info("openEmptyBundle(sw=" + sw + ", thread=" + threadId
				+ ", eventId=" + eventId + ")");
		OFBundle b = getCurrentSwitchBundle(threadId, sw, swId);
		b.setEventId(eventId);
	}

	/**
	 * Adds a PacketOut to the bundle before committing it.
	 * 
	 * If not using EventReplication, only commits the bundle.
	 * 
	 * @param b
	 * @requires b != null && b.getSwitch() != null
	 * @return
	 */
	private boolean doBundleCommit(final OFBundle b) {
		
		OFPacketOut pout = EventReplication.createPacketOutBundleCommitted(b,
				b.getSwitch());
		
		log.debug("doBundleCommit: Sending PacketOut and committing bundle {}", b.getBundleId());
		
		b.add(pout);

		if (replicationService != null) {
			replicationService.addAffectedSwitch(b.getEventId(), b.getSwitchId());
		}

		b.commit(new FutureCallback<OFBundleCtrlMsg>() {

			@Override
			public void onSuccess(OFBundleCtrlMsg arg0) {
				log.debug("Received commit reply message for bundle {}."
						+ "Reply: {}", b.getBundleId(), arg0);
				b.commitReplyReceived();
				finishEventLoopProcessment(b.getBundleId(), b.getEventId(),
						getSwitchId(b.getSwitch()));
			}

			@Override
			public void onFailure(Throwable arg0) {
				log.debug("Received failed commit reply message for "
						+ "bundle {}\n Error: {}", b.toString(), arg0);
				if (repeatBundle(arg0)) {
					repeatFailedBundle(b);
				} else {
					// cannot commit bundle nor repeat, assume switch
					// disconnected
					finishEventLoopProcessment(b.getBundleId(),
							b.getEventId(), getSwitchId(b.getSwitch()));
				}
			}

			private boolean repeatBundle(Throwable arg0) {
				if (!(arg0 instanceof OFMessage)) {
					return false;
				}

				OFMessage msg = (OFMessage) arg0;
				if (msg.getType() != OFType.ERROR) {
					return false;
				}

				OFErrorMsg error = (OFErrorMsg) msg;
				if (error.getErrType() != OFErrorType.BUNDLE_FAILED) {
					return false;
				}

				OFBundleFailedErrorMsg bError = (OFBundleFailedErrorMsg) error;
				if (REASONS_TO_REPEAT_BUNDLE.contains(bError.getCode())) {
					return true;
				} else {
					return false;
				}
			}
		});
		return true;
	}

	/**
	 * TODO
	 */
	private void repeatFailedBundle(OFBundle failedBundle) {
		// TODO: on commit failure, open two new bundles
		// each bundle has half of the messages in this failed bundle
	}

	private long getCurrentThreadId() {
		return Thread.currentThread().getId();
	}

	private Map<String, OFBundle> getCurrentBundlesMap(long threadId) {
		return currentBundles.get(threadId);
	}

	/**
	 * 
	 * @param sw
	 * @requires replicationService != null && eventId != -1
	 */
	private void addAffectedSwitchToCurrentEvent(String swId, long eventId) {
		if (replicationService.addAffectedSwitch(eventId, swId))
			log.debug("Added switch {} to event {}", swId, eventId);
	}

	/**
	 * Returns the current bundle being used for this switch in this thread for
	 * non events (commands generated by modules but not in response to an event
	 * by a switch). If no one is being used or the one being used is
	 * closed/committed, creates and returns new one.
	 * 
	 * @param threadId
	 * @param sw
	 * @requires sw != null
	 * @return
	 */
	private OFBundle getSwitchNonEventBundle(long threadId, IOFSwitch sw) {
		OFBundle currentBundle = getSwitchBundle(nonEventBundles, threadId,
				getSwitchId(sw));
		if (currentBundle == null || currentBundle.isClosed()
				|| currentBundle.isCommited()) {
			log.debug("Opening new (non event) bundle for switch {} "
					+ "in thread {}", getSwitchId(sw), threadId);
			currentBundle = new OFBundle(sw);
			updateSwitchBundle(nonEventBundles, threadId, getSwitchId(sw),
					currentBundle);

			// commit the bundle in 1 second
			if (currentBundle.getBundleId() % 100 == 0) {
				log.info("Committing bundle in " + BUNDLE_TIMEOUT_MILIS + "ms");
			}
			scheduler.schedule(new BundleCommiter(currentBundle),
					BUNDLE_TIMEOUT_MILIS, TimeUnit.MILLISECONDS);
		}
		return currentBundle;
	}

	private class BundleCommiter implements Runnable {

		private OFBundle b;

		public BundleCommiter(OFBundle bundle) {
			this.b = bundle;
		}

		@Override
		public void run() {
			log.debug("BundleCommiter activated. Sending PacketOut");
			doBundleCommit(b);
		}
	}

	/**
	 * Returns the current bundle being used for this switch in this thread. If
	 * no one is being used or the one being used is closed/committed, creates
	 * and returns new one.
	 * 
	 * @param threadId
	 * @param sw
	 * @requires sw != null
	 * @return
	 */
	private OFBundle getCurrentSwitchBundle(long threadId, IOFSwitch sw,
			String swId) {
		OFBundle currentBundle = getSwitchBundle(currentBundles, threadId, swId);
		if (currentBundle == null || currentBundle.isClosed()
				|| currentBundle.isCommited()) {
			currentBundle = new OFBundle(sw);
			updateSwitchBundle(currentBundles, threadId, swId, currentBundle);
			log.debug("New bundle for switch {} in thread {} opened: "
					+ currentBundle.toString(), swId, threadId);
		}
		return currentBundle;
	}

	/**
	 * Returns the current bundle being used for switch 'sw' in thread
	 * 'threadId' or null if there is no bundle for switch 'sw' in thread
	 * 'threadId'. Also returns null if there is no mapping for 'threadId'.
	 * 
	 * @param nonEventBundles2
	 * 
	 * @param threadId
	 * @param sw
	 * @requires sw != null
	 * @return
	 */
	private OFBundle getSwitchBundle(Map<Long, Map<String, OFBundle>> map,
			long threadId, String swId) {
		if (!map.containsKey(threadId))
			return null;

		return getThreadMap(map, threadId).get(swId);
	}

	/**
	 * 
	 * @param map
	 * @param threadId
	 * @param sw
	 * @param currentBundle
	 */
	private void updateSwitchBundle(Map<Long, Map<String, OFBundle>> map,
			long threadId, String swId, OFBundle currentBundle) {
		if (!map.containsKey(threadId))
			createThreadMap(map, threadId);

		OFBundle old = getThreadMap(map, threadId).put(swId, currentBundle);

		if (old != null && (!old.isClosed() || !old.isCommited())) {
			log.warn(
					"Replaced a not closed nor committed bundle.\nOld:\t{}\nNew:\t{}",
					old, currentBundle);
		}
	}

	/**
	 * The map of (switchId,bundle) of the thread with id threadId from the map
	 * map (currentBundles or nonEventBundles)
	 * 
	 * @param map
	 * @param threadId
	 * @return
	 */
	private Map<String, OFBundle> getThreadMap(
			Map<Long, Map<String, OFBundle>> map, long threadId) {
		return map.get(threadId);
	}

	private void createThreadMap(Map<Long, Map<String, OFBundle>> map,
			long threadId) {
		map.put(threadId, new ConcurrentHashMap<String, OFBundle>());
	}

	/**
	 * 
	 * @param sw
	 * @requires sw != null
	 * @return
	 */
	private String getSwitchId(IOFSwitch sw) {
		return sw == null ? "?" : getSwitchId(sw.getId());
	}

	/**
	 * 
	 * @param switchId
	 * @return
	 */
	private String getSwitchId(DatapathId switchId) {
		return switchId == null ? null : switchId.toString();
	}

	/*************************************************************************/
	/******************************* PRINTS **********************************/
	/*************************************************************************/

	@Override
	public void printCurrentBundles() {
		if (currentBundles.size() == 0 && nonEventBundles.size() == 0) {
			System.out.println("No bundles open in this controller.");
		} else {
			StringBuilder sb = new StringBuilder(
					"BundleManager maps:\ncurrentBundles:\n");
			appendThreadMap(sb, currentBundles);
			sb.append("Non event bundles:\n");
			appendThreadMap(sb, nonEventBundles);
			System.out.println(sb.toString());
		}
	}

	private void appendThreadMap(StringBuilder sb,
			Map<Long, Map<String, OFBundle>> map) {
		for (Long threadId : map.keySet()) {
			Map<String, OFBundle> switchMap = getThreadMap(map, threadId);
			for (String swId : switchMap.keySet()) {
				OFBundle b = switchMap.get(swId);
				sb.append("\t" + threadId + ": ");
				sb.append(swId + " -> id=" + b.getBundleId() + "; ");
				if (b.isClosed())
					sb.append("closed; ");
				if (b.isCommited())
					sb.append("committed; ");
				sb.append("events=" + b.getEventId() + ";\n");
				// switch loop in same thread
			}
			sb.append(""); // thread loop
		}
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IBundleService.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		m.put(IBundleService.class, this);
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {

		log = LoggerFactory.getLogger(BundleManager.class);

		log.info("BundleManager: on");

		this.floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);

		if (context.getAllServices().contains(IReplicationService.class)) {
			replicationService = context
					.getServiceImpl(IReplicationService.class);
			log.info("Using ReplicationService");
		} else {
			log.info("Did not found ReplicationService in FloodlightModuleContext");
		}
		currentBundles = new ConcurrentHashMap<Long, Map<String, OFBundle>>();
		nonEventBundles = new ConcurrentHashMap<Long, Map<String, OFBundle>>();
		sentBundles = ConcurrentHashMap.newKeySet();

		currentEventId = new ThreadLocal<Long>() {
			@Override
			public Long initialValue() {
				return new Long(-1);
			}
		};
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider.addCompletionListener(this);
	}

	@Override
	public String getName() {
		return this.getClass().getName().toString();
	}

	@Override
	public boolean addSentBundle(int bundleId) {
		return sentBundles.add(bundleId);
	}
}
