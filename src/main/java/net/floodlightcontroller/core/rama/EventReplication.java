package net.floodlightcontroller.core.rama;

import static net.floodlightcontroller.core.rama.ContextKeys.getEventId;
import static net.floodlightcontroller.core.internal.Controller.getInitialContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchManager;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.internal.IPipelineEvent;
import net.floodlightcontroller.core.internal.PipelineEvent;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.BasePacket;
import net.floodlightcontroller.packet.Data;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.util.OFBundle;

import org.projectfloodlight.openflow.protocol.OFBarrierReply;
import org.projectfloodlight.openflow.protocol.OFBarrierRequest;
import org.projectfloodlight.openflow.protocol.OFFlowRemoved;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketInReason;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFPortStatus;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 
 * 
 * @author Andre Mantas
 */
public class EventReplication implements IFloodlightModule, IOFSwitchListener,
		IReplicationService {

	// Our dependencies
	protected IFloodlightProviderService floodlightProviderService;
	protected IOFSwitchService switchService;

	// bundle service. can be null if we dont use bundles
	private IBundleService bundleService = null;

	private ZKManager zk;

	private static final Logger log = LoggerFactory.getLogger(EventReplication.class);

	private static Scanner sc;

	// default values for configs
	private static String zkHostPort = "127.0.0.1:2181";
	private static int batchSize = 800;
	// wait time in for batching ms
	private static int waitTime = 100;
	private static int sessionTimeOut = 10000;

	/**
	 * Queue with the events to be delivered. The events must be ordered by
	 * event id. This queue is used by Controller.java
	 */
	private BlockingQueue<IPipelineEvent> pipelineQueue;

	/**
	 * Queue with ids of delivered events (by order of delivery) to apps for
	 * correctness check
	 */
	private Queue<Long> deliveredEvents;

	/**
	 * Last event enqueued in the toDeliverQueue
	 */
	private AtomicLong lastEnqueued;

	/**
	 * A set containing the ids of the events schedule to be delivered asap.
	 */
	private Map<Long, IPipelineEvent> scheduledEvents;

	private Map<Long, FloodlightContext> contextById;

	/**************************************************************************/
	/**************************************************************************/
	/**************************************************************************/

	/**
	 * Stores the event in ZK
	 * 
	 * @param sw
	 * @param m
	 * @param cntx
	 *            TODO
	 * @param cntx
	 * @return
	 * @requires cntx != null
	 */
	private long replicateEvent(IOFSwitch sw, OFMessage m,
			FloodlightContext cntx) {
		/*
		 * long start = System.nanoTime(); if (replicationStartTime == 0)
		 * replicationStartTime = start;
		 */
		long eId = zk.storeEvent(m, sw);
		if (cntx != null) {
			cntx.setEventId(eId);
			contextById.put(eId, cntx);
		}
		/*
		 * long end = System.nanoTime(); replicationEndTime = end; double
		 * timeElapsed = ZKManager.getElapsedTimeNanos(start, end);
		 * replicationTime += timeElapsed; replicationCounter++;
		 * 
		 * log.debug("Time to replicate event {} ms", timeElapsed);
		 */
		return eId;
	}

	private boolean bufferEvent(IOFSwitch sw, OFMessage msg) {
		boolean result = zk.bufferEvent(msg, sw);
		if (!result)
			log.info("Event not buffered. Already logged?");
		return result;
	}

	private void scheduleEnqueue(long key, IPipelineEvent val) {
		scheduledEvents.put(key, val);
	}

	private final void incrementLastEnqueued() {
		lastEnqueued.incrementAndGet();
		//log.debug("Incremented lastEnqueued to {} ", result);
	}

	private long getNextToEnqueue() {
		return (lastEnqueued.get() + 1);
	}

	@Override
	public void onMessageConsumed(IOFSwitch sw, FloodlightContext bc) {
		onMessageConsumed(sw, null, bc);
	}

	/**
	 * 
	 * @param sw
	 * @param messageProcessed
	 * @param bc
	 * @requires bc != null && bc.getStore() != null
	 */
	@Override
	public void onMessageConsumed(IOFSwitch sw, OFMessage m,
			FloodlightContext bc) {
		// some message was completely delivered to all modules.

		//log.debug("onMessageConsumed: {}", bc.getId());
		
		long eventId = getEventId(bc);

		if (eventId == -1) {
			return;// nothing to do
		}

		/* TODO: remove comment section to mark events from OF < 1.4 switches
		if (markEventAsProcessed(sw)) {
			log.debug("Marking event {} as processed", eventId);
			zk.processEvent(eventId);
		}
		*/

		tryEnqueueScheduledEvents();
		
		if (bundleService == null) {
			storeCommit(-2, eventId, sw.getId().toString());
		}

		//log.debug("Finished processing event {}", eventId);

	}

	/**
	 * Determines if the event from the switch sw should be marked as processed
	 * now. Returns true if:
	 * 
	 * switch version < 1.4 (no bundle support);
	 * 
	 * switch supports bundles but we are not using bundles;
	 * 
	 * it is a disconnected switch.
	 * 
	 * @param sw
	 * @return
	 */
	@SuppressWarnings("unused")
	private boolean markEventAsProcessed(IOFSwitch sw) {
		return (sw instanceof OFDisconnectedSwitch) || 
				sw != null && 
				(sw.getOFFactory().getVersion().compareTo(OFVersion.OF_14) < 0 || 
						bundleService == null);
	}

	@Override
	public void onSwitchUpdateFinish(IOFSwitch sw, FloodlightContext cntx) {
		onMessageConsumed(sw, cntx);
	}

	/**
	 * Tries to deliver the event with id getNextEventIdToDeliver(). I.e.,
	 * delivers the event with that id if the event is scheduled to be
	 * delivered.
	 * 
	 * @return true if some scheduled event was enqueued
	 */
	private void tryEnqueueScheduledEvents() {
		// check if we can enqueue any scheduled events
		if (scheduledEvents.size() == 0) {
			return;
		}

		synchronized (scheduledEvents) {
			long nextToEnqueue = getNextToEnqueue();
			
//			log.debug("Next event to enqueue is {}; Scheduled events: {}",
//					nextToEnqueue, scheduledEvents.keySet().toString());
			while (scheduledEvents.containsKey(nextToEnqueue)) {
				nextToEnqueue = enqueueAndGetNext(nextToEnqueue);
			}
		}

		// no need to loop here because deliverMsg() will increment
		// lastDelivered and Controller.java will call onMessageConsumed again.
	}

	private long enqueueAndGetNext(long nextToEnqueue) {
		// remove and obtain
		IPipelineEvent toEnqueue = scheduledEvents.remove(nextToEnqueue);

//		log.debug("Enqueueing the event {} that was scheduled", nextToEnqueue);

		addToQueue(toEnqueue, false);

		return getNextToEnqueue();
	}

	/**
	 * Adds toEnqueue to the toDeliverQueue and increments lastEnqueued
	 * 
	 * @param toEnqueue
	 */
	private void addToQueue(@Nonnull IPipelineEvent toEnqueue,
			boolean tryEnqueue) {
		if (toEnqueue == null) {
			log.error("toEnqueue cannot be null");
			return;
		}
		pipelineQueue.add(toEnqueue);
		incrementLastEnqueued();
		if (tryEnqueue)
			tryEnqueueScheduledEvents();
	}

	@Override
	public void sendToDeliverQueue(long eventId, String swId, OFMessage m) {
		FloodlightContext cntx = contextById.get(eventId);
		if (cntx == null) {
			cntx = getInitialContext(cntx, m);
			cntx.setEventId(eventId);
		}
		// assert (cntx.getId() == eventId);
		IOFSwitch sw = switchService.getSwitch(DatapathId.of(swId));
		if (sw == null) {
			sw = new OFDisconnectedSwitch((IOFSwitchManager) switchService,
					DatapathId.of(swId));
		}
		sendToDeliverQueue(eventId, sw, m, cntx);
	}

	/**
	 * Adds the eventId to the FloodlightContext before trying to add the event
	 * to the deliver queue or scheduling it.
	 * 
	 * @param eventId
	 *            the id of the event to deliver to the pipeline
	 * @param sw
	 *            the switch that originally sent the message
	 * @param m
	 *            the message sent by the switch
	 * @param cntx
	 *            the context associated with this event
	 * 
	 * @requires cntx != null
	 */
	private void sendToDeliverQueue(long eventId, IOFSwitch sw, OFMessage m,
			FloodlightContext cntx) {
		/*
		 * log.debug("sendToDeliverQueue: eventId={}, nextToEnqueue={}",
		 * eventId, getNextToEnqueue()); }
		 */
		IPipelineEvent pe = new PipelineEvent(m, sw, cntx);
		if (eventId == getNextToEnqueue()) {
			// log.debug("Event {} is the next to put in the queue. Enqueuing now.",
			// eventId);
			addToQueue(pe, true);
			// notifyNewEvents();
		} else {
			// log.debug("Event {} is not the next to put in the queue. Scheduling now.",
			// eventId);
			scheduleEnqueue(eventId, pe);
		}
	}

	@Override
	public boolean isMaster(String swId) {
		return zk.isMaster(swId);
	}

	@Override
	public boolean isConnected(String swId) {
		return zk.isConnected(swId);
	}

	@Override
	public boolean addAffectedSwitch(long eventId, String swId) {
		return zk.addAffectedSwitch(eventId, swId);
	}

	@Override
	public void storeCommit(int bundleId, long eventId, String swId) {
		zk.storeCommit(bundleId, eventId, swId);
	}

	@Override
	public void processReceivedCommitReply(long id, String sw) {
		zk.processReceivedCommitReply(id, sw);
	}

	/**************************************************************************/
	/**************************** Handle Messages *****************************/
	/**************************************************************************/
	private boolean handleFlowRemoved(IOFSwitch sw, OFFlowRemoved offr,
			FloodlightContext cntx) {
		if (zk.isMaster(sw)) {
			long eId = replicateEvent(sw, offr, cntx);
			if (eId != -1) {
				// sendToDeliverQueue(eId, sw, offr);
				return true;
			} else {
				return false;
			}
		} else {
			return bufferEvent(sw, offr);
		}
	}

	private boolean handlePortStatus(IOFSwitch sw, OFPortStatus m,
			FloodlightContext cntx) {
		boolean result = false;
		if (zk.isMaster(sw)) {
			long eId = replicateEvent(sw, m, cntx);
			if (eId != -1) {
				// sendToDeliverQueue(eId, sw, m, cntx);
				result = true;
			}
		} else {
			bufferEvent(sw, m);
		}
		return result;
	}

	private boolean handlePacketIn(IOFSwitch sw, OFPacketIn ofpi, Ethernet eth,
			FloodlightContext cntx) {
		boolean result = false;

		if (!isBundleCommittedPacketOut(ofpi, eth)) {
			// not packet in from our packet out
			if (zk.isMaster(sw)) {
				long eId = replicateEvent(sw, ofpi, cntx);
				if (eId != -1) {
					// sendToDeliverQueue(eId, sw, ofpi);
					result = true;
				} else {
					log.error("Error replicating event?");
				}
			} else {
				// slave: buffer event and stop pipeline
				bufferEvent(sw, ofpi);
			}
		} else {
			// isBundleCommittedPacketOut
			if (zk.isMaster(sw)) {
				log.debug("Master received PacketIn from own PacketOut. Stopping pipeline");
			} else {
				log.debug("Bundle committed packet out received by slave");
				// some bundle was committed on the switch by the master
				handlePacketOutFromMaster(getDataFromSimpleEthernet(eth));
			}
		}
		return result;
	}

	private void handlePacketOutFromMaster(String packetOutData) {
		// TODO: can we receive from other source? -> validate
		// format: <swId|bundle_id|id1,id2,id3,...>
		try {
			String[] fields = packetOutData.split("\\|");
			String swId = fields[0];
			int bundleId = Integer.parseInt(fields[1]);
			long eventId = Long.parseLong(fields[2]);
			zk.storeCommit(bundleId, eventId, swId);
			bundleService.addSentBundle(bundleId);
		} catch (IndexOutOfBoundsException | NumberFormatException e) {
			log.error(
					"Error parsing data from packet in from packet out. Data {}",
					packetOutData);
		}
	}

	/*************************************************************************/
	/************************** Main Receive method **************************/
	/*************************************************************************/

	@Override
	public boolean receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		// long start = System.nanoTime();
		boolean result = true;

		// assert (cntx != null);

		switch (msg.getType()) {

		case PACKET_IN:
			OFPacketIn ofpi = (OFPacketIn) msg;
			Ethernet eth = (Ethernet) cntx.getStorage().get(
					IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			result = handlePacketIn(sw, ofpi, eth, cntx);
			break;

		case FLOW_REMOVED:
			OFFlowRemoved offr = (OFFlowRemoved) msg;
			log.info("{}", offr.toString());
			result = handleFlowRemoved(sw, offr, cntx);
			break;

		case PORT_MOD:
		case PORT_STATUS:
			OFPortStatus ofps = (OFPortStatus) msg;
			log.info("{}", ofps.toString());
			result = handlePortStatus(sw, ofps, cntx);
			break;

		default:
			log.warn("Received unexpected message {}", msg);
			result = false;
			break;
		}

		/*
		 * double timeElapsed = ZKManager.getElapsedTimeNanos(start);
		 * processingTime += timeElapsed; processingCounter++;
		 * log.debug("Time to process message by EventReplication: {}ms",
		 * timeElapsed);
		 */

		return result;
	}

	/*************************************************************************/
	/****************************** Aux methods ******************************/
	/*************************************************************************/

	@SuppressWarnings("unused")
	private Ethernet getPacketInEthernet(OFPacketIn ofpi) {
		Ethernet eth = new Ethernet();
		eth.deserialize(ofpi.getData(), 0, ofpi.getData().length);
		return eth;
	}

	private boolean isBundleCommittedPacketOut(OFPacketIn ofpi, Ethernet eth) {
		return (isPacketInFromPacketOut(ofpi) && eth != null
				&& eth.getEtherType() != null && eth.getEtherType().getValue() == RESERVED_ETH_TYPE);
	}

	private boolean isPacketInFromPacketOut(OFPacketIn ofpi) {
		return ofpi.getReason() == OFPacketInReason.PACKET_OUT
				|| ofpi.getReason() == OFPacketInReason.ACTION;
	}

	/**
	 * Creates a OFPacketOut message to be sent to slave controllers informing
	 * them that the bundle b was committed in switch sw.
	 * 
	 * Format of data in PacketOut: <swId|bundle_id|id1,id2,id3,...>
	 * 
	 * @param b
	 * @param sw
	 * @return
	 */
	public static OFPacketOut createPacketOutBundleCommitted(OFBundle b,
			IOFSwitch sw) {

		// assert (b.getSwitchId().equals(sw.getId().toString()));

		String data = getBundleCommittedString(sw.getId().toString(), b.getBundleId(), b.getEventId());

		Ethernet eth = buildSimpleEthernetWithData(data.getBytes());

		return buildPacketOut(sw, OFPort.CONTROLLER,
				buildActionOutputPort(sw, OFPort.CONTROLLER), eth);
	}

	private static String getBundleCommittedString(String swId, int bundleId, long eventId) {
		return swId + "|" + bundleId + "|" + eventId;
	}

	private final static int RESERVED_ETH_TYPE = 65535;
	private final static String testHostMac = "0a:ae:d4:f6:70:30";

	private static Ethernet buildSimpleEthernetWithData(byte[] data) {
		Ethernet l2 = new Ethernet();
		l2.setEtherType(EthType.of(RESERVED_ETH_TYPE));
		l2.setSourceMACAddress(MacAddress.of("00:00:00:00:00:01"));
		l2.setDestinationMACAddress(MacAddress.of(testHostMac));
		/*
		 * Data packetData = new Data(); packetData.setData(data);
		 */
		l2.setPayload(new Data().setData(data));
		return l2;
	}

	private String getDataFromSimpleEthernet(Ethernet eth) {
		// cast to Data and return a new String with the byte[]
		return new String(((Data) eth.getPayload()).getData());
	}

	public OFPort getInPort(OFPacketIn pi) {
		return (pi.getVersion().compareTo(OFVersion.OF_12) < 0 ? pi.getInPort()
				: pi.getMatch().get(MatchField.IN_PORT));
	}

	private static List<OFAction> buildActionOutputPort(IOFSwitch sw,
			OFPort port) {
		List<OFAction> actions = new ArrayList<OFAction>();
		actions.add(sw.getOFFactory().actions().buildOutput().setPort(port)
				.setMaxLen(0xFFFF).build());
		return actions;
	}

	private static OFPacketOut buildPacketOut(IOFSwitch sw, OFPort inPort,
			List<OFAction> actions, BasePacket packet) {
		return sw.getOFFactory().buildPacketOut()
				.setBufferId(OFBufferId.NO_BUFFER).setInPort(inPort)
				.setActions(actions).setData(packet.serialize()).build();
	}

	@SuppressWarnings("unused")
	private String getOutGoingPacketOut(IOFSwitch sw, OFPacketOut out,
			byte[] data) {
		String spo = out.getType() + "(xid=" + out.getXid() + ", bufferId="
				+ out.getBufferId() + ", inPort=" + out.getInPort()
				+ ", actions=" + out.getActions() + ")";
		String s = "Sending: " + spo + " | data: " + new String(data) + " to "
				+ sw.getId().toString();
		return s;
	}

	public static void sendBarrier(final IOFSwitch sw, final Semaphore sem) {
		if (sw == null) {
			log.error("Trying to send barrier to non existing switch?");
			return;
		}
		OFBarrierRequest barReq = sw.getOFFactory().buildBarrierRequest()
				.build();

		// send barrier and save ListenableFuture
		ListenableFuture<OFBarrierReply> future = sw.writeRequest(barReq);
		log.debug("Barrier_Request sent to switch {}. xid: {}", sw.getId()
				.toString(), barReq.getXid());

		// add callback to execute when future computation is complete
		Futures.addCallback(future, new FutureCallback<OFBarrierReply>() {
			@Override
			public void onFailure(Throwable arg0) {
				log.error("Failed to receive BARRIER_REPLY from switch {}",
						sw.getId());
				sem.release();
			}

			@Override
			public void onSuccess(OFBarrierReply reply) {
				log.debug("Received BARRIER_REPLY from switch {} xid: {}",
						sw.getId(), reply.getXid());
				sem.release();
			}
		});
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IReplicationService.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		m.put(IReplicationService.class, this);
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IOFSwitchService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {

		floodlightProviderService = context
				.getServiceImpl(IFloodlightProviderService.class);

		switchService = context.getServiceImpl(IOFSwitchService.class);

		if (context.getAllServices().contains(IBundleService.class)) {
			bundleService = context.getServiceImpl(IBundleService.class);
		}

		Map<String, String> configOptions = context.getConfigParams(this);

		String configValue;

		configValue = configOptions.get("sessionTimeOut");
		if (configValue != null) {
			try {
				sessionTimeOut = Integer.parseInt(configValue);
				log.info("Found sessionTimeOut config: " + sessionTimeOut);
			} catch (NumberFormatException e) {
				log.warn("Invalid value in config file for some param in {}",
						EventReplication.class.getName());
			}
		}

		configValue = configOptions.get("zkHostPort");
		if (configValue != null) {
			zkHostPort = configValue;
			log.info(
					"Found zkHostPort config param. Connecting with ZooKeeper at {}",
					zkHostPort);
		} else {
			log.info(
					"No zkHostPort config param. Connecting with ZooKeeper at {}",
					zkHostPort);
		}

		configValue = configOptions.get("batchSize");
		if (configValue != null) {
			try {
				batchSize = Integer.parseInt(configValue);
				log.info("Found batchSize config param. Using batch size {}",
						batchSize);
			} catch (NumberFormatException e) {
				log.warn("Invalid value in config file for some param in {}",
						EventReplication.class.getName());
			}
		} else {
			log.info("No batchSize config param. Using batch size {}",
					batchSize);
		}

		configValue = configOptions.get("waitTime");
		if (configValue != null) {
			try {
				waitTime = Integer.parseInt(configValue);
				log.info("Found waitTime config param. Using wait time {}",
						waitTime);
			} catch (NumberFormatException e) {
				log.warn("Invalid value in config file for some param in {}",
						EventReplication.class.getName());
			}
		} else {
			log.info("No waitTime config param. Using wait time {}", waitTime);
		}

		try {
			zk = new ZKManager(zkHostPort, sessionTimeOut,
					floodlightProviderService, switchService, this, batchSize,
					waitTime);
			log.info("Started ZooKeeper");
		} catch (IllegalArgumentException e) {
			log.error("ZooKeeper did not start. Invalid arguments");
			System.exit(0);
		} catch (IOException e) {
			log.error("ZooKeeper did not start. " + e.getLocalizedMessage());
			System.exit(0);
		} catch (InterruptedException e) {
			log.error("ZooKeeper did not start. " + e.getLocalizedMessage());
			System.exit(0);
		}

		sc = new Scanner(System.in);

		// pipelineQueue = new LinkedBlockingQueue<Object[]>();

		scheduledEvents = new ConcurrentHashMap<Long, IPipelineEvent>();

		deliveredEvents = new ConcurrentLinkedQueue<Long>();

		contextById = new HashMap<Long, FloodlightContext>();

		// start at -1
		// first event is 0
		this.lastEnqueued = new AtomicLong(ZKManager.FIRST_ID - 1);
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		switchService.addOFSwitchListener(this);
		floodlightProviderService.addCompletionListener(this);
		startScannerThread();
	}

	/**************************************************************************/
	/*********************** IOFSwitchService methods *************************/
	/**************************************************************************/

	@Override
	public void switchAdded(DatapathId switchId) {
		log.info("Switch Added: {}", switchId.toString());
	}

	@Override
	public void switchRemoved(DatapathId switchId) {
		zk.removeSwitch(switchId);
	}

	@Override
	public void switchActivated(DatapathId switchId) {

		log.info("Switch Activated: {}", switchId.toString());

		IOFSwitch temp = switchService.getSwitch(switchId);
		// fix to a problem where switchService.getSwitch(switchId) would
		// return null
		int sleep = 100;
		while ((temp == null || !temp.isActive()) && sleep <= 2000) {
			try {
				Thread.sleep(sleep);
				sleep += 100;
				temp = switchService.getSwitch(switchId);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (temp == null) {
			log.error("switchActivated: could not get IOFSwitch from switchService");
			return;
		}

		final IOFSwitch sw = temp;

		new Thread(new Runnable() {
			@Override
			public void run() {
				zk.addSwitch(sw);
			}
		}).start();
	}

	@Override
	public void switchPortChanged(DatapathId switchId, OFPortDesc port,
			PortChangeType type, FloodlightContext cntx) {

		log.info("Switch port changed: sw={}, type={}", switchId.toString(),
				type);

		IOFSwitch sw = switchService.getSwitch(switchId);
		if (sw == null) {
			// OFSwitchManager implements both IOFSwitchService and
			// IOFSwitchManager. change here if this fact changes
			sw = new OFDisconnectedSwitch((IOFSwitchManager) switchService,
					switchId);
		}

		if (bundleService != null) {
			// only open after replicate because we need traceEvent() first

			// setCurrentEvent is called by Controller.java already.
			// if not do here
			// bundleService.setCurrentEvent(getEventId(cntx));

			bundleService.openEmptyBundle(sw);
		}

		/*
		 * handlePortStatus(sw, buildPortStatus(sw.getOFFactory(), port, type),
		 * cntx);
		 */
	}

	@Override
	public void switchChanged(DatapathId switchId) {
		// currently not used by Floodlight
	}

	@Override
	public void switchDeactivated(DatapathId switchId) {

	}

	private static final String availableCommands = "\tzk log" + "\n\tzk sw"
			+ "\n\tzk buf" + "\n\tzk order"
			+ "\n\tzk status\n\tbundles\n\tperformance\n\tconfig";

	private void startScannerThread() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				String op;
				try {
					while ((op = sc.nextLine()) != null) {
						log.info("Option typed: {}", op);
						if (op.equals("exit")) {
							zk.printLeadership();
							zk.printLogInfo();
							zk.printBufferInfo();
							printDeliveredEvents();
							if (bundleService != null)
								bundleService.printCurrentBundles();
							break;
						} else if (op.equals("test")) {

						} else if (op.equals("portmod up")) {

						} else if (op.equals("portmod down")) {

						} else if (op.equals("zk log")) {
							zk.printLogInfo();
						} else if (op.equals("zk sw")) {
							zk.printLeadership();
						} else if (op.equals("zk buf")) {
							zk.printBufferInfo();
						} else if (op.equals("zk order")) {
							printDeliveredEvents();
						} else if (op.equals("bundles")) {
							if (bundleService != null)
								bundleService.printCurrentBundles();
						} else if (op.equals("zk status")) {
							printStatus();
						} else if (op.equals("config")) {
							log.info(
									"zkHostPort={}, sessionTimeOut={}, batchSize={}, waitTime={}",
									new Object[] { zkHostPort, sessionTimeOut,
											batchSize, waitTime });
						} else {
							log.info("Unknown option. Try again.\n{}",
									availableCommands);
						}
					}
				} finally {
					log.info("Exiting floodlight...");
					sc.close();
					System.exit(0);
				}
			}
		}).start();
	}

	void printDeliveredEvents() {
		StringBuilder sb = new StringBuilder(
				"Delivered events to apps in controller " + zk.getId() + ":");
		Iterator<Long> itr = deliveredEvents.iterator();
		int i = 0;
		while (itr.hasNext()) {
			if (i % 10 == 0) {
				sb.append("\n\t");
			}
			i++;
			sb.append(itr.next() + ",");
		}
		System.out.println(sb.toString());
	}

	public void printStatus() {
		StringBuilder sb = new StringBuilder();
		sb.append("Last Enqueued=" + lastEnqueued.get());
		if (scheduledEvents == null) {
			sb.append(" scheduledEvents is null?");
		} else if (scheduledEvents.size() == 0) {
			sb.append(" No scheduled events, all clear!");
		} else {
			sb.append("\n" + scheduledEvents.size() + " scheduled Events: "
					+ scheduledEvents.keySet().toString());
		}
		log.info(sb.toString());
	}

	@Override
	public void setPipelineQueue(BlockingQueue<IPipelineEvent> pipelineQueue) {
		this.pipelineQueue = pipelineQueue;
	}

	@Override
	public String getName() {
		return this.getClass().getName().toString();
	}
}
