package net.floodlightcontroller.core.rama;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.Controller.ModuleLoaderState;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.rama.RamaBuffer.BufferedEvent;
import net.floodlightcontroller.util.OFMessageUtils;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.nustaq.serialization.FSTConfiguration;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.types.DatapathId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKManager {

	private final static Logger log = LoggerFactory.getLogger(ZKManager.class);

	/** Performs some asserts to validate code execution **/
	private final static boolean validate = false;

	/** Strings to be used in ZK **/

	/** Root **/
	private final static String ROOT_PATH = "/rama";

	/** Controllers path **/
	private final static String CONTROLLERS_PATH = ROOT_PATH + "/controllers";

	/** Name of the folder to store received events. **/
	private final static String RECEIVED_EVENTS_PATH = ROOT_PATH + "/"
			+ "received-events";

	/** Name of the folder to store processed events. **/
	private final static String PROCESSED_EVENTS_PATH = ROOT_PATH + "/"
			+ "processed-events";

	/** Name of the folder to store connected switches **/
	private final static String SWITCHES_PATH = ROOT_PATH + "/switches";

	/**
	 * List of base folders. Use functions so it can be easily changed in the
	 * future.
	 */
	public final static List<String> baseFolders = Arrays.asList(getRootPath(),
			getControllersPath(), getSwitchesPath(), getReceivedEventsPath(),
			getProcessedEventsPath());

	/**
	 * Prefix of for controllers
	 */
	private final static String CONTROLLERS_PREFIX = "c";

	/**
	 * Prefix for the names of each event node. Use getEventPath(eId, swId)
	 */
	private final static String EVENT_PREFIX = "";

	/**
	 * Byte array for no data
	 */
	private final static byte[] NO_DATA = new byte[0];

	/**
	 * The first sequential i that ZooKeeper uses in the sequential template
	 * name.
	 */
	static final long FIRST_ID = 0000000000;

	/**
	 * Object to be used as mutex in critical zones with synchronized block
	 */
	private final Object mutex = new Object();

	/**
	 * ZooKeeper client instance to comunicate with ZK server.
	 */
	private ZooKeeper zk;
	
	// zk.setData(): The maximum allowable size of the data array is
	// 1 MB (1,048,576 bytes). Arrays larger than this will cause a
	// KeeperException to be thrown.
	private final static int MAX_DATA_SIZE 		= 1048576;
	private final static int MAX_EVENTS 		= 100;
	private final static int MAX_OPS_PER_MULTI 	= ((int) 
					   (MAX_DATA_SIZE / (MAX_EVENTS * RamaEvent.AVG_MAX_SIZE)))-1;

	/**
	 * Set of connected switches. Each element is the id of the switch
	 */
	private Set<String> connectedSwitches;

	/**
	 * Set of known controllers. Each element is the id of the controller
	 */
	private Set<String> knownControllers;

	/**
	 * Leadership represented in the form of <controller, <switch ids>>.
	 * Controllers can be master of 0 or some switches.
	 */
	private Map<String, Set<String>> leadership;

	/**
	 * The Buffer of events used by slave controllers for all switches.
	 */
	private RamaBuffer switchesBuffer;

	/**
	 * The log of events in this controller for all switches. The order is
	 * imposed by the master.
	 */
	private RamaLog switchesLog;

	/**
	 * My controller id
	 */
	private String myId;

	/**
	 * Floodlight Provider to deliver message to other modules
	 */
	private IFloodlightProviderService floodlightProviderService;

	/**
	 * Switch service to get IOFSwitch objects based on switch id
	 */
	private IOFSwitchService switchService;

	/**
	 * 
	 */
	private IReplicationService replicationService;

	/**
	 * If this manager is connected with ZK and has started (i.e., loaded all
	 * state from ZK)
	 */
	private boolean connected;

	/**
	 * Submits zk.create operations to replicate received events
	 */
	private final ZKSubmitter zkSubmitter;

	/**
	 * https://github.com/RuedigerMoeller/fast-serialization
	 */
	private static final FSTConfiguration conf = FSTConfiguration
			.createDefaultConfiguration();

	ZKManager(String zkHostPort, int sessionTimeout,
			IFloodlightProviderService floodlightProviderService,
			IOFSwitchService switchService,
			IReplicationService replicationService, int batchSize, int waitTime)
			throws IOException, InterruptedException {

		if (zkHostPort == null || zkHostPort.length() == 0
				|| sessionTimeout <= 0 || floodlightProviderService == null
				|| switchService == null) {
			throw new IllegalArgumentException();
		}

		this.connected = false;

		this.zk = new ZooKeeper(zkHostPort, sessionTimeout,
				new ConnectionWatcher());

		// myId = controllerId; // this is set in loadZKState

		// connectedSwitches must be thread safe
		this.connectedSwitches = Collections
				.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

		// we update knownControllers and leadership when loading ZK state
		this.knownControllers = new HashSet<String>();
		this.leadership = new HashMap<String, Set<String>>();

		this.switchesLog = new RamaLog(log);

		this.switchesBuffer = new RamaBuffer();

		this.floodlightProviderService = floodlightProviderService;
		this.switchService = switchService;
		this.replicationService = replicationService;

		// register RamaEvent in the conf to make it faster to seralize and
		// deserialize data.
		// https://github.com/RuedigerMoeller/fast-serialization/wiki/Serialization#pregistering-classes
		conf.registerClass(RamaEvent.class);
		conf.registerClass(LinkedList.class);

		if (batchSize != 0 && waitTime != 0) {
			zkSubmitter = new ZKSubmitter(zk, batchSize, waitTime,
					getReceivedEventsPath(), getProcessedEventsPath());
			log.info("ZKManager using batch. size={} time={}", batchSize,
					waitTime);
		} else {
			zkSubmitter = null;
			log.info("ZKManager not using batch.");
		}
	}

	private class ConnectionWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			final WatchedEvent e = event;
			new Thread(new Runnable() {
				@Override
				public void run() {
					switch (e.getType()) {
					case None: // connection state changed
						try {
							handleConnectionChange(e);
						} catch (KeeperException e1) {
							e1.printStackTrace();
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						break;
					case NodeCreated:
					case NodeDataChanged:
					case NodeDeleted:
					case NodeChildrenChanged:
						log.info("Node change [" + e.toString() + "]");
						break;

					default:
						log.info("Unknown ZK event [" + e.toString() + "]");
						break;
					}
				}

			}).start();
		}

		private void handleConnectionChange(WatchedEvent event)
				throws KeeperException, InterruptedException {
			switch (event.getState()) {
			case SyncConnected:
				synchronized (mutex) {
					log.info("Connected with ZooKeeper [" + event.toString()
							+ "]");
					if (!connected) {
						loadZKState();
						connected = true;
					}
				}
				break;
			case AuthFailed:
				log.error("Auth with ZooKeeper failed: " + event.toString());
				break;
			case Disconnected:
				log.error("Auth with ZooKeeper disconnected: "
						+ event.toString());
				break;
			case Expired:
				log.error("Auth with ZooKeeper expired: " + event.toString());
				break;
			case ConnectedReadOnly:
			case SaslAuthenticated:
				log.info("[" + event.toString() + "]");
				break;
			default:
				log.info("Unexpected state");
				break;
			}
		}

		/**
		 * Loads the initial controller state from ZK
		 */
		private void loadZKState() {
			long initTime = System.currentTimeMillis();
			log.info("Started loading state from ZK...");

			// create base folders if they dont exist
			createBaseFolders();

			// create our node in ZK and get id from there.
			// creating a node returns the actual path, sequential ensures
			// that we have an unique id.
			String createdPath = createEphemeralSequentialNode(
					getControllersPath(), CONTROLLERS_PREFIX, NO_DATA);

			myId = createdPath.substring(createdPath.lastIndexOf('/') + 1);
			log.info("Got my id from ZK: " + myId);

			// load connected controllers into our in memory structures
			// leaves a ControllerWatcher in CONTROLLERS_FOLDER to be
			// notified
			// of new or crashed controllers
			loadConnectedControllers();
			assert (knownControllers.contains(myId));
			assert (leadership.containsKey(myId));

			log.debug("Controllers: {} \n\tLeadership: {}",
					knownControllers.toString(), leadership.toString());

			// loads connected switches and loads previous logged events for
			// each switch into our in memory structures.
			loadConnectedSwitches();

			// update our log based on events already on ZK for this sw
			try {
				// leave watcher in events node of this switch
				loadLogFromZK();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			printLogInfo();

			// im at startup -> deliver event to apps
			deliverLoggedEventstoApps();

			log.debug("Finished loading state from ZK. Time elapsed: {}",
					getElapsedTime(initTime) + "s");
		}

		/**
		 * Creates the base folders in ZK for the Rama structure (if they dont
		 * exist)
		 */
		private void createBaseFolders() {
			for (String s : baseFolders) {
				createNode(s, NO_DATA, CreateMode.PERSISTENT);
			}
		}

		/**
		 * Loads the connected controllers in ZK and leaves a ControllersWatcher
		 * in the controllers folder.
		 */
		private void loadConnectedControllers() {
			log.debug("Loading connected controllers...");
			try {
				// get children and leave watcher
				List<String> controllers = zk.getChildren(getControllersPath(),
						new ControllersWatcher());

				if (controllers == null || controllers.isEmpty()) {
					log.error("No connected controllers in the network. At least this controller should be there.");
					return;
				}

				log.debug("Controllers in ZK: {}", controllers.toString());
				for (String c : controllers) {
					// we also register our own id
					registerNewController(c);
				}
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * Gets all children in switches folder path and loads them to the
		 * switches set. Leaves a watcher in the switches main folder.
		 * 
		 * Tries to become master for each connected switch. If there is already
		 * a master, we are slave and an EventWatcher is set for that switch
		 * node.
		 * 
		 * For each switch in ZK, loads the events and updates the log.
		 */
		private void loadConnectedSwitches() {
			log.debug("Loading connected switches...");
			List<String> switches = null;
			try {
				// get children and leave watcher
				switches = zk.getChildren(getSwitchesPath(),
						new SwitchesWatcher());
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			if (switches == null || switches.isEmpty()) {
				log.info("No connected switches to controllers in the network");
				return;
			}

			log.debug("Switches in ZK: {}", switches.toString());
			for (String sw : switches) {
				if (switchService.getSwitch(DatapathId.of(sw)) == null) {
					// switch is not connected (yet)
					log.debug("Switch {} not connected", sw);
				} else {
					// this will most likely never happen since at load,
					// floodlight will never be connected with any switch
					addToConnectedSwitches(sw);
					boolean master = tryBecomeMaster(sw);
					if (master) {
						// im master for s
						updateLeadershipMemory(myId, sw);
						log.info("I am master for switch {}", sw);
					} else {
						// im slave for s
						if (initSlave(sw, false)) {
							log.info("I am slave for switch {}", sw);
						} else {
							log.error("Error initializing slave?");
						}
					}
				}

			}
			log.debug("connectedSwitches: {}", connectedSwitches.toString());
			log.debug("leadership: {}", leadership.toString());
		}

		/**
		 * Reads all events in the events folder and updates the log
		 * accordingly.
		 * 
		 * Master and slaves arrive here during "loadZKState"
		 * 
		 * 
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		private void loadLogFromZK() throws KeeperException,
				InterruptedException {

			assert (getLog() != null);

			log.debug("Loading log from events in ZK");

			// do not leave watchers yet

			loadProcessedEvents(getProcessedEventsPath());

			// see if we can clean any events in received folder
			loadReceivedEvents(getReceivedEventsPath());
		}

		private void loadProcessedEvents(String baseFolderPath)
				throws KeeperException, InterruptedException {
			List<String> eventNodes = zk.getChildren(baseFolderPath, null);
			log.info("Loading events from the processed folder: {}", eventNodes);
			eventNodes.stream()
					// get data in each node
					.map(node -> getZKNodeData(baseFolderPath + "/" + node,
							null))
					// get list of events from data
					.map(data -> getEventsFromData(data))
					// check for nulls
					.filter(list -> list != null)
					// get stream from list
					.flatMap(notNullList -> notNullList.stream())
					// filter only events that are not in the log
					.filter(re -> (!getLog().contains(re.getId())))
					// put events in the log
					.forEach(re -> getLog().put(re));
		}

		/**
		 * Loads the events in the specified folder. Assumes that these events
		 * can be deleted if they're already in the log. Call after loading
		 * events from the processed events folder.
		 * 
		 * @param folderPath
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		private void loadReceivedEvents(String folderPath)
				throws KeeperException, InterruptedException {
			List<String> receivedEventsNodes = zk.getChildren(folderPath, null);
			log.info("Loading events from the received folder: {}",
					receivedEventsNodes);
			for (String nodeName : receivedEventsNodes) {
				String nodePath = folderPath + "/" + nodeName;
				byte[] nodeData = getZKNodeData(nodePath, null);
				List<RamaEvent> res = getEventsFromData(nodeData);
				if (res == null) {
					log.error(
							"Error getting list of events from node data for node {}",
							nodeName);
					continue;
				}
				boolean alreadyHaveAll = true;
				for (RamaEvent re : res) {
					if (!getLog().contains(re.getId())) {
						alreadyHaveAll = false;
						getLog().put(re);
					}
				}
				if (alreadyHaveAll) {
					log.info(
							"Already have all events in 'received events' node {}. Deleting it.",
							nodePath);
					zk.delete(nodePath, -1);
				}
			}
		}

		/**
		 * To be used at startup only. Delivers events on log to apps from the
		 * first event id to the last.
		 */
		private void deliverLoggedEventstoApps() {
			if (getLog().getCurrentId() <= 0) {
				log.debug("No events to be delivered to apps");
				return;
			}

			long startTime = System.currentTimeMillis();
			log.debug("Delivering {} logged events to apps in order", getLog()
					.getCurrentId());
			long sleep = 0;
			while (floodlightProviderService.getModuleLoaderState() != ModuleLoaderState.COMPLETE) {
				// TODO: not optimal
				log.debug("Waiting for modules to load...");
				sleep += 1000;
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// we assume RamaLog has an ordered iterator
			for (RamaEvent re : getLog()) {
				replicationService.sendToDeliverQueue(re.getId(),
						re.getSwitchId(), re.getMessage());
			}
			log.debug("Delivering events to apps: done ({})",
					getElapsedTime(startTime));
		}
	}

	private void registerNewController(String controllerId) {
		knownControllers.add(controllerId);
		leadership.put(controllerId, new LinkedHashSet<String>());
	}

	/**
	 * Checks if the node folder already exists and if not, creates it.
	 * 
	 * @param folder
	 * @param data
	 * @param ephemeral
	 * @return
	 */
	private boolean createNode(String folder, byte[] data, CreateMode mode) {
		try {
			// TODO: prob can optimize. no need to check if exists
			// just create and if it exists, we catch and ignore exception and
			// return false
			Stat s = null;
			if (mode == CreateMode.PERSISTENT)
				s = zk.exists(folder, null);

			if (mode == CreateMode.EPHEMERAL || s == null) {
				zk.create(folder, data, Ids.OPEN_ACL_UNSAFE, mode);
				return true;
			}
		} catch (KeeperException e) {
			if (e.code() != Code.NODEEXISTS)
				e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Creates an ephemeral and sequential node in ZK with the given args
	 * 
	 * @param folderPath
	 *            the path to the node where the node will be created
	 * @param prefix
	 *            the prefix for the name of this sequential node
	 * @param data
	 *            the data to set in the node
	 * @return the actual path of the created node or an empty string if the
	 *         session with ZK expired
	 */
	private String createEphemeralSequentialNode(String folderPath,
			String prefix, byte[] data) {
		try {
			String path = folderPath + "/" + prefix;
			return zk.create(path, data, Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "";
	}

	/**
	 * Checks if the node folder already exists and if not, creates it.
	 * 
	 * @param folder
	 * @param data
	 * @param ephemeral
	 * @return
	 */
	private boolean createNodeUnsafe(String folder, byte[] data, CreateMode mode) {
		try {
			zk.create(folder, data, Ids.OPEN_ACL_UNSAFE, mode);
			return true;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Leaves a watcher for children changes in node specified by path.
	 * 
	 * @param path
	 * @param watcher
	 * @requires zk.exists(path)
	 */
	private void setChildrenWatcher(String path, Watcher watcher) {
		try {
			zk.getChildren(path, watcher);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param s
	 * @requires zk.exists(s)
	 * @return
	 */
	private boolean tryBecomeMaster(String swId) {
		if (isMaster_(swId)) {
			log.warn("Controller " + myId + " tried become master for switch "
					+ swId + " but it is master already!");
			return true;
		}

		log.debug("Trying to become master for switch " + swId);
		try {
			zk.create(getMasterNode(swId), myId.getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			return true;
		} catch (KeeperException e) {
			switch (e.code()) {
			case NODEEXISTS:
				// why didnt become master, return false
				break;
			case NONODE:
				log.error("Tried becoming master of switch that does not have a node yet");
				break;
			default:
				log.error("Unexpected KeeperException: " + e.toString());
				break;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	private final static String getSwitchesPath() {
		return SWITCHES_PATH;
	}

	private final static String getControllersPath() {
		return CONTROLLERS_PATH;
	}

	private final static String getRootPath() {
		return ROOT_PATH;
	}

	/**
	 * 
	 * @return The ZK folder (full path since /) where events are stored
	 */
	private final static String getReceivedEventsPath() {
		return RECEIVED_EVENTS_PATH;
	}

	/**
	 * 
	 * @return The ZK folder (full path since /) where events are stored
	 */
	private final static String getProcessedEventsPath() {
		return PROCESSED_EVENTS_PATH;
	}

	@SuppressWarnings("unused")
	private static String getControllerNodePath(String controllerId) {
		return getControllersPath() + "/" + controllerId;
	}

	/**
	 * Returns the path to the switch with id swId. E.g.: /rama/switches/swId
	 * 
	 * @param sw
	 * @return
	 */
	private final static String getSwitchPath(String swId) {
		return getSwitchesPath() + "/" + swId;
	}

	/**
	 * Returns the path to an existing znode representing an event with id
	 * eventId.
	 * 
	 * @param eventId
	 * @param swId
	 * @return
	 */
	private final static String getEventPath(long eventId) {
		return getNewEventPath() + getPaddedEventId(eventId);
	}

	/**
	 * Returns the path to be used for creating/getting a node for an event with
	 * id eId in/from the folder given by basePath
	 * 
	 * @param basePath
	 * @param eId
	 * @return
	 */
	private String getEventPath(String basePath, long eId) {
		return basePath + "/" + eId;
	}

	/**
	 * Returns the event id in the format kept by ZK (e.g., 0000000001) given a
	 * local event id (e.g., 1)
	 * 
	 * @param eventId
	 * @return
	 */
	private final static String getPaddedEventId(long eventId) {
		return StringUtils.leftPad(eventId + "", 10, "0");
	}

	private static final int eventPathPrefixLength = getNewEventPath().length();

	/**
	 * Use this method to get the path that ZK will use to create a new
	 * sequential (event) node.
	 * 
	 * Because events must be created with the ZK SEQUENTIAL flag, this method
	 * only returns the "prefix" that ZK will use. E.g.: this method returns
	 * /rama/events/e and ZK adds the id after (e.g., 0000000001)
	 * 
	 * @return the template path to create a new event node
	 */
	private final static String getNewEventPath() {
		return getReceivedEventsPath() + "/" + EVENT_PREFIX;
	}

	/**
	 * Returns the path for the node that contains the id of the master for the
	 * switch with id swId in his data. E.g.: /rama/switches/swId/master
	 * 
	 * @param swId
	 * @return
	 */
	private final static String getMasterNode(String swId) {
		return getSwitchPath(swId) + "/master"; // TODO
	}

	/**
	 * Returns the id of the switch sw
	 * 
	 * @param sw
	 *            the IOFSwitch
	 * @requires sw != null && sw.getId() != null
	 * @return
	 */
	final static String getSwitchId(IOFSwitch sw) {
		return sw.getId().toString();
	}

	/**
	 * Returns the id of the switch sw
	 * 
	 * @param sw
	 *            the IOFSwitch
	 * @requires sw != null
	 * @return
	 */
	final static String getSwitchId(DatapathId dpid) {
		return dpid.toString();
	}

	/**
	 * 
	 * @param eventId
	 *            the id of the event
	 * @return the switch id of the event eventId or null if there is no such
	 *         event
	 */
	String getSwitchId(long eventId) {
		RamaEvent re = getLog().getEvent(eventId);
		return re != null ? re.getSwitchId() : null;
	}

	/**
	 * Returns the id of the switch sw
	 * 
	 * @param sw
	 * @return
	 */
	private IOFSwitch getIOFSwitch(String swId) {
		return switchService.getSwitch(DatapathId.of(swId));
	}

	private RamaEvent addNewEventToLog(RamaEvent re) {
		return getLog().add(re);
	}

	/**
	 * Returns the serialized data to be stored for the event e
	 * 
	 * @see getEventFromData(byte[] data)
	 * @param event
	 * @return
	 */
	private final static byte[] getDataFromEvent(RamaEvent re) {
		// return SerializationUtils.serialize(re);
		return serialize(re);
	}

	/**
	 * @see getEventsFromData(byte[] data)
	 * @param res
	 * @return
	 */
	private final static byte[] getDataFromEvents(List<RamaEvent> res) {
		return serialize(res);
	}

	private final static byte[] serialize(Object o) {
		return conf.asByteArray(o);
	}

	/**
	 * Returns the deserialized RamaEvent from the serialized data
	 * 
	 * @param data
	 * @return
	 * @require data != null && data.length > 0
	 */
	@SuppressWarnings("unused")
	private final static RamaEvent getEventFromData(@Nonnull byte[] data) {
		try {
			// Object o = SerializationUtils.deserialize(data);
			// RamaEvent e = (RamaEvent) o;
			RamaEvent e = (RamaEvent) conf.asObject(data);
			e.updateMessage();
			return e;
		} catch (SerializationException | ClassCastException e) {
			log.error("Error getting RamaEvent from data. data.length="
					+ data.length);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns a deserialized List containing multiple RamaEvent, from the
	 * serialized data
	 * 
	 * @param data
	 * @return
	 * @require data != null && data.length > 0
	 */
	@SuppressWarnings("unchecked")
	private final static List<RamaEvent> getEventsFromData(@Nonnull byte[] data) {
		try {
			LinkedList<RamaEvent> res = new LinkedList<RamaEvent>();
			Object o = conf.asObject(data);
			if (o == null) {
				log.error("Error deserializing data?");
				return res;
			}
			if (o instanceof RamaEvent) {
				res.add((RamaEvent) o);
			} else if (o instanceof List<?>) {
				res = (LinkedList<RamaEvent>) o;
			} else {
				log.error("Unknown instance of object? {} - {}", o.getClass()
						.toString(), o.toString());
			}

			res.forEach(e -> e.updateMessage());

			return res;
		} catch (SerializationException | ClassCastException e) {
			log.error("Error getting RamaEvent from data. data.length="
					+ data.length);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Used for backward compatability.
	 * 
	 * @return the log of the switch swId or null
	 */
	private final RamaLog getLog() {
		return switchesLog;
	}

	/**
	 * 
	 * @return the buffer of events maintained by this controller.
	 */
	private RamaBuffer getSwitchBuffer() {
		return switchesBuffer;
	}

	/**
	 * Reads the ZK node containing the id of the master of the switch sw and
	 * updates the in memory leadership accordingly.
	 * 
	 * @param sw
	 *            the switch id
	 * @return true if it was succesful to read the data from the master node or
	 *         false if there was no master node for that switch
	 */
	private boolean updateLeadershipFromZK(String sw) {
		String masterNodePath = getMasterNode(sw);
		byte[] data = getZKNodeData(masterNodePath, null);
		if (data != null) {
			String masterId = new String(data);
			updateLeadershipMemory(masterId, sw);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 
	 * @param path
	 *            the full path to the znode
	 * @param watcher
	 *            the watcher to leave in the node or null
	 * @return the data in the specified node
	 */
	private byte[] getZKNodeData(String path, Watcher watcher) {
		try {
			return zk.getData(path, watcher, null);
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				log.error("No node '" + path + "'");
			} else {
				e.printStackTrace();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Updates the in memory leadership structure.
	 * 
	 * @param cId
	 *            the id of the new master controller
	 * @param swId
	 *            the id of the switch
	 */
	private void updateLeadershipMemory(String cId, String swId) {
		leadership.get(cId).add(swId);
	}

	/**
	 * Called when the switch is added to the ZKManager and this controller is
	 * assigned as slave.
	 * 
	 * @param swId
	 *            the switch id
	 * @return true if this it was successful to init and perform all required
	 *         modifications for a slave or false otherwise.
	 */
	private boolean initSlave(String swId, boolean leaveEventWatcher) {

		// get the id of the controller who is master for this switch
		if (!updateLeadershipFromZK(swId))
			return false;
		if (validate) {
			int masterCount = 0;
			for (String controller : leadership.keySet()) {
				if (leadership.get(controller).contains(swId)) {
					masterCount++;
				}
			}
			assert (masterCount == 1);
		}

		// set event watcher to receive updates from master
		if (leaveEventWatcher)
			setChildrenWatcher(getReceivedEventsPath(), new EventsWatcher(swId));
		return true;
	}

	/**
	 * Determines if this Manager is already connected with the ZK server and
	 * has loaded the whole state.
	 * 
	 * @return
	 */
	protected boolean isConnected() {
		return connected;
	}

	/**
	 * Returns true if this controller is the current master for the switch with
	 * id swId
	 * 
	 * @param string
	 * @return
	 */
	protected boolean isMaster(IOFSwitch sw) {
		return isMaster(getSwitchId(sw));
	}

	/**
	 * Returns true if this controller is the current master for the switch with
	 * id swId
	 * 
	 * @param string
	 * @return
	 */
	protected boolean isMaster(DatapathId swId) {
		return isMaster(getSwitchId(swId));
	}

	protected boolean isMaster(String swId) {
		synchronized (mutex) {
			// make sure no one is doing critical stuff
			// this controller can be transitioning from slave to master
		}
		return isMaster_(swId);
	}

	/**
	 * Determines if this controller is master for switch swId
	 * 
	 * @param swId
	 *            the switch id
	 * @requires leadership.get(myId) != null
	 * @return
	 */
	private boolean isMaster_(String swId) {
		return leadership.get(myId).contains(swId);
	}

	/**
	 * Determines if this switch is already connected with this Manager
	 * 
	 * @param sw
	 * @return
	 */
	boolean isConnected(IOFSwitch sw) {
		return sw == null ? false : isConnected(getSwitchId(sw));
	}

	/**
	 * Determines if this switch is already connected with this Manager
	 * 
	 * @param sw
	 *            the switch id
	 * @return
	 */
	boolean isConnected(String swId) {
		return connectedSwitches.contains(swId);
	}

	/**
	 * Adds a new switch to the ZooKeeper structure.
	 * 
	 * @param sw
	 *            the switch
	 * @return true if this controller is assigned as the master of the switch
	 *         or false otherwise
	 */
	protected boolean addSwitch(IOFSwitch sw) {
		log.info("Adding new switch to ZK: " + getSwitchId(sw));
		synchronized (mutex) {
			// make sure no one is doing critical stuff
		}
		synchronized (connectedSwitches) {
			return addSwitch(getSwitchId(sw));
		}
	}

	/**
	 * Adds a new switch to the ZooKeeper structure.
	 * 
	 * Returns true if this controller is assigned as the master of the switch
	 * or false otherwise.
	 * 
	 * @param swId
	 *            the switch id
	 * @return true if this controller is assigned as the master of the switch
	 *         or false otherwise
	 */
	private boolean addSwitch(String swId) {
		if (isConnected(swId)) {
			log.warn("Switch with id " + swId + " already exists.");
			return false;
		}

		boolean master = true;

		// try create node for switch
		boolean created = createNode(getSwitchPath(swId), NO_DATA,
				CreateMode.PERSISTENT);

		if (created) {
			// if no KeeperException thrown, we are first and so master

			// create events folder for this switch --> not needed anymore
			// createNodeUnsafe(getEventsPath(swId), NO_DATA,
			// CreateMode.PERSISTENT);

			// create master node inside this switch node
			createNodeUnsafe(getMasterNode(swId), myId.getBytes(),
					CreateMode.EPHEMERAL);
		} else {
			// try create master node for switch
			created = createNode(getMasterNode(swId), myId.getBytes(),
					CreateMode.EPHEMERAL);
			if (!created) {
				// NODEEXISTS: we are slave

				if (!initSlave(swId, true)) {
					// if cant initSlave, restart process
					return addSwitch(swId);
				}
				master = false;
			}
		}

		String role = "";
		if (master) {
			updateLeadershipMemory(myId, swId);
			role = "master";
		} else {
			role = "slave";
		}
		log.info("Im {} for this switch. Leadership: {}", role,
				leadership.toString());

		// its important that adding this switch to the connectedSwitches is the
		// last thing to do. other threads will check for this
		addToConnectedSwitches(swId);

		notifyAddSwitchCompleted(swId);

		return master;
	}

	private void addToConnectedSwitches(String swId) {
		connectedSwitches.add(swId);
	}

	/**
	 * Waits for other thread to call (and fully process) addSwitch(swId). This
	 * is mandatory if !connectedSwitches.contains(swId)
	 * 
	 * No need in being called if connectedSwitches.contains(swId)
	 * 
	 * This method must be in accordance with notifyAddSwitchCompleted
	 * 
	 * @param swId
	 *            the switch id
	 */
	private void waitAddSwitchCompleted(String swId) {
		// see Object.wait() javadoc
		synchronized (getLog()) {
			// here our condition is switch not being connected
			while (!isConnected(swId)) {
				try {
					log.info("Waiting for AddSwitchCompleted for switch "
							+ swId);
					getLog().wait();

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Notifies any threads waiting for this switch being fully added that they
	 * can proceed. This method must be in accordance with
	 * waitAddSwitchCompleted
	 * 
	 * @param swId
	 *            the switch id
	 * @requires getSwitchLog(swId) != null
	 */
	private void notifyAddSwitchCompleted(String swId) {
		synchronized (getLog()) {
			log.info("Notifing all threads waiting for AddSwitchCompleted "
					+ "for switch " + swId);
			getLog().notifyAll();
		}
	}

	protected boolean removeSwitch(DatapathId dpid) {
		synchronized (mutex) {
			// make sure no one is doing critical stuff
		}
		String swId = getSwitchId(dpid);

		log.info("Removing switch from ZK: " + swId);

		if (!isConnected(swId)) {
			log.warn("Tried to remove switch not connected? " + swId);
			return false;
		}

		// if we are master, just remove from our set
		if (!leadership.get(myId).remove(swId)) {
			// if we are slave remove from other master set
			boolean deleted = false;
			for (String c : knownControllers) {
				if (leadership.get(c).remove(swId)) {
					deleted = true;
					break;
				}
			}
			if (!deleted) {
				log.warn("removeSwitch: did not find " + swId
						+ " in leadership map");
				return false;
			}
		}

		removeSwitchMemory(swId);

		return true;
	}

	private void removeSwitchMemory(String swId) {
		connectedSwitches.remove(swId);
	}

	/**
	 * Stores the event in a persistent way and logs the event in memory. Should
	 * only be used by the master controller for switch sw. Storing events
	 * enables recovering and loading initial state as well as propagate logged
	 * events from the master to the slaves.
	 * 
	 * Just because the event is replicated does not mean it is safe to
	 * delivered it to the other modules. Use canDeliver(re)
	 * 
	 * @param event
	 * @param sw
	 * @return the event created or null in case of error.
	 */
	long storeEvent(OFMessage m, IOFSwitch sw) {
		log.trace("Storing new event in ZK...");
		String swId = getSwitchId(sw);
		// synchronized (mutex) {
		// make sure no one is doing critical stuff
		// }
		if (!isConnected(swId)) {
			log.info("Waiting for switch to be fully connected with ZK");
			waitAddSwitchCompleted(swId);
		}
		return storeEvent(m, swId).getId();
	}

	/**
	 * Call this only if no one is doing critical stuff and if the switch is
	 * already connected to the ZKManager.
	 * 
	 * The id of the created event is decided by the total order in ZK
	 * 
	 * @param m
	 * @param swId
	 * @return
	 */
	@SuppressWarnings("unused")
	private RamaEvent storeEventMultipleMasters(OFMessage m, String swId) {
		// create event node to see what is this event ID
		// only ZK can decide the total order of events
		// set data but event id is 0
		// the id of the event is in the node name.
		RamaEvent re = new RamaEvent(m, swId, -1);
		String createdPath = createNewEventNode(getDataFromEvent(re));

		// retrieve only the id from the full path
		long eventId = getIdFromPath(createdPath);

		log.trace("Created path in ZK for the new node: {}; Event id: {}",
				createdPath, eventId);

		// add event to in memory log after updating the event id
		re.setEventId(eventId);
		addNewEventToLog(re);
		return re;
	}

	/**
	 * Call this only if no one is doing critical stuff and if the switch is
	 * already connected to the ZKManager
	 * 
	 * Creates a new node on ZK and sets the data with the new RamaEvent
	 * serialized data. RamaEvent has a sequential (controller specific) ID.
	 * 
	 * @param m
	 * @param swId
	 * @return
	 */
	private RamaEvent storeEvent(OFMessage m, String swId) {

		RamaEvent re = getLog().add(m, swId);

		createNewEventNode(re.getId(), getDataFromEvent(re));

		return re;
	}

	/**
	 * Creates a new (sequential) znode in the events folder with the given
	 * data. The id of this event can be obtained with
	 * getEventId(createNewEventNode(data))
	 * 
	 * @return the path created for this znode that represents an event
	 */
	private String createNewEventNode(byte[] data) {
		try {
			return zk.create(getNewEventPath(), data, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Creates a new znode in the events folder with the given data. The path of
	 * the created event depends on the eventId
	 * 
	 * @return the path created for this znode that represents an event
	 */
	private void createNewEventNode(long eventId, byte[] data) {
		if (zkSubmitter != null) {
			zkSubmitter.submitCreateNode(getLog().getEvent(eventId));
			// log.trace("Submitted the new event event for batching: {}",
			// eventId);
		} else {
			String path = getEventPath(eventId);
			try {
				String created = zk.create(path, data, Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				log.trace("Created event node: {}", created);
				RamaEvent re = getLog().getEvent(eventId);
				replicationService.sendToDeliverQueue(re.getId(),
						re.getSwitchId(), re.getMessage());
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private class ZKSubmitter implements Runnable {

		private final Object eventListLock = new Object();
		private final Object updateListLock = new Object();
		private final Object opListLock = new Object();

		private List<RamaEvent> updateList;
		private List<RamaEvent> eventList;
		private LinkedList<List<Op>> opList;

		private final int batchSize;
		private final int waitTime;

		private final Map<Long, List<RamaEvent>> eventsPerBatch;

		private final String createPath;
		private final String updatePath;

		private ScheduledExecutorService ses;

		private ZooKeeper zk;

		private ScheduledFuture<?> scheduledSubmit;

		private long createStartId;
		private long updateStartId;

		private int eventsBatched;
		private int currentNodeSize;

		/**
		 * 
		 * @param batchSize
		 *            total amount of ops of a full batch
		 * @param waitTime
		 *            time to wait before submitting a non full batch (in ms)
		 */
		ZKSubmitter(ZooKeeper zk, int batchSize, int waitTime,
				String baseCreatePath, String baseUpdatePath) {
			this.zk = zk;
			resetOpList();
			resetEventList();
			resetUpdateList();
			this.batchSize 			= batchSize;
			this.waitTime 			= waitTime;
			this.ses 				= Executors.newScheduledThreadPool(1);
			this.scheduledSubmit 	= null;
			this.eventsPerBatch 	= new HashMap<Long, List<RamaEvent>>();
			this.createPath 		= baseCreatePath;
			this.updatePath 		= baseUpdatePath;
			assert(!createPath.equals(updatePath));
		}

		void resetEventList() {
			this.eventList 		= new LinkedList<RamaEvent>();
			this.createStartId 	= Long.MAX_VALUE;
		}
		
		void resetUpdateList() {
			this.updateList 	= new LinkedList<RamaEvent>();
			this.updateStartId 	= Long.MAX_VALUE;
		}
		
		/**
		 * Resets opList and eventsBatched
		 */
		void resetOpList() {
			this.opList 			= new LinkedList<List<Op>>();
			goToNextNode();
			this.eventsBatched 		= 0;
		}
		
		/**
		 * Adds a new List of Ops to the List of Lists and resets opListDataSize
		 */
		private void goToNextNode() {
			opList.add(new LinkedList<Op>());
			currentNodeSize = 0;
		}
		
		/**
		 * 
		 * @param op
		 */
		private void addToCurrentNode(String path, byte[] data) {
			opList.getLast().add(Op.create(path, data, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT));
			currentNodeSize += data.length;
		}
		
		/**
		 * 
		 * @param path
		 * @param data
		 */
		private final void addToOpList(String path, byte[] data) {
			if (currentNodeSize + data.length >= MAX_DATA_SIZE) {
				goToNextNode();
			}
			addToCurrentNode(path, data);
		}

		void submitCreateNode(RamaEvent re) {
			synchronized (opListLock) {
				synchronized (eventListLock) {
					if (re.getId() < this.createStartId) {
						this.createStartId = re.getId();
					}
					eventList.add(re);
					eventsBatched++;
					if (eventsBatched >= batchSize) {
						if (scheduledSubmit != null) {
							scheduledSubmit.cancel(false);
							// log.debug("Canceled scheduled submit");
						}
						// submitNow also adds the events in eventsList
						submitNow();
						return;
					} else if (eventList.size() >= MAX_EVENTS) {
						addMultipleEventsToOneOp();
					}
				}

				if (scheduledSubmit == null || scheduledSubmit.isDone()) {
					scheduledSubmit = ses.schedule(this, waitTime,
							TimeUnit.MILLISECONDS);
				}
			}
		}
		
		/**
		 * Adds one Op.create object to opList. The data of this op is the
		 * serialized eventsList. Resets the eventsList
		 * 
		 * Must be called within a synchronized (opListLock)
		 */
		private void addMultipleEventsToOneOp() {
			//synchronized (opListLock) {
				byte[] data;
				List<RamaEvent> aux;
				long auxStart;
//				synchronized (eventListLock) {
					aux = eventList;
					auxStart = createStartId;
					resetEventList();
//				}
				
				data = getDataFromEvents(aux);
				
				if (aux.size() > MAX_EVENTS || data.length > MAX_DATA_SIZE) {
					log.error("List to serialize has more events than allowed? "
							+ "{} | {}bytes", aux.size(), data.length);
				}

				// String path = getEventPath(startId);
				String path = getEventPath(this.createPath, auxStart);
				// log.debug(
				// "Adding Op.Create to opList with {} events. path={} data.length={}",
				// new Object[] { aux.size(), path, data.length });
				addToOpList(path, data);
				eventsPerBatch.put(auxStart, new LinkedList<RamaEvent>(aux));
			//}
		}

		void submitUpdateNode(RamaEvent re) {
			synchronized (opListLock) {
				synchronized (updateListLock) {
					if (re.getId() < this.updateStartId) {
						this.updateStartId = re.getId();
					}
					updateList.add(re);
					if (updateList.size() >= MAX_EVENTS) {
						addMultipleUpdatesToOneOp();
					}
				}
			}
		}
		
		/**
		 * Adds one Op.create object to opList. The data of this op is the
		 * serialized eventsList. Resets the eventsList
		 * 
		 * Must be called within a synchronized (opListLock)
		 */
		private void addMultipleUpdatesToOneOp() {
			//synchronized (opListLock) {
				List<RamaEvent> aux;
				long auxStart;
//				synchronized (updateListLock) {
					aux 		= updateList;
					auxStart 	= updateStartId;
					resetUpdateList();
//				}
				byte[] data = getDataFromEvents(aux);
				if (aux.size() > MAX_EVENTS || data.length > MAX_DATA_SIZE) {
					log.error("Update: List to serialize has more events than allowed? "
							+ "{} | {}bytes", aux.size(), data.length);
				}
				String path = getEventPath(this.updatePath, auxStart);
				addToOpList(path, data);
			//}
		}

		/**
		 * adds any remaining events in eventsList to opList
		 * 
		 * Sends a zk.multi request with all Ops in opList
		 * 
		 * sets eventsBatched to 0
		 */
		private void submitNow() {
			List<List<Op>> newList;
			synchronized (opListLock) {
				if (opList.size() == 0 && eventList.size() == 0) {
					log.warn("submitNow with no operations?");
					return;
				}
				
				synchronized (eventListLock) {
					if (eventList.size() > 0) {
						addMultipleEventsToOneOp();
					}
				}
				synchronized (updateListLock) {
					if (updateList.size() > 0) {
						addMultipleUpdatesToOneOp();
					}
				}
				
				newList = opList;
				resetOpList();
			}

			for (List<Op> list : newList) {
				zk.multi(list, new MultiReplyHandler(), null);
			}
		}

		@Override
		public void run() {
			submitNow();
		}
		
		private class MultiReplyHandler implements MultiCallback {
			
			@Override
			public void processResult(int rc, String path, Object ctx,
					List<OpResult> opResults) {
				if (rc == Code.OK.intValue()) {
					processOkMultiReply(opResults);
				} else {
					StringBuilder sb = new StringBuilder("[");
					for (OpResult or : opResults) {
						sb.append("type="+or.getType());
						if (or instanceof CreateResult) {
							CreateResult cr = (CreateResult) or;
							sb.append("; path="+ cr.getPath());
						}
						sb.append("],");
					}
					sb.append("]");
					log.error("Error processing multi request in zookeeper: "
							+ "code={}, path={}, ctx={}, opResults={}",
							new Object[] { Code.get(rc), path, ctx, sb.toString() });
				}
			}
			
			void processOkMultiReply(List<OpResult> opResults) {

				opResults.stream()
				// get the OpResult that are of type CreateResult
				// .filter(result -> result instanceof CreateResult)
				// cast to ease next steps
				.map(op -> (CreateResult) op).map(cr -> cr.getPath())
				// filter to only continue with operations that asked to create 
				// received events instead of update events too
				.filter(path -> path.startsWith(createPath))
				// sort by path (lowest ids first)
				// .sorted()
				// for each path, obtain the event id corresponding to that
				// path and get the list of longs that are contained in the
				// data of that create and order events by their id
				// .flatMap(p ->
				// eventsPerBatch.get(getIdFromPath(p)).stream())
				.flatMap(p -> eventsPerBatch.remove(getIdFromPath(p)).stream())
				// in the end, call sendToDeliverQueue for each
				// event
				.forEach(e -> replicationService.sendToDeliverQueue(e.getId(),
																	e.getSwitchId(),
																	e.getMessage()));
			}
		}
	}

	/**
	 * Gets the event id from an event node path. Given a full path to an event
	 * node (e.g., /rama/events/e0000000001) returns the event id
	 * 
	 * This method must be "synched" with the method that creates the znodes
	 * representing events.
	 * 
	 * @param eventNodePath
	 *            the path to the ZK node that contains the event, since root.
	 * @return the id of the event stored in eventNodePath or -1 if the
	 *         eventNodePath format is wrong.
	 */
	private long getIdFromPath(String eventNodePath) {
		try {
			return Long.parseLong(eventNodePath
					.substring(eventPathPrefixLength));
		} catch (NumberFormatException e) {
			return -1L;
		}
	}

	/**
	 * Gets the event id from an event node name. Given a string
	 * "EVENT_PREFIX+event_id" (e.g., e00000001) returns event_id in long
	 * 
	 * @param eventNodeName
	 * @return the id of the event specified in the event node name or -1 if the
	 *         format of the node name is invalid.
	 */
	private long getIdFromName(String eventNodeName) {
		try {
			return Long
					.parseLong(eventNodeName.substring(EVENT_PREFIX.length()));
		} catch (NumberFormatException e) {
			return -1L;
		}
	}

	/**
	 * Method to be used by slaves. Stores the event in a temporary in-memory
	 * buffer that will be used in case this slave becomes the new master for
	 * switch sw.
	 * 
	 * @param ofpi
	 * @param sw
	 */
	boolean bufferEvent(OFMessage m, IOFSwitch sw) {
		String swId = getSwitchId(sw);
		log.debug("Buffering message from switch {}: {}", swId,
				OFMessageUtils.msgToString(m));
		synchronized (mutex) {
			// make sure no one is doing critical stuff
		}
		if (!isConnected(swId)) {
			log.debug("Switch not yet connected with ZK, waiting...");
			waitAddSwitchCompleted(swId);
		}
		return bufferEvent(m, swId);
	}

	private boolean bufferEvent(OFMessage m, String swId) {
		// check if by any chance this controller transitioned to master
		if (!isMaster(swId))
			return bufferEvent_(m, swId);
		else {
			log.debug("Controller transitioned to master. Replicating event instead of buffering.");
			// need to store event and check if it can be delivered. we are
			// master but EventReplication thinks we are slave and will not
			// deliver event nor schedule it to delivery

			RamaEvent re = storeEvent(m, swId);

			return re != null;

		}
	}

	/**
	 * Effectively buffers the pair <swId, m> for this slave controller
	 * 
	 * @param m
	 * @param swId
	 * @return true if this pair was buffered or false if it was supossed to be
	 *         ignored because this pair is already logged.
	 */
	private boolean bufferEvent_(OFMessage m, String swId) {
		// synchronize on log because we may be receiving updates from ZK, which
		// will mark events to be ignored
		synchronized (getLog()) {
			return addEventToBuffer(m, swId);
		}
	}

	private boolean addEventToBuffer(OFMessage m, String swId) {
		return getSwitchBuffer().add(m, swId);
	}

	void storeCommit(int bundleId, long eventId, String swId) {
		log.debug("storeCommit for event {} from switch {}", eventId, swId);
		boolean swConnected = isConnected(swId);
		if (isMaster(swId) && swConnected) {
			// switch connected and we are master
			storeMasterCommit(bundleId, eventId, swId);
		} else if (swConnected) {
			// switch connected but we are slave
			storeSlaveCommit(bundleId, eventId, swId);
		} else {
			// switch not connected
			log.debug(
					"Storing commit reply from disconnected switch {} for events {}",
					swId, eventId);
			processReceivedCommitReply(eventId, swId);
		}
	}

	/**
	 * 
	 * @param bundleId
	 * @param eventId
	 * @param swId
	 * @return
	 * @requires isConnected(swId) && getSwitchesLog() != null
	 */
	private void storeMasterCommit(int bundleId, long eventId, String swId) {
		// synchronized (mutex) {
		// make sure no one is doing critical stuff
		// }
		storeMasterCommit_(bundleId, eventId, swId);
	}

	/**
	 * Marks all events with ids in eventIds as being processed. This can only
	 * include events already in the log.
	 * 
	 * The master has just received a BUNDLE_COMMIT_REPLY for the bundle with id
	 * 'bundleId'. This means that these events can be marked as processed and
	 * controllers should never resend any commands generated from the events in
	 * 'eventIds'.
	 * 
	 * Because slaves receive a PacketIn informing the bundle was committed,
	 * they do not need to monitor changes inside each event node.
	 * 
	 * The master also modifies the data in the zk node for this event.
	 * 
	 * @param bundleId
	 *            the id of the bundle that was committed on the switch
	 * @param eventId
	 *            the ids of the RamaEvents that generated the commands in the
	 *            bundle.
	 * @param sw
	 *            the switch
	 * @requires isConnected(swId)
	 */
	private void storeMasterCommit_(int bundleId, long eventId, String swId) {
		// TODO: bundle id?
		log.debug("Storing commit for events {}", eventId);
		if (eventId == -1) {
			// commands not generated by an event
			// master already sent the commands and they are committed
		} else {
			processReceivedCommitReply(eventId, swId);
		}
	}

	/**
	 * 
	 * @param eId
	 * @param swId
	 * @requires getSwitchesLog() != null
	 */
	void processReceivedCommitReply(long eId, String swId) {
		if (getLog().addReceivedCommitReply(eId, swId)) {
			log.debug("Commit reply from {} for event {} marked as received",
					swId, eId);
			// if after this commit reply received the event is marked as
			// processed, update node on ZK
			if (getLog().isProcessed(eId)) {
				updateEventNode(eId);
				handleEventLoopCompleted(eId);
			} else {
				log.debug("Event not processed yet, waiting commit replies "
						+ "from other switches");
			}
		}
	}

	private void handleEventLoopCompleted(long eId) {
		// TODO: here we may need to deliver outgoing messages to modules
		// depending on config (on slaves)
	}

	/**
	 * Slaves receive "BUNDLE_COMMIT_REPLY" by means of a PacketIn that the
	 * master added to the bundle. When this PacketIn is received, the data
	 * should be extracted and this method is called with that data.
	 * 
	 * Before calling this method, the slave already has the event in his log
	 * (the order was decided by the master) and is waiting to receive
	 * confirmation that the bundle was committed on the switch. The event was
	 * added to the slave's log by the EventsWatcher that monitors each switch
	 * event folder. Since the master creates a new node for each event, the
	 * watcher will receive a notification and check the children, ignoring
	 * events already in the log, and adding new ones.
	 * 
	 * The master marks the event as being processed in ZK (by setting a new
	 * data in the node). However, because the master can fail, the slave will
	 * check if the data in the event node already corresponds to a processed
	 * event.
	 * 
	 * IF the slave receives his "commit reply" before receiving the replicated
	 * event from the master, this method will not known about the event, and
	 * will skip marking it as processed. Therefore, receiving events from ZK
	 * must check if the event is already marked as processed by the master.
	 * 
	 * @param bundleId
	 * @param eventId
	 * @param swId
	 * @return
	 */
	private void storeSlaveCommit(int bundleId, long eventId, String swId) {
		synchronized (mutex) {
			// make sure no one is doing critical stuff
		}

		// TODO: bundle id?

		// check if we transition to master meanwhile
		if (isMaster_(swId)) {
			log.debug("Controller transitioned to master while committing as slave");
			storeMasterCommit_(bundleId, eventId, swId);
			return;
		}

		log.debug(
				"Storing slave commit for switch {}; Bundle id: {}; Event ids: {}",
				new Object[] { swId, bundleId, eventId });

		// if bundle from non event
		if (eventId == -1) {
			storeSlaveCommitNonEvent(bundleId, eventId, swId);
			return;
		}

		RamaLog rLog = getLog();
		if (rLog == null) {
			log.error("storeSlaveCommit: no log for switch " + swId);
			return;
		}

		if (rLog.addReceivedCommitReply(eventId, swId)) {
			log.debug("Commit reply from {} for event {} marked as received",
					swId, eventId);
			if (rLog.isProcessed(eventId)) {
				// if after this commit reply received the event is marked
				// as processed, update node on ZK
				checkEventIsProcessedInZK(eventId, swId);
				handleEventLoopCompleted(eventId);
			}
		} else {
			log.warn("Slave tried to commit event with id {}", eventId);
		}
	}

	private boolean storeSlaveCommitNonEvent(int bundleId, long eventId,
			String swId) {
		// TODO: o que fazer em caso de ser um bundle comandos que nao surgiram
		// de eventos?
		// em principio nada...
		return true;
	}

	/**
	 * Checks if the event stored in the ZK node is already marked as processed.
	 * If it isn't, updates the node data in ZK.
	 * 
	 * @param eId
	 * @param swId
	 */
	private void checkEventIsProcessedInZK(long eId, String swId) {

		List<String> sortedProcessedEvents = getSortedChildren(getProcessedEventsPath());

		if (sortedProcessedEvents.size() == 0) {
			log.warn("getSortedChildren(getProcessedEventsPath()) is empty?");
			return;
		}

		log.debug("sortedProcessedEvents: " + sortedProcessedEvents);

		String nodeToGet = getNodeOfEvent(eId, sortedProcessedEvents);

		log.debug("nodeToGet: " + nodeToGet);

		String nodePath = getProcessedEventsPath() + "/" + nodeToGet;

		log.debug("nodePath: " + nodePath);

		byte[] data = getZKNodeData(nodePath, null);

		if (data == null || data.length == 0) {
			log.warn("Ignoring event {}: no node for it in ZK.", eId);
			return;
		}

		List<RamaEvent> events = getEventsFromData(data);

		RamaEvent re = null;

		for (RamaEvent event : events) {
			if (event.getId() == eId) {
				re = event;
				break;
			}
		}

		if (re == null) {
			log.warn("Ignoring event could not deserialize data to event.", eId);
			return;
		}
		if (!re.isProcessed()) {
			// master crashed or slower? we need to update zk node data
			log.info("checkEventIsProcessedInZK: slave found event in ZK that should be processed but isnt.");
			if (!getLog().contains(eId)) {
				log.error(
						"checkEventIsProcessedInZK: slave doesnt have event {} from switch {} in his log!",
						eId, swId);
			} else if (!getLog().getEvent(eId).isProcessed()) {
				log.error(
						"checkEventIsProcessedInZK: slave has the event {} from switch {} in his log but it isnt processed!",
						eId, swId);
			} else {
				// we have this event in our log and it is processed
				updateEventNode(eId);
			}
		} else {
			log.info("Event " + eId + " from ZK is already processed - good.");
		}
	}

	/**
	 * 
	 * @param eId
	 *            the id of the event to get the data
	 * @param list
	 *            getChildren of some path containing multiple batched events
	 *            (SORTED from first to last id)
	 * @return the name of the event that contains eId (among other events). Use
	 *         getData on this node and then loop the list searching for it
	 *         again.
	 */
	private String getNodeOfEvent(long eId, List<String> list) {
		String result = "";
		// start at the end of the sorted list
		// the node we want is the first that has id of equal or less than what
		// we want
		for (int i = list.size() - 1; i >= 0; i--) {
			String nodeName = list.get(i);
			long nodeId = getIdFromName(nodeName);
			if (nodeId <= eId) {
				return nodeName;
			}
		}
		return result;
	}

	private List<String> getSortedChildren(String path) {
		List<String> children;
		try {
			children = zk.getChildren(path, null);
			children.sort(String.CASE_INSENSITIVE_ORDER);
			return children;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			log.error("Error in ZK: " + e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassCastException e) {
			e.printStackTrace();
		}
		return new LinkedList<String>();
	}

	/**
	 * Updates the event node data in ZK with the current data of the event with
	 * id eId.
	 * 
	 * The event node path is given by getEventPath(eId, swId)
	 * 
	 * The new data to set in the node is given by
	 * getDataFromEvent(getSwitchLog(swId).getEvent(eId))
	 * 
	 * Always overrides the data in the node, not checking versions.
	 * 
	 * @param eId
	 *            the id of the event
	 * @requires getSwitchLog(swId) != null
	 * @requires getSwitchLog(swId).getEvent(eId) != null
	 * @return true if the node exists and the data was set. false otherwise.
	 */
	private boolean updateEventNode(long eId) {
		log.debug("updateEventNode: eId={}; {}", eId, getLog().getEvent(eId)
				.toString());
		// zk.setData(getEventPath(eId), getDataFromEvent(getSwitchesLog()
		// .getEvent(eId)), -1);
		// return updateEventNode(getEventPath(eId), getLog().getEvent(eId));
		return updateEventNode(getLog().getEvent(eId));
	}

	/**
	 * Updates the event node data in ZK with the current data of the event re
	 * 
	 * The new data to set in the node is given by getDataFromEvent(re)
	 * 
	 * Always overrides the data in the node, not checking versions.
	 * 
	 * @param nodePath
	 *            The event node path to update the data
	 * @param re
	 *            the RamaEvent to get the data from
	 * @return true if the node exists and the data was set. false otherwise.
	 */
	private boolean updateEventNode(RamaEvent re) {
		if (zkSubmitter != null) {
			zkSubmitter.submitUpdateNode(re);
		} else {
			String path = getEventPath(getProcessedEventsPath(), re.getId());
			// create async
			zk.create(path, getDataFromEvent(re), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT, null, null);
		}
		return true;
	}

	/**
	 * Reads all events in the switch with id swId folder and updates the log
	 * accordingly.
	 * 
	 * Slaves arrive here by EventWatchers when NodeChildrenChanged
	 * 
	 * @param swId
	 *            the id of the switch
	 * @param watcher
	 *            the watcher to be left on the node getEventsPath(swId)
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private void updateLogFromZK(String path, Watcher watcher)
			throws KeeperException, InterruptedException {

		// get events and leave watcher
		List<String> eventNodes = zk.getChildren(path, watcher);

		if (log.isDebugEnabled()) {
			String s = "Updating log from new events in ZK. ";
			if (getSwitchBuffer().size() != 0) {
				s += getBufferInfo();
			}
			log.debug(s);
		}

		synchronized (getLog()) {
			updateLogFromZK(path, eventNodes);
		} // end synchronized
	}

	void updateLogFromZK(String basePath, List<String> nodes)
			throws InterruptedException {
		for (String nodeName : nodes) {
			long eventId = getIdFromName(nodeName);
			if (eventId == -1L) {
				log.error("Error parsing event id from node name: " + nodeName);
				continue;
			}

			if (getLog().contains(eventId)) {
				continue; // we already have this event in our log
			}

			// we dont have this event in our log yet

			// get data from the created node
			byte[] eventData = getZKNodeData(basePath + "/" + nodeName, null);
			int sleep = 100;
			while (eventData == null || eventData.length == 0) {
				log.warn("Master did not set the data for the created event"
						+ " yet, waiting...");
				Thread.sleep(sleep);
				sleep += 100;
				eventData = getZKNodeData(getEventPath(eventId), null);
				if ((eventData == null || eventData.length == 0)
						&& sleep > 1000) {
					log.error("Could never get data for the created event "
							+ eventId);
					break;
				}
			}

			List<RamaEvent> res = getEventsFromData(eventData);

			if (res == null)
				continue; // error msg is logged by getEventFromData

			handleNewEventsFromMaster(res);
		}
	}

	/**
	 * Adds each event to the log, sends to deliver queue and removes
	 * corresponding buffered event from our slave buffer.
	 * 
	 * @param res
	 */
	private void handleNewEventsFromMaster(List<RamaEvent> res) {
		for (RamaEvent re : res) {
			// we are slave and received new event from the master for
			// this switch.
			RamaEvent prev = getLog().put(re);
			if (prev != null) {
				log.error("Replaced existing event in the log! Prev: " + prev
						+ "; New: " + re);
			}

			log.debug("New event found in ZK: {}", re.toString());
			// im slave and the master just logged this event from a
			// connected switch. Do:
			// 1. check if we can deliver this event
			// 1.1 if yes, deliver (in a new thread?)
			// 1.2 if no, schedule
			// 2. remove event from our buffer

			replicationService.sendToDeliverQueue(re.getId(), re.getSwitchId(),
					re.getMessage());

			// synchronize on log because we may be buffering the same
			// event
			 boolean removed = getSwitchBuffer().remove(re);
			if (removed) {
				log.debug("Slave removed event {} from his buffer. {}",
						re.getId(), getBufferInfo());
			} else {
				// this can happen if the master replicated one or
				// more events before/while we are receiving the
				// event from the switch
				log.warn("## Slave did not have the replicated event "
						+ "in his buffer. Scheduling remove. Event: {}",
						re.getId());
				getSwitchBuffer().ignoreNextEvent(re);
			}
		}
	}

	/**
	 * 
	 * @param eventId
	 * @param sw
	 * @return
	 */
	boolean addAffectedSwitch(long eventId, @Nonnull String swId) {
		return getLog().addAffectedSwitch(eventId, swId);
	}

	public void finishEventDisconnectedSwitch(Long eventId, String swId,
			FloodlightContext cntx) {
		log.info("finishEventDisconnectedSwitch for event {} from switch {}",
				swId.toString(), cntx);
		// TODO finish
	}

	/*************************************************************************/
	/*********************** ZooKeeper watchers ******************************/
	/*************************************************************************/

	/**
	 * Watches for nodes that represent controllers in the controllers folder.
	 * 
	 * Events can be:
	 * 
	 * - Controllers folder is deleted (should never happen)
	 * 
	 * - New child is added or deleted: new controller entered or left
	 * 
	 * Leave new Watcher after process to continue receiving notifications
	 */
	private class ControllersWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			switch (event.getType()) {
			case NodeChildrenChanged:
				try {
					updateControllers();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
			case NodeDeleted:
				log.error("Controllers folder was deleted!!!");
				break;
			default:
				log.error("Unexpected event in SwitchWatcher");
				break;
			}
		}

		private void updateControllers() throws KeeperException,
				InterruptedException {

			Set<String> children = new HashSet<String>(zk.getChildren(
					getControllersPath(), new ControllersWatcher()));
			log.debug(
					"Notified by ZK of changes in controllers.\n\tControllers list in ZK: {}\n\tMy known Controllers: {}",
					children.toString(), knownControllers.toString());
			// union set between known controllers and children
			Set<String> union = new HashSet<>(knownControllers);
			union.addAll(children);
			boolean changes = false;
			for (String c : union) {
				if (knownControllers.contains(c) && children.contains(c)) {
					continue;
				}
				if (knownControllers.contains(c) && !children.contains(c)) {
					// c crashed
					synchronized (mutex) {
						log.info("Locking operations... some controller crashed");
						// do our critical stuff
						handleControllerCrash(c);
					}
				} else {
					// c joined ZK
					registerNewController(c);
				}
				changes = true;
			}
			if (changes) {
				log.debug(
						"Controllers after update: {} ; Leadership after update: {}",
						knownControllers.toString(), leadership.toString());
			}
		}

		private void handleControllerCrash(String c) {
			// find any switches that this controller was master of
			Set<String> switchesWithoutMaster = leadership.get(c);
			
			if (switchesWithoutMaster.size() == 0) {
				log.info("Controller was slave for all switches");
			}

			for (String sw : switchesWithoutMaster) {
				log.info("controller {} crashed and was master of switch {}",
						c, sw);
				if (tryBecomeMaster(sw)) {
					log.info("I am now master for switch {}. Transitioning...",
							sw);
					updateLeadershipMemory(myId, sw);
					transitionToMaster(sw);
				} else {
					log.info("Other controller became master switch {}", sw);
					updateLeadershipFromZK(sw);
				}
			}
			leadership.remove(c);
			knownControllers.remove(c);
		}
	}

	/**
	 * Executes the transition of this controller as being slave of switch swId
	 * to master.
	 * 
	 * @param swId
	 *            the id of the switch
	 * @requires tryBecomeMaster(swId) && isMaster(swId)
	 */
	private void transitionToMaster(String swId) {

		log.info("transitionToMaster: begin");
		RamaLog rlog = getLog();

		if (rlog == null) {
			log.error("transitionToMaster: no log for switch {}", swId);
			return;
		}

		// send barrier to switch
		Semaphore sem = new Semaphore(0);
		EventReplication.sendBarrier(getIOFSwitch(swId), sem);

		// wait if we are still receiving or processing events from ZK
		synchronized (getLog()) {

		}

		// log buffered events
		while (getSwitchBuffer().size() > 0) {
			BufferedEvent be = getSwitchBuffer().poll();
			storeEvent(be.getMessage(), be.getSwitchId());
		}

		// wait barrier reply
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			log.error("Error while waiting for Barrier_Reply:");
			e.printStackTrace();
		}

		// TODO: what if we get a storeSlaveCommit while waiting for the barrier
		// reply? put lock inside storeSlaveCommit and wait here for release?

		// deliver previously logged but unprocessed events to apps
		long lastProcessedId = rlog.getLastProcessedId();
		log.info("transitionToMaster: starting at last processed event: "
				+ lastProcessedId + "; currentId: " + rlog.getCurrentId());

		// synchronize because we need to stop replicating events and receiving
		// replicated events from other masters
		synchronized (getLog()) {
			for (long id = lastProcessedId; id <= rlog.getCurrentId(); id++) {
				RamaEvent re = rlog.getEvent(id);
				if (re == null) {
					log.error("transitionToMaster: event " + id
							+ " doesnt exist in my log yet!");
					continue;
				}
				if (!re.isProcessed()) {
					replicationService.sendToDeliverQueue(re.getId(),
							re.getSwitchId(), re.getMessage());
				} else {
					log.debug("{} already processed.", re.getId());
				}
			}
		}

		// apps will send commands to switches and these events will be marked
		// as processed
		log.info("transitionToMaster: end");
	}

	/**
	 * Watches for nodes that represent events. The wacther must be left again
	 * the the events 'folder' to continue to receive notifications.
	 * 
	 * Slaves set this watcher to receive new events created by master
	 * controllers. This means the event was replicated and its safe to add it
	 * to our log and remove it from temporary buffer.
	 */
	private class EventsWatcher implements Watcher {

		private String swId;

		public EventsWatcher(String swId) {
			this.swId = swId;
		}

		@Override
		public void process(WatchedEvent event) {
			switch (event.getType()) {
			case NodeChildrenChanged:
				try {
					if (!isMaster(swId))
						updateLogFromZK(getReceivedEventsPath(), this);
					// else, dont reset the watcher -> end watcher loop
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
			case NodeDeleted:
				log.error("Events folder was deleted!!!");
				break;
			default:
				log.error("Unexpected event in SwitchWatcher");
				break;
			}
		}
	}

	/**
	 * 
	 * Watches for nodes in switches folder.
	 * 
	 * Events can be:
	 * 
	 * - Events folder is deleted (should never happen)
	 * 
	 * - New child is added or deleted: new switch entered or left
	 * 
	 * Leaves a new Watcher after process to continue receiving notifications
	 * 
	 */
	private class SwitchesWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			switch (event.getType()) {
			case NodeChildrenChanged:
				try {
					List<String> zkSwitches = zk.getChildren(getSwitchesPath(),
							new SwitchesWatcher());
					log.debug(
							"Notified by ZK of changes in switches. Switches list: {}",
							zkSwitches.toString());
					synchronized (connectedSwitches) {
						// wait for our addSwitch to finish

						/*
						 * if we enter here first, he has to wait we add
						 * everything to the in memory data structures and he
						 * sees an updated connectedSwitches which will result
						 * in him returning immediately because the switch was
						 * already added by us
						 */

						log.debug("My connected switches: ",
								connectedSwitches.toString());

						// union set between connected switches and children
						Set<String> union = new HashSet<>(connectedSwitches);
						union.addAll(zkSwitches);
						for (String sw : union) {
							if (connectedSwitches.contains(sw)
									&& zkSwitches.contains(sw)) {
								continue;
							} else if (zkSwitches.contains(sw)
									&& !connectedSwitches.contains(sw)) {
								// sw added to ZK by the master, we are slave
								if (initSlave(sw, true)) {
									addToConnectedSwitches(sw);
								}
							} else { // switch removed
								// should not happen while controller is on
								// switches are persistent and only deleted
								// manually from the ZK client when cleaning all
								// data to reset.
							}
						}
					}
					log.debug("Connected switches after updating from ZK: {}",
							connectedSwitches.toString());
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
			case NodeDeleted:
				log.error("SwitchesWatcher: Events folder was deleted!");
				break;
			default:
				log.error("Unexpected event in SwitchesWatcher");
				break;
			}
		}
	}

	/*************************************************************************/
	/****************************** Prints ***********************************/
	/*************************************************************************/

	void printLogInfo() {
		StringBuilder sb = new StringBuilder("\nMy id: " + myId + "; ");
		sb.append(switchesLog.toString());
		int processed = 0;
		List<RamaEvent> unprocessedEvents = new LinkedList<RamaEvent>();
		boolean printEachEvent = switchesLog.size() < 100;
		for (RamaEvent re : switchesLog) {
			if (re.isProcessed()) {
				processed++;
			} else {
				unprocessedEvents.add(re);
			}
			if (printEachEvent) {
				if (connectedSwitches.contains(re.getSwitchId()))
					if (leadership.get(myId).contains(re.getSwitchId()))
						sb.append("(Master) ");
					else
						sb.append("(Slave) ");
				sb.append(re.toString() + "\n");
			}
		}
		sb.append("\nnum events=" + switchesLog.size() + " processed="
				+ processed + " unprocessed=" + unprocessedEvents.size());
		if (unprocessedEvents.size() > 0) {
			for (RamaEvent re : unprocessedEvents)
				sb.append("\n" + re.toString());
		}
		log.info(sb.toString());
	}

	void printLeadership() {
		StringBuilder sb = new StringBuilder("\nLeadership:\n");
		for (String c : leadership.keySet()) {
			if (c.equals(myId))
				sb.append("(me)");
			sb.append("\t" + c + "=" + leadership.get(c) + "\n");
		}
		log.info(sb.toString());
	}

	void printBufferInfo() {
		log.info(getBufferInfo());
	}

	private String getBufferInfo() {
		StringBuilder sb = new StringBuilder("Buffered Events in controller "
				+ myId + ":");
		Iterator<?> itr = switchesBuffer.iterator();
		while (itr.hasNext()) {
			sb.append("\n\t" + itr.next());
		}
		return sb.toString();
	}

	/*************************************************************************/

	/**
	 * 
	 * @param initTime
	 *            the init time got with System.currentTimeMillis();
	 * @return the elapsed time since initTime in seconds
	 */
	public static final double getElapsedTime(long start) {
		return getElapsedTime(start, System.currentTimeMillis());
	}

	/**
	 * 
	 * @param start
	 *            the start time got with System.currentTimeMillis();
	 * @param end
	 *            the end time got with System.currentTimeMillis();
	 * @return the elapsed time between start and end
	 */
	public static final double getElapsedTime(long start, long end) {
		return (end - start) / 1000.0;
	}

	/**
	 * 
	 * @param start
	 *            the start time got with System.nanoTime();
	 * @param end
	 *            the end time got with System.nanoTime();
	 * @return the elapsed time between start and end in ms
	 */
	public static final double getElapsedTimeNanos(long start, long end) {
		return (end - start) / 1000000.0;
	}

	/**
	 * 
	 * @param initTime
	 *            the start time got with System.nanoTime();
	 * @return the elapsed time since initTime in miliseconds
	 */
	public static final double getElapsedTimeNanos(long start) {
		return getElapsedTimeNanos(start, System.nanoTime());
	}

	public void processEvent(long eventId) {
		if (getLog().getEvent(eventId) != null) {
			getLog().getEvent(eventId).process();
		} else {
			log.warn("Tried to process non existing event {}", eventId);
		}
	}

	public String getId() {
		return myId;
	}
}
