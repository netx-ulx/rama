package net.floodlightcontroller.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import net.floodlightcontroller.core.IOFConnection;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.LogicalOFMessageCategory;
import net.floodlightcontroller.core.internal.OFSwitch;

import org.projectfloodlight.openflow.protocol.BundleIdGenerators;
import org.projectfloodlight.openflow.protocol.OFBundleAddMsg;
import org.projectfloodlight.openflow.protocol.OFBundleCtrlMsg;
import org.projectfloodlight.openflow.protocol.OFBundleCtrlType;
import org.projectfloodlight.openflow.protocol.OFBundleFlags;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.types.BundleId;
import org.python.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class OFBundle {

	public static final Set<OFBundleFlags> ALL_BUNDLE_FLAGS = Sets
			.immutableEnumSet(OFBundleFlags.ATOMIC, OFBundleFlags.ORDERED);

	public static final Set<OFBundleFlags> ATOMIC_BUNDLE_FLAG = Sets
			.immutableEnumSet(OFBundleFlags.ATOMIC);

	public static final Set<OFBundleFlags> ORDERED_BUNDLE_FLAG = Sets
			.immutableEnumSet(OFBundleFlags.ORDERED);

	public static final Set<OFBundleFlags> NO_FLAGS = Collections.emptySet();

	public static final OFFactory factory = OFFactories
			.getFactory(OFVersion.OF_14);

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static OFBundleCtrlMsg createBundleCtrlMsg(OFBundleCtrlType type,
			int id, Set<OFBundleFlags> flags) {
		return factory.buildBundleCtrlMsg().setBundleCtrlType(type)
				.setBundleId(BundleId.of(id)).setFlags(flags).build();
	}

	public static OFBundleCtrlMsg createBundleCtrlMsg(OFBundleCtrlType type,
			int id, Set<OFBundleFlags> flags, long xid) {
		return factory.buildBundleCtrlMsg().setBundleCtrlType(type)
				.setBundleId(BundleId.of(id)).setFlags(flags).setXid(xid)
				.build();
	}

	public static OFBundleAddMsg createBundleAddMsg(int id,
			Set<OFBundleFlags> flags, OFMessage msg) {
		return factory.buildBundleAddMsg().setBundleId(BundleId.of(id))
				.setFlags(flags).setData(msg).setXid(msg.getXid()).build();
	}

	protected static Logger log = LoggerFactory.getLogger(OFBundle.class);

	private final int bundleId;
	private final Set<OFBundleFlags> bundleFlags;
	private long eventId;
	private final IOFSwitch sw;
	private List<OFMessage> msgsToSend;

	/** Immutable switch ID */
	private final String swId;

	private final IOFConnection conn;

	/**
	 * If we opened the bundle in the switch (i.e., if we sent a OPEN_REQUEST to
	 * the switch)
	 */
	private boolean openSent;

	/**
	 * If this bundle was successfully opened (i.e., if we received a OPEN_REPLY
	 * from the switch)
	 */
	private boolean openReceived;

	/**
	 * If this bundle was successfully committed (i.e., if we received a
	 * COMMIT_REPLY from the switch)
	 */
	private boolean commitSent;

	/**
	 * If this bundle was successfully committed (i.e., if we received a
	 * COMMIT_REPLY from the switch)
	 */
	private boolean commitReceived;

	/**
	 * If this bundle already sent a CLOSE_REQUEST. No messages should be added
	 * to this bundle after this.
	 */
	private boolean closeSent;

	/**
	 * If this bundle was successfully closed (i.e., if we received a
	 * CLOSE_REPLY from the switch)
	 */
	private boolean closeReceived;

	/**
	 * Creates an OFBundle object with a new bundle id and sends an OPEN_REQUEST
	 * to the switch. Uses the main connection with the switch sw to send
	 * messages.
	 * 
	 * Uses no flags in the bundle.
	 * 
	 * @param sw
	 *            the switch
	 */
	public OFBundle(@Nonnull IOFSwitch sw) throws IllegalArgumentException {
		this(sw, ((OFSwitch) sw).getConnection(LogicalOFMessageCategory.MAIN
				.getAuxId()), null);
	}

	/**
	 * Creates an OFBundle object with a new bundle id and sends an OPEN_REQUEST
	 * to the switch. Uses the main connection with the switch sw to send
	 * messages.
	 * 
	 * @param sw
	 *            the switch
	 * @param flags
	 *            flags to be used in this bundle
	 */
	public OFBundle(IOFSwitch sw, Set<OFBundleFlags> flags)
			throws IllegalArgumentException {
		this(sw, ((OFSwitch) sw)
				.getConnectionByCategory(LogicalOFMessageCategory.MAIN), flags);
	}

	/**
	 * Creates an OFBundle object with a new bundle id and sends an OPEN_REQUEST
	 * to the switch. If null is passed to flags, the bundle will use
	 * ORDERED_BUNDLE_FLAG as default.
	 * 
	 * Registers a callback for when the controller receives a OPEN_REPLY
	 * message
	 * 
	 * @param sw
	 *            the switch
	 * @param flags
	 *            flags to be used in this bundle
	 * @param callback
	 *            callback function to be executed when the OPEN_REQUEST_REPLY
	 *            arrives at the controller.
	 */
	public OFBundle(IOFSwitch sw, IOFConnection conn, Set<OFBundleFlags> flags)
			throws IllegalArgumentException {

		if (flags == null)
			flags = ORDERED_BUNDLE_FLAG;

		if (sw == null) {
			throw new IllegalArgumentException("sw cannot be null");
		} else if (flags == null) {
			throw new IllegalArgumentException("flags cannot be null");
			/*
			 * } else if
			 * (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_14) < 0) {
			 * throw new IllegalArgumentException(
			 * "Bundles not supported in this OF version (" +
			 * sw.getOFFactory().getVersion().toString() + ")");
			 */
		} else if (!(flags.equals(ALL_BUNDLE_FLAGS)
				|| flags.equals(ATOMIC_BUNDLE_FLAG)
				|| flags.equals(ORDERED_BUNDLE_FLAG) || flags.equals(NO_FLAGS))) {
			throw new IllegalArgumentException("Unknown Set of bundle flags ("
					+ flags.toString() + ")");
		}

		this.openSent = false;
		this.openReceived = false;
		this.commitSent = false;
		this.commitReceived = false;
		this.closeSent = false;
		this.closeReceived = false;
		this.bundleId = BundleIdGenerators.global().nextBundleId().getInt();
		this.bundleFlags = flags;
		this.eventId = -1;
		this.sw = sw;
		this.swId = sw.getId().toString();
		this.conn = conn;
		this.msgsToSend = new LinkedList<OFMessage>();
	}

	public int getBundleId() {
		return bundleId;
	}

	public Set<OFBundleFlags> getFlags() {
		return bundleFlags;
	}

	public IOFSwitch getSwitch() {
		return sw;
	}

	public boolean isOpen() {
		return openSent;
	}

	public boolean isCommited() {
		return commitSent;
	}

	public boolean isClosed() {
		return closeSent || commitSent;
	}

	public String getSwitchId() {
		return swId;
	}

	public boolean open() {
		return open(null);
	}

	public boolean open(FutureCallback<OFBundleCtrlMsg> callback) {
		OFBundleCtrlMsg open = createBundleCtrlMsg(
				OFBundleCtrlType.OPEN_REQUEST, this.bundleId, this.bundleFlags);

		ListenableFuture<OFBundleCtrlMsg> future = sw.writeRequest(open);

		openSent = true;

		if (callback != null) {
			Futures.addCallback(future, callback);
		}

		log.debug(
				"Sent Open Request for Bundle {} in switch [{}]: {}",
				new Object[] { getBundleId(), sw.getId().toString(),
						open.toString() });
		return true;
	}

	/**
	 * 
	 * @param msg
	 *            the msg to be added to the bundle
	 * @param eventId
	 *            the event that originated this call
	 * @param conn
	 *            the connection where the BUNDLE_ADD_MSG should be sent
	 * @return
	 */
	public boolean add(OFMessage msg) {
		synchronized (this) { // lock till we finish adding
			Collection<OFMessage> result = add(Collections.singletonList(msg));
			return (result == null) ? false : result.size() == 0;
		}
	}

	/**
	 * Returns null if this bundle is already closed or committed. Returns an
	 * empty collection if all messages were sent. Otherwise returns a
	 * collection with the unsent messages.
	 * 
	 * @param msglist
	 *            the list of messages to be added to the bundle
	 * @param eventId
	 *            the event that originated this call
	 * @param conn
	 *            the connection where the BUNDLE_ADD_MSG should be sent
	 * @return
	 */
	public Collection<OFMessage> add(List<OFMessage> msglist) {
		// synchronized (this) { // lock till we finish adding
		if (closeSent || commitSent) {
			log.error("Attempting to add message to bundle already closed or committed."
					+ this.toString());
			return null;
		}

		if (msglist == null || msglist.size() == 0)
			return Collections.emptyList();

		for (OFMessage m : msglist) {
			msgsToSend.add(createBundleAddMsg(bundleId, bundleFlags, m));
		}

		return Lists.newLinkedList();
		// }
	}

	public void close() {
		close(null);
	}
	
	private void finishBundle() {
		Collection<OFMessage> result = conn.write(msgsToSend);

		if (result.isEmpty()) {
			msgsToSend = null;
		} else {
			for (OFMessage m : msgsToSend) {
				if (!result.contains(m)) {
					log.debug(
							"Msg added to bundle {} " + " in switch {}:"
									+ m.toString(), getBundleId(), sw.getId()
									.toString());
				} else {
					log.debug("Error adding msg to bundle {} "
							+ " in switch {}:" + m.toString(), getBundleId(),
							sw.getId().toString());
				}
			}
		}
	}

	public void close(FutureCallback<OFBundleCtrlMsg> callback) {
		// synchronized (this) { // sync with add to this bundle
		
		finishBundle();
		
		
		OFBundleCtrlMsg close = createBundleCtrlMsg(
				OFBundleCtrlType.CLOSE_REQUEST, bundleId, bundleFlags);

		ListenableFuture<OFBundleCtrlMsg> future = sw.writeRequest(close);

		closeSent = true;

		log.debug(
				"Sent Close Request for Bundle {} in switch {} [{}]",
				new Object[] { getBundleId(), sw.getId().toString(),
						close.toString() });

		if (callback != null) {
			// if we are only closing and client wants a callback
			Futures.addCallback(future, callback);
		}
		// }
	}

	public void commit() {
		commit(null);
	}

	public void commit(FutureCallback<OFBundleCtrlMsg> callback) {
		// synchronized (this) { // sync with add to this bundle
		
		if (!closeSent) {
			finishBundle();
		}
		
		OFBundleCtrlMsg commit = createBundleCtrlMsg(
				OFBundleCtrlType.COMMIT_REQUEST, bundleId, bundleFlags);

		ListenableFuture<OFBundleCtrlMsg> future = sw.writeRequest(commit);

		commitSent = true;

		if (callback != null) {
			Futures.addCallback(future, callback);
		}

		log.debug(
				"Sent commit request for bundle {} to switch {}; Full msg={}",
				new Object[] { getBundleId(), sw.getId().toString(), commit });
		// }
	}

	/**
	 * 
	 * @param eId
	 */
	public void setEventId(long eId) {
		eventId = eId;
	}

	/**
	 * The ids of the events that generated the commands in this buffer.
	 * 
	 * @return
	 */
	public long getEventId() {
		return eventId;
	}

	/**
	 * Utility method for printing.
	 * 
	 * @param msgs
	 * @return the OFType of each OFMessage separated by ,
	 */
	public static String getMsgTypes(Collection<OFMessage> msgs) {
		StringBuilder sb = new StringBuilder("");
		for (OFMessage m : msgs)
			sb.append(m.getType() + ",");
		sb.setLength(Math.max(sb.length() - 1, 0));
		return sb.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("OFBundle [id=" + bundleId
				+ ", flags=" + bundleFlags + ", eventIds=" + eventId + ", sw="
				+ sw.getId() + ", open=[");
		if (openSent)
			sb.append("sent ");
		else
			sb.append("X ");
		if (openReceived)
			sb.append("received");
		else
			sb.append("X");

		sb.append("] close=[");

		if (closeSent)
			sb.append("sent ");
		else
			sb.append("X ");
		if (closeReceived)
			sb.append("received");
		else
			sb.append("X");

		sb.append("] commit=[");

		if (commitSent)
			sb.append("sent ");
		else
			sb.append("X ");
		if (commitReceived)
			sb.append("received]");
		else
			sb.append("X]");

		sb.append(" ]");

		return sb.toString();
	}

	public void commitReplyReceived() {
		this.commitReceived = true;		
	}
}
