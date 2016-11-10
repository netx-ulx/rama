package net.floodlightcontroller.core.rama;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import net.floodlightcontroller.util.OFMessageUtils;

import org.projectfloodlight.openflow.exceptions.OFParseError;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFVersion;

public class RamaEvent implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8150564048183425748L;

	/**
	 * The id of this event. Volatile because the id is in the node name.
	 */
	private long id;

	/**
	 * Determines if this event was fully processed or not. An event should be
	 * marked as processed after the switch ack the reception of all commands
	 * originated from this event.
	 */
	private boolean processed;

	/**
	 * The switch id that sent the OFMessage of this event.
	 */
	private String swId;

	/**
	 * The set of affected switches by this event. Affected switches are
	 * switches that receive commands generated from this event.
	 */
	private Set<String> affectedSwitches;

	private Set<String> receivedCommitReplies;

	/**
	 * The OFMessage received from the switch that originated this event.
	 * Transient because it is not Serializable. Instead, the byte[]
	 * serializedMsg is used
	 */
	private transient OFMessage m;

	/**
	 * The serialized OFMessage
	 */
	private byte[] serializedMsg;

	private OFVersion version;

	public final static transient int AVG_MAX_SIZE = 600;

	RamaEvent(OFMessage m, String swId, long id) {
		this.m = m;
		this.version = m.getVersion();
		this.swId = swId;
		this.id = id;
		this.processed = false;
		this.affectedSwitches = new HashSet<String>();
		this.receivedCommitReplies = new HashSet<String>();

		ByteBuf buf = Unpooled.buffer(AVG_MAX_SIZE);
		m.writeTo(buf);
		if (buf.hasArray()) {
			serializedMsg = buf.array();
		} else {
			serializedMsg = new byte[buf.readableBytes()];
			buf.readBytes(serializedMsg);
		}
	}

	/**
	 * Use after deserialization to initialize the message of this event. After
	 * this call, this.getMessage() will return the OFMessage object
	 * representing the serialized message
	 */
	void updateMessage() {
		ByteBuf buf = Unpooled.copiedBuffer(serializedMsg);
		try {
			this.m = OFFactories.getFactory(this.version).getReader()
					.readFrom(buf);
		} catch (OFParseError e) {
			e.printStackTrace();
		}
	}

	/**
	 * Marks the switch swId as being affected by this event
	 * 
	 * @param swId
	 */
	boolean addAffectedSwitch(String swId) {
		return affectedSwitches.add(swId);
	}

	/**
	 * 
	 * @return a set of affected switches by this event.
	 */
	Set<String> getAffectedSwitches() {
		return affectedSwitches;
	}

	/**
	 * Adds the commit reply from the switch swId to the received commit reply
	 * collection of this event.
	 * 
	 * An event is only "processed" when the collection of received commit
	 * replies is the same as the collection of affected switches.
	 * 
	 * @param swId
	 * @return
	 */
	boolean addReceivedCommitReply(String swId) {
		boolean added = receivedCommitReplies.add(swId);
		// containsAll also if affectedSwitches is empty
		if (receivedCommitReplies.containsAll(affectedSwitches))
			this.process();
		return added;
	}

	String getSwitchId() {
		return swId;
	}

	OFMessage getMessage() {
		return m;
	}

	long getId() {
		return id;
	}

	/**
	 * An event is only "processed" when the collection of received commit
	 * replies is the same as the collection of affected switches.
	 * 
	 * @return if this event is processed or not
	 */
	boolean isProcessed() {
		return processed;
	}

	void process() {
		this.processed = true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RamaEvent [id=" + id + ",\t processed=" + processed + ",\t sw="
				+ swId + ", affectedSwitches=" + affectedSwitches.toString()
				+ ", receivedCommitReplies=" + receivedCommitReplies.toString()
				+ ", " + OFMessageUtils.msgToString(m) + " ]";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + (processed ? 1231 : 1237);
		// we do not use the serialized message for hash codes and equals
		// result = prime * result + Arrays.hashCode(serializedMsg);
		result = prime
				* result
				+ ((affectedSwitches == null) ? 0 : affectedSwitches.hashCode());
		result = prime * result + ((swId == null) ? 0 : swId.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RamaEvent)) {
			return false;
		}
		RamaEvent other = (RamaEvent) obj;
		if (id != other.id) {
			return false;
		}
		if (processed != other.processed) {
			return false;
		}
		// we do not use the serialized message for hash codes and equals
		/*
		 * if (!Arrays.equals(serializedMsg, other.serializedMsg)) { return
		 * false; }
		 */
		if (swId == null) {
			if (other.swId != null) {
				return false;
			}
		} else if (!swId.equals(other.swId)) {
			return false;
		}

		if (affectedSwitches == null) {
			if (other.affectedSwitches != null) {
				return false;
			}
		} else if (!affectedSwitches.equals(other.affectedSwitches)) {
			return false;
		}

		return true;
	}

	public void setEventId(long eventId) {
		this.id = eventId;
	}
}