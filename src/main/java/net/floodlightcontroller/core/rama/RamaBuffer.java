package net.floodlightcontroller.core.rama;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.floodlightcontroller.util.OFMessageUtils;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RamaBuffer
		implements
		Iterable<net.floodlightcontroller.core.rama.RamaBuffer.BufferedEvent> {

	private static final Logger log = LoggerFactory.getLogger(RamaBuffer.class);
	private ConcurrentLinkedQueue<BufferedEvent> buffer;
	private Map<String, List<OFMessage>> eventsToIgnore;

	class BufferedEvent {

		private OFMessage m;
		private String swId;

		/**
		 * Creates a new buffered event with the given params. The message
		 * maintained by this buffered event has its xid set to 0 regardless of
		 * the xid of m
		 * 
		 * @param m
		 *            the OFMessage sent by the switch
		 * @param swId
		 *            the switch id
		 */
		BufferedEvent(OFMessage m, String swId) {
			// we need xid = 0 to be able to compare messages to dif controllers
			this.m = resetMessageXid(m);
			this.swId = swId;
		}

		/**
		 * @return the m
		 */
		OFMessage getMessage() {
			return m;
		}

		/**
		 * @return the swId
		 */
		String getSwitchId() {
			return swId;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "BufferedEvent [swId=" + swId + ", "
					+ OFMessageUtils.msgToString(m) + "]";
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
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((m == null) ? 0 : m.hashCode());
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
			if (!(obj instanceof BufferedEvent)) {
				return false;
			}
			BufferedEvent other = (BufferedEvent) obj;
			if (!getOuterType().equals(other.getOuterType())) {
				return false;
			}
			if (m == null) {
				if (other.m != null) {
					return false;
				}
			} else if (!m.equals(other.m)) {
				return false;
			}
			if (swId == null) {
				if (other.swId != null) {
					return false;
				}
			} else if (!swId.equals(other.swId)) {
				return false;
			}
			return true;
		}

		private RamaBuffer getOuterType() {
			return RamaBuffer.this;
		}

	}

	/**
	 * Creates a new buffer that allows to store temporary 'buffered events'
	 * that consist of pairs <switchId, message>. A log is associated to the
	 * buffer event so ease the task of adding buffered events to the log when
	 * necessary. Allows ignoring future events when trying to add them to the
	 * buffer.
	 * 
	 * @param rLog
	 */
	RamaBuffer() {
		this.buffer = new ConcurrentLinkedQueue<BufferedEvent>();
		this.eventsToIgnore = new ConcurrentHashMap<String, List<OFMessage>>();
	}

	/**
	 * Adds a new buffered event 'be' to the buffer such that be.msg == m &&
	 * be.swId == swId. This event is only added to the buffer if there was no
	 * scheduling to ignore an event with this parameters (the pair <swId, m>)
	 * 
	 * @param m
	 * @param swId
	 * @return true if an event was added to the buffer or false if the event
	 *         was not added because it was supposed to be ignored.
	 */
	public boolean add(OFMessage m, String swId) {
		// TODO: check if slave incrementing id is safe for consistency
		// long id = getNextBufferedEventId(swId, m); RamaEvent re = new
		// RamaEvent(m, swId, id);
		//
		// // this event can already be in our log because of updateLogFromZK
		// slave replicated one event, we got notified. when we call
		// getChildren from ZK, it can return 1 or more new events if
		// (getSwitchLog(swId).contains(id)) {
		// log.warn("Slave tried to buffer event but it was already in the log!
		// "
		// + re.toString()); } else { addEventToBuffer(re);
		// log.info("Slave buffered new event: " + re.toString() + "; " +
		// getBufferInfo()); }
		List<OFMessage> eti = getEventsToIgnore(swId);
		if (eti != null && eti.remove(m)) {
			log.info("Ignoring buffering of message from switch " + swId
					+ " because it is already logged");
			return false;
		} else {
			addEventToBuffer(m, swId);
			return true;
		}
	}

	/**
	 * @param swId
	 *            the switch id
	 * @return a list of events to ignore from switch swId
	 */
	private List<OFMessage> getEventsToIgnore(String swId) {
		return eventsToIgnore.get(swId);
	}

	/**
	 * Simply adds a new BufferedEvent to the end of the buffer.
	 * 
	 * @param m
	 *            the message sent by the switch
	 * @param swId
	 *            the switch di
	 * @requires m.getXid() == 0 to properly work later
	 */
	private void addEventToBuffer(OFMessage m, String swId) {
		buffer.add(new BufferedEvent(m, swId));
	}

	/**
	 * Removes the first buffered event 'be' from the buffer such that be.swId
	 * == re.swId && be.msg == re.msg. If this multiple equal messages from the
	 * same swId, only one will be removed.
	 * 
	 * @param re
	 * @return true if one buffered event was removed from the buffer (i.e., the
	 *         buffered changed as result of this call), or false if no buffered
	 *         event was removed
	 */
	public boolean remove(RamaEvent re) {
		// create a temporary buffered event with the data from the rama event
		// queue.remove will remove the first element that is .equals()
		return buffer.remove(createBufferedEventFromRamaEvent(re));
	}

	/**
	 * 
	 * @param re
	 *            the RamaEvent
	 * @return a new BufferedEvent with the same parameters as the RamaEvent
	 */
	private BufferedEvent createBufferedEventFromRamaEvent(RamaEvent re) {
		return new BufferedEvent(re.getMessage(), re.getSwitchId());
	}

	@SuppressWarnings("unused")
	private boolean areEquals(RamaEvent re, BufferedEvent be) {
		return be.getSwitchId().equals(re.getSwitchId())
				&& be.getMessage().equals(re.getMessage());
	}

	int size() {
		return buffer.size();
	}

	/**
	 * Retrieves and removes the first (oldest) Buffered Event of this buffer,
	 * or returns null if this buffer is empty
	 * 
	 * @return the first event from this buffer
	 */
	BufferedEvent poll() {
		return buffer.poll();
	}

	/**
	 * Makes sure that one buffered event consisting of the pair <re.swId,
	 * re.msg> will be ignored when trying to add it to the buffer. For the same
	 * switch, multiple (equal) messages may be ignored if this method is called
	 * multiple times, resulting in ignoring multiple calls of this.add
	 * 
	 * @param re
	 *            the event to ignore in the future
	 */
	void ignoreNextEvent(RamaEvent re) {
		if (!eventsToIgnore.containsKey(re.getSwitchId()))
			eventsToIgnore.put(re.getSwitchId(), new ArrayList<OFMessage>());

		OFMessage m = re.getMessage();
		if (m.getXid() != 0)
			m = resetMessageXid(m);

		ignoreNextMessageFromSwitch(re.getSwitchId(), m);
	}

	private void ignoreNextMessageFromSwitch(String swId, OFMessage m) {
		eventsToIgnore.get(swId).add(m);
	}

	/**
	 * @param m
	 *            the OFMessage whose xid will be replaced
	 * @return the same OFMessage m but with the xid set to 0
	 */
	private OFMessage resetMessageXid(OFMessage m) {
		return m.createBuilder().setXid(0).build();
	}

	/**
	 * 
	 * @param re
	 * @return true if this event was successfully added to the log in the index
	 *         re.getId() or false if there was already a RamaEvent in that
	 *         position.
	 */
	// private boolean addExistingEventToLog(RamaEvent re) {
	// // TODO v1:
	// // aqui podemos usar apenas .put(re) para por no Id que o 're' ja tem.
	// // Ao usar o add fazemos append ao log, e ignoramos o id do 're'.
	// // Apesar de que vai dar ao mesmo id a partida
	//
	// // TODO v2:
	// // se ja temos o RamaEvent, ja temos um id unico para este
	// // evento/mensagem
	// // se vamos adicionar ao logo como evento novo vai usar um id diferente
	// // (novo) porque incrementa o currentId do log
	// RamaEvent old = getSwitchLog(re.getSwitchId()).put(re);
	// return old == null;
	// }

	void clear() {
		buffer = new ConcurrentLinkedQueue<BufferedEvent>();
	}

	@Override
	public Iterator<BufferedEvent> iterator() {
		return buffer.iterator();
	}
}