package net.floodlightcontroller.core.rama;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.slf4j.Logger;

/**
 * 
 * An in-memory log maintained by each Rama Controller.
 * 
 * Iterating over this collection will return all events in order.
 * 
 * @author Andre Mantas
 *
 */
public class RamaLog implements Iterable<RamaEvent> {

	/**
	 * In-memory shared log of events for master and slave controllers.
	 */
	private Map<Long, RamaEvent> eventLog;

	private AtomicLong currentId;

	private AtomicLong lastProcessedEvent;
	
	private Logger log;

	RamaLog(Logger log) {
		this.eventLog = new ConcurrentHashMap<Long, RamaEvent>(100);
		this.currentId = new AtomicLong(ZKManager.FIRST_ID - 1);
		this.lastProcessedEvent = new AtomicLong(ZKManager.FIRST_ID - 1);
		this.log = log;
	}

	/**
	 * 
	 * @return
	 */
	long getCurrentId() {
		return currentId.get();
	}

	/**
	 * Returns the id of the last processed event or 0 if no event was processed
	 * yet.
	 * 
	 * @return
	 */
	long getLastProcessedId() {
		synchronized (this) {
			// wait for writes to finish
		}
		updateLastProcessed();
		return lastProcessedEvent.get();
	}

	private void updateLastProcessed() {
		long last;
		while ((last = lastProcessedEvent.get()) < currentId.get()) {
			last++;
			if (!eventLog.containsKey(last)) {
				break;
			}
			if (eventLog.get(last).isProcessed()) {
				lastProcessedEvent.incrementAndGet();
			} else {
				break;
			}
		}
	}

	int size() {
		return eventLog.size();
	}

	/**
	 * @param re
	 *            the event to check
	 * @return true if this RamaLog contains an event with re.getId()
	 * @requires re != null
	 */
	boolean contains(RamaEvent re) {
		return eventLog.containsKey(re.getId());
	}

	/**
	 * 
	 * @param eventId
	 * @return
	 */
	boolean contains(long eventId) {
		return eventLog.containsKey(eventId);
	}

	/**
	 * Adds the OFMessage m from the switch swId to the log.
	 * 
	 * @param m
	 *            the message
	 * @param swId
	 *            the id of the switch that sent this message
	 * @param id
	 *            the id decided by a total order algorithm between master
	 *            controllers
	 * @requires m != null && m != null
	 * @return the newly created RamaEvent
	 */

	/**
	 * 
	 * @param re
	 * @return
	 */
	RamaEvent add(RamaEvent re) {
		// synchronized (this) { // sync with put(RamaEvent re)
		if (re.getId() > currentId.get()) {
			currentId.set(re.getId());
			// TODO: CARE: this may leave holes in the middle of the log.
			// e.g.: 2 master controllers for 2 switches receive events at
			// same time. this controller gets the second id and we did not
			// yet receive the other event from that master
		}
		// RamaEvent re = new RamaEvent(m, swId, id);
		// this add never fails because we just incremented the id
		addToLog(re);
		return re;
		// }
	}

	/**
	 * Adds the OFMessage m from the switch swId to the log.
	 * 
	 * @param m
	 *            the message
	 * @param swId
	 *            the id of the switch that sent this message
	 * @requires m != null && m != null
	 * @return the newly created RamaEvent
	 */
	RamaEvent add(OFMessage m, String swId) {
		RamaEvent re = new RamaEvent(m, swId, currentId.incrementAndGet());
		addToLog(re);
		return re;
	}

	/**
	 * Puts the event re in the log of events, indexed by re.getId().
	 * 
	 * Updates currentId if this RamaEvent has bigger id
	 * 
	 * @requires !contains(re.getId())
	 * @requires getSwitchId().equals(re.getSwitchId())
	 * @param re
	 *            the RamaEvent to put in specific index
	 * @requires re != null
	 * @return the previous RamaEvent in the same id, if any. Otherwise returns
	 *         null
	 */
	RamaEvent put(RamaEvent re) {
		synchronized (this) { // sync with add(OFMessage m)
			RamaEvent result = addToLog(re);
			if (currentId.get() < re.getId()) {
				currentId.set(re.getId());
			}
			return result;
		}
	}

	/**
	 * @requires re != null
	 * @param re
	 */
	private RamaEvent addToLog(RamaEvent re) {
		return eventLog.put(re.getId(), re);
	}

	/**
	 * Adds the commit reply from the switch swId to the received commit reply
	 * collection of the event eventId.
	 * 
	 * An event is only "processed" when the collection of received commit
	 * replies is the same as the collection of affected switches. Therefore,
	 * this method returning true does not mean that the event is processed.
	 * 
	 * @param eventId
	 *            the id of the event
	 * @param swId
	 *            the if of the switch
	 * @return false if this event is not in the log or if it is already
	 *         processed. True otherwise.
	 */
	boolean addReceivedCommitReply(long eventId, String swId) {
		if (!eventLog.containsKey(eventId)) {
			log.warn("adding commit reply from switch {}: Event {} does not exist", swId, eventId);
			return false;
		} else if (eventLog.get(eventId).isProcessed()) {
			log.warn("adding commit reply from switch {}: Event {} is already processed", swId, eventId);
			return false;
		}
		
		RamaEvent re = eventLog.get(eventId);
		boolean added = re.addReceivedCommitReply(swId);
		/*
		 * if (re.isProcessed()) updateLastProcessed();
		 */
		return added;
	}

	/**
	 * @param id
	 *            the id of the event
	 * @return the event with the specified id or null if it is not present in
	 *         the log.
	 */
	RamaEvent getEvent(long id) {
		return eventLog.get(id);
	}

	/**
	 * Warning: Do not modify the returned set. Read-only! Changes in the set
	 * will affect the log.
	 * 
	 * @return the (unordered) set of keys (ids) of this log.
	 */
	Set<Long> keySet() {
		return eventLog.keySet();
	}

	public boolean addAffectedSwitch(long eventId, String switchId) {
		RamaEvent re = getEvent(eventId);
		return re != null ? re.addAffectedSwitch(switchId) : false;
	}

	/**
	 * 
	 * @param eId
	 *            the id of the event
	 * @return true if the event with id eId is marked as processed.
	 * @requires contains(eId)
	 */
	public boolean isProcessed(long eId) {
		return getEvent(eId).isProcessed();
	}

	@Override
	public Iterator<RamaEvent> iterator() {
		List<RamaEvent> list = new LinkedList<RamaEvent>();
		long i = ZKManager.FIRST_ID;
		// make sure to check each time if any event was added
		while (i <= getCurrentId()) {
			RamaEvent re = getEvent(i);
			if (re != null)
				list.add(re);
			i++;
		}
		return list.iterator();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("RamaLog [currentId="
				+ getCurrentId() + ", lastProcessedEvent="
				+ getLastProcessedId() + ", size=" + eventLog.size() + "]");

		return sb.append("\n").toString();
	}
}
