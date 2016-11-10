package net.floodlightcontroller.core.rama;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFSwitch;

public class OldCode {

	// @Override
	// public void onSwitchUpdateFinish(IOFSwitch sw, FloodlightContext cntx) {
	// lock.lock();
	// portStatusDelivered.signal();
	// lock.unlock();
	// onMessageConsumed(sw, cntx);
	// }

	// private static final ReentrantLock lock = new ReentrantLock();
	// private static final Condition hasPortStatus = lock.newCondition();
	// private static final Condition portStatusDelivered = lock.newCondition();
	// private static final ReentrantLock eventsLock = new ReentrantLock();
	// private static final Condition hasEvents = eventsLock.newCondition();

	// @Override
	// public Object[] getNextToDeliver() {
	// try {
	// Object[] result;
	// synchronized (toDeliverQueue) {
	// result = toDeliverQueue.peek();
	// if (result == null) {
	// eventsLock.lock();
	// hasEvents.await();
	// result = toDeliverQueue.peek();
	// eventsLock.unlock();
	// }
	// OFMessage m = (OFMessage) result[1];
	// while (m.getType() == OFType.PORT_STATUS) {
	// // notify thread waiting on getNextPortStatusToDeliver
	// lock.lock();
	// hasPortStatus.signal();
	// // wait that the port status finishes
	// portStatusDelivered.await();
	// lock.unlock();
	// result = toDeliverQueue.peek();
	// m = (OFMessage) result[1];
	// }
	// result = toDeliverQueue.take();
	// }
	//
	// IOFSwitch old_sw = (IOFSwitch) result[0];
	// IOFSwitch new_sw = null;
	// if (old_sw != null) {
	// new_sw = switchService.getSwitch(old_sw.getId());
	// }
	// if (old_sw == null || !old_sw.isConnected() || new_sw == null
	// || !new_sw.isConnected()) {
	// assert (old_sw == null || old_sw == new_sw);
	// log.warn("Switch disconnected?");
	// result[0] = new OFDisconnectedSwitch(
	// (IOFSwitchManager) switchService, old_sw.getId());
	// }
	// return result;
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// return null;
	// }
	// }

	// @Override
	// public IPipelineEvent getNextPortStatusToDeliver(IOFSwitch sw) {
	// try {
	// IPipelineEvent result = portStatusQueueBySwitch
	// .get(getSwitchId(sw)).take();
	//
	// IOFSwitch old_sw = (IOFSwitch) result[0];
	// IOFSwitch new_sw = null;
	// if (old_sw != null) {
	// new_sw = switchService.getSwitch(old_sw.getId());
	// }
	// if (old_sw == null || !old_sw.isConnected() || new_sw == null
	// || !new_sw.isConnected()) {
	// assert (old_sw == new_sw);
	// log.warn("Switch disconnected?");
	// result[0] = new OFDisconnectedSwitch(
	// (IOFSwitchManager) switchService, old_sw.getId());
	// }
	// return result;
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// return null;
	// }
	// }

	/**
	 * private void deliverPortStatus(@Nonnull IOFSwitch sw, OFPortStatus
	 * message, long eId, boolean processed, FloodlightContext cntx) { if (sw
	 * instanceof OFDisconnectedSwitch) {
	 * deliverPortStatusToDisconnectedSwitch(sw, eId); } else {
	 * OFSwitchHandshakeHandler swh = switchService
	 * .getSwitchHandshakeHandler(sw.getId()); if (swh == null) {
	 * deliverPortStatusToDisconnectedSwitch(sw, eId); } else { // switch is
	 * connected, use handler swh.processOFMessage(message); } } }
	 */

	// private void deliverPortStatusByDisconnectedSwitch(IOFSwitch sw, long
	// eId) {
	// processReceivedCommitReply(eId, zk.getSwitchId(sw));
	// }

	// private void notifyNewEvents() {
	// eventsLock.lock();
	// hasEvents.signal();
	// eventsLock.unlock();
	// }

}
