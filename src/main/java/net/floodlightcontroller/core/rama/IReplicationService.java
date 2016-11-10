package net.floodlightcontroller.core.rama;

import java.util.concurrent.BlockingQueue;

import org.projectfloodlight.openflow.protocol.OFMessage;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IControllerCompletionListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IPipelineEvent;
import net.floodlightcontroller.core.module.IFloodlightService;

public interface IReplicationService extends IFloodlightService,
		IControllerCompletionListener {

	/**
	 * 
	 * @param sw
	 * @param msg
	 * @return true if this event was replicated and should be delivered now or
	 *         in the future
	 */
	public boolean receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx);

	/**
	 * 
	 * @param sw
	 * @param m
	 * @param bc
	 */
	//public void onMessageConsumed(IOFSwitch sw, OFMessage m, FloodlightContext bc);

	public void onSwitchUpdateFinish(IOFSwitch sw, FloodlightContext cntx);

	/**
	 * 
	 * @param sw
	 * @param bc
	 */
	public void onMessageConsumed(IOFSwitch sw, FloodlightContext bc);

	/**
	 * TODO Returns an Object[3] with:
	 * 
	 * [0] -> IOFSwitch sw
	 * 
	 * [1] -> OFMessage msg
	 * 
	 * [2] -> FloodlightContext cntx
	 * 
	 * Of the next <sw, msg, cntx> to be delivered to the floodlight pipeline.
	 * 
	 * Note that this method blocks until one event is received by the
	 * IReplicationService implementation module.
	 * 
	 * @return
	 */
	// public IPipelineEvent getNextToDeliver();

	// IPipelineEvent getNextPortStatusToDeliver(IOFSwitch sw);

	public boolean isMaster(String swId);

	public boolean isConnected(String swId);

	public boolean addAffectedSwitch(long eventId, String swId);

	public void storeCommit(int bundleId, long eventId, String swId);

	public void processReceivedCommitReply(long id, String sw);

	/**
	 * Used by ZKManager to deliver events received from the master (we are
	 * slaves)
	 * 
	 * @param eventId
	 * @param swId
	 * @param m
	 */
	void sendToDeliverQueue(long eventId, String swId, OFMessage m);

	public void setPipelineQueue(BlockingQueue<IPipelineEvent> pipelineQueue);

}
