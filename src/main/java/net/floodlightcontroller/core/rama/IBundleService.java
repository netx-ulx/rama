package net.floodlightcontroller.core.rama;

import java.util.Collection;
import java.util.List;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.types.DatapathId;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IControllerCompletionListener;
import net.floodlightcontroller.core.IOFConnection;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.module.IFloodlightService;

public interface IBundleService extends IFloodlightService,
		IControllerCompletionListener {

	//boolean onMessageConsumed(IOFSwitch sw, OFMessage m, FloodlightContext bc);

	void setCurrentEvent(long eventId);

	void openEmptyBundle(IOFSwitch sw);

	void printCurrentBundles();

	void onSwitchUpdateFinish(DatapathId swId, IOFSwitch switch1,
			OFPortDesc port, PortChangeType changeType, FloodlightContext cntx);

	/**
	 * Adds the list of messages to the current bundle for this switch in this
	 * thread (creates new bundles as required). Messages are wrapped in
	 * BundleAdd messages and sent to the switch using the specified connection.
	 * 
	 * The bundles are committed after all modules are done processing the each
	 * event.
	 * 
	 * Returns null if this bundle is already closed or committed. Returns an
	 * empty collection if all messages were sent. Otherwise returns a
	 * collection with the unsent messages (empty on success).
	 * 
	 * @param msgs
	 * @param sw
	 * @param conn
	 * @requires sw != null && conn != null &&
	 *           sw.getOFFactory().getVersion().compareTo(OFVersion.OF_14) >= 0
	 * @return
	 */
	public Collection<OFMessage> add(List<OFMessage> msgs, IOFSwitch sw,
			IOFConnection conn);
	
	public boolean addSentBundle(int bundleId);

}
