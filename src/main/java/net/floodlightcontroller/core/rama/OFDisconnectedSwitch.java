package net.floodlightcontroller.core.rama;

import java.util.Collection;
import java.util.Collections;

import net.floodlightcontroller.core.LogicalOFMessageCategory;
import net.floodlightcontroller.core.internal.IOFSwitchManager;
import net.floodlightcontroller.core.internal.OFSwitch;

import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFAuxId;

public class OFDisconnectedSwitch extends OFSwitch {

	private static final OFFactory factory = OFFactories
			.getFactory(OFVersion.OF_14);

	public OFDisconnectedSwitch(IOFSwitchManager switchManager,
			DatapathId datapathId) {
		super(new OFDisconnectedConnection(datapathId, OFAuxId.MAIN), factory,
				switchManager, datapathId);
	}

	/**
	 * Write a single message to the switch
	 * 
	 * @param m
	 *            the message to write
	 * @return true upon success; false upon failure; failure can occur either
	 *         from sending a message not supported in the current role, or from
	 *         the channel being disconnected
	 */
	@Override
	public boolean write(OFMessage m) {
		return this.write(Collections.singletonList(m)).isEmpty();
	}

	/**
	 * Write a list of messages to the switch
	 * 
	 * @param msglist
	 *            list of messages to write
	 * @return list of failed messages; messages can fail if sending the
	 *         messages is not supported in the current role, or from the
	 *         channel becoming disconnected
	 */
	@Override
	public Collection<OFMessage> write(Iterable<OFMessage> msglist) {
		return this.write(msglist, LogicalOFMessageCategory.MAIN);
	}

	@Override
	public boolean write(OFMessage m, LogicalOFMessageCategory category) {
		return this.write(Collections.singletonList(m), category).isEmpty();
	}

	@Override
	public Collection<OFMessage> write(Iterable<OFMessage> msgList,
			LogicalOFMessageCategory category) {
		return Collections.emptyList();
	}
}