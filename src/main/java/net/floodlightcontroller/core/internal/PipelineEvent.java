package net.floodlightcontroller.core.internal;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFSwitch;

import org.projectfloodlight.openflow.protocol.OFMessage;

public class PipelineEvent implements IPipelineEvent {

	private final FloodlightContext cntx;
	private OFMessage msg;
	private IOFSwitch sw;

	public PipelineEvent(OFMessage msg, IOFSwitch sw, FloodlightContext cntx) {
		this.msg = msg;
		this.sw = sw;
		this.cntx = cntx;
	}

	@Override
	public final FloodlightContext getContext() {
		return cntx;
	}

	@Override
	public final OFMessage getMessage() {
		return msg;
	}

	@Override
	public final IOFSwitch getSwitch() {
		return sw;
	}
}