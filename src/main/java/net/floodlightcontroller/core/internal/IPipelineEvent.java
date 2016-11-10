package net.floodlightcontroller.core.internal;

import org.projectfloodlight.openflow.protocol.OFMessage;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFSwitch;

public interface IPipelineEvent {

	OFMessage getMessage();

	IOFSwitch getSwitch();

	FloodlightContext getContext();

}