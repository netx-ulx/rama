package net.floodlightcontroller.core.rama;

import net.floodlightcontroller.core.FloodlightContext;

public class ContextKeys {

	/**
	 * Value must be a boolean
	 */
	static final String STOP_BUNDLE = ContextKeys.class.getName()
			+ "_StopBundle";

	/**
	 * The key to get a boolean saying if the event was delivered to all modules
	 */
	static final String EVENT_DELIVERED = ContextKeys.class.getName()
			+ "_EventDelivered";

	/**
	 * Gets the event id stored in the FloodlightContext using the key
	 * ContextKeys.EVENT_ID
	 * 
	 * @param cntx
	 * 
	 * @return the event id in the FloodlightContext or -1 if not present
	 * 
	 * @throws ClassCastException
	 * 
	 * @requires cntx != null && cntx.getStorage() != null
	 */
	public static long getEventId(FloodlightContext cntx)
			throws ClassCastException {
		return cntx != null ? cntx.getId() : -1;
	}
}
