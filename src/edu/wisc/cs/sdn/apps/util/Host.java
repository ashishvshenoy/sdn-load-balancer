package edu.wisc.cs.sdn.apps.util;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.devicemanager.IDevice;

public class Host
{
	/* Meta-data about the host from Floodlight's device manager */
	private IDevice device;
	
	/* Floodlight module which is needed to lookup switches by DPID */
	private IFloodlightProviderService floodlightProv;
	
	/**
	 * Create a host.
	 * @param device meta-data about the host from Floodlight's device manager
	 * @param floodlightProv Floodlight module to lookup switches by DPID
	 */
	public Host(IDevice device, IFloodlightProviderService floodlightProv)
	{
		this.device = device;
		this.floodlightProv = floodlightProv;
	}
	
	/**
	 * Get the host's name (assuming a host's name corresponds to its MAC address).
	 * @return the host's name
	 */
	public String getName()
	{ return String.format("h%d",this.getMACAddress()); }
	
	/**
	 * Get the host's MAC address.
	 * @return the host's MAC address
	 */
	public long getMACAddress()
	{ return this.device.getMACAddress(); }
	
	/**
	 * Get the host's IPv4 address.
	 * @return the host's IPv4 address, null if unknown
	 */
	public Integer getIPv4Address()
	{
		if (null == this.device.getIPv4Addresses()
				|| 0 == this.device.getIPv4Addresses().length)
		{ return null; }
		return this.device.getIPv4Addresses()[0];
	}
	
	/**
	 * Get the switch to which the host is connected.
	 * @return the switch to which the host is connected, null if unknown
	 */
	public IOFSwitch getSwitch()
	{
		if (null == this.device.getAttachmentPoints()
				|| 0 == this.device.getAttachmentPoints().length)
		{ return null; }
		long switchDPID = this.device.getAttachmentPoints()[0].getSwitchDPID();
		return this.floodlightProv.getSwitch(switchDPID);
	}
	
	/**
	 * Get the port on the switch to which the host is connected.
	 * @return the port to which the host is connected, null if unknown
	 */
	public Integer getPort()
	{
		if (null == this.device.getAttachmentPoints()
				|| 0 == this.device.getAttachmentPoints().length)
		{ return null; }
		return this.device.getAttachmentPoints()[0].getPort();
	}
	
	/**
	 * Checks whether the host is attached to some switch.
	 * @return true if the host is attached to some switch, otherwise false
	 */
	public boolean isAttachedToSwitch()
	{ return (null != this.getSwitch()); }
	
	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof Host))
		{ return false; }
		Host other = (Host)obj;
		return other.device.equals(this.device);
	}
}