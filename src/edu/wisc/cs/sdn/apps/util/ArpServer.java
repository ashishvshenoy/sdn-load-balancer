package edu.wisc.cs.sdn.apps.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.internal.DeviceManagerImpl;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.util.MACAddress;

public class ArpServer implements IFloodlightModule, IOFMessageListener
{
	public static final String MODULE_NAME = ArpServer.class.getSimpleName();
	
	// Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
	// Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;
    
    // Interface to device manager service
    private IDeviceService deviceProv;

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));       
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
		this.deviceProv = context.getServiceImpl(IDeviceService.class);
	}

	/**
     * Subscribes to events and performs other startup tasks.
     */
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Starting %s...", MODULE_NAME));
		this.floodlightProv.addOFMessageListener(OFType.PACKET_IN, this);
	}

	/**
     * Tell the module system which services we provide.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() 
	{ return null; }

	/**
     * Tell the module system which services we implement.
     */
	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> 
			getServiceImpls() 
	{ return null; }

	/**
     * Tell the module system which modules we depend on.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> 
			getModuleDependencies() 
	{
		Collection<Class<? extends IFloodlightService >> floodlightService =
	            new ArrayList<Class<? extends IFloodlightService>>();
        floodlightService.add(IFloodlightProviderService.class);
        floodlightService.add(IDeviceService.class);
        return floodlightService;
	}

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) 
	{ return false; }

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) 
	{
		return (OFType.PACKET_IN == type 
				&& name.equals(DeviceManagerImpl.MODULE_NAME)); 
	}
	
	/**
	 * Handle incoming ARP packets.
	 */
	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) 
	{
		// We only care about packet-in messages
		if (msg.getType() != OFType.PACKET_IN)
		{ return Command.CONTINUE; }
		OFPacketIn pktIn = (OFPacketIn)msg;
		
		// We only care about ARP packets
		Ethernet eth = new Ethernet();
		eth.deserialize(pktIn.getPacketData(), 0, pktIn.getPacketData().length);
		if (eth.getEtherType() != Ethernet.TYPE_ARP)
		{ return Command.CONTINUE; }
		ARP arp = (ARP)eth.getPayload();
		
		// We only care about ARP requests for IPv4 addresses
		if (arp.getOpCode() != ARP.OP_REQUEST 
				|| arp.getProtocolType() != ARP.PROTO_TYPE_IP)
		{ return Command.CONTINUE; }
				
		// See if we known about the device whose MAC address is being requested
		int targetIP = IPv4.toIPv4Address(arp.getTargetProtocolAddress());
		log.info(String.format("Received ARP request for %s from %s",
				IPv4.fromIPv4Address(targetIP),
				MACAddress.valueOf(arp.getSenderHardwareAddress()).toString()));
		Iterator<? extends IDevice> deviceIterator = 
				this.deviceProv.queryDevices(null, null, targetIP, null, null);
		if (!deviceIterator.hasNext())
		{ return Command.CONTINUE; }
		
		// Create ARP reply
		IDevice device = deviceIterator.next();
		byte[] deviceMac = MACAddress.valueOf(device.getMACAddress()).toBytes();
		arp.setOpCode(ARP.OP_REPLY);
		arp.setTargetHardwareAddress(arp.getSenderHardwareAddress());
		arp.setTargetProtocolAddress(arp.getSenderProtocolAddress());
		arp.setSenderHardwareAddress(deviceMac);
		arp.setSenderProtocolAddress(IPv4.toIPv4AddressBytes(targetIP));
		eth.setDestinationMACAddress(eth.getSourceMACAddress());
		eth.setSourceMACAddress(deviceMac);
		
		// Send the ARP reply
		log.info(String.format("Sending ARP reply %s->%s",
				IPv4.fromIPv4Address(targetIP),
				MACAddress.valueOf(deviceMac).toString()));
		SwitchCommands.sendPacket(sw, (short)pktIn.getInPort(), eth);
	
		return Command.STOP;
	}
}
