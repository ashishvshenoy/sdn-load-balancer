package edu.wisc.cs.sdn.apps.loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMatchField;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFOXMFieldType;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.action.OFActionSetField;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;
import org.openflow.protocol.instruction.OFInstructionGotoTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.wisc.cs.sdn.apps.l3routing.L3Routing;
import edu.wisc.cs.sdn.apps.util.ArpServer;
import edu.wisc.cs.sdn.apps.util.SwitchCommands;
import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
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
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.util.MACAddress;

public class LoadBalancer implements IFloodlightModule, IOFSwitchListener,
		IOFMessageListener
{
	public static final String MODULE_NAME = LoadBalancer.class.getSimpleName();
	
	private static final byte TCP_FLAG_SYN = 0x02;
	
	private static final short IDLE_TIMEOUT = 20;
	
    public boolean DEBUG = false;

    // Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;
    
    // Interface to device manager service
    private IDeviceService deviceProv;
    
    // Switch table in which rules should be installed
    private byte table;
    
    // Set of virtual IPs and the load balancer instances they correspond with
    private Map<Integer,LoadBalancerInstance> instances;

    /**
     * Loads dependencies and initializes data structures.
     */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		
		// Obtain table number from config
		Map<String,String> config = context.getConfigParams(this);
        this.table = Byte.parseByte(config.get("table"));
        
        // Create instances from config
        this.instances = new HashMap<Integer,LoadBalancerInstance>();
        String[] instanceConfigs = config.get("instances").split(";");
        for (String instanceConfig : instanceConfigs)
        {
        	String[] configItems = instanceConfig.split(" ");
        	if (configItems.length != 3)
        	{ 
        		log.error("Ignoring bad instance config: " + instanceConfig);
        		continue;
        	}
        	LoadBalancerInstance instance = new LoadBalancerInstance(
        			configItems[0], configItems[1], configItems[2].split(","));
            this.instances.put(instance.getVirtualIP(), instance);
            log.info("Added load balancer instance: " + instance);
        }
        
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        
        /*********************************************************************/
        /* TODO: Initialize other class variables, if necessary              */
        
        /*********************************************************************/
	}

	/**
     * Subscribes to events and performs other startup tasks.
     */
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Starting %s...", MODULE_NAME));
		this.floodlightProv.addOFSwitchListener(this);
		this.floodlightProv.addOFMessageListener(OFType.PACKET_IN, this);
		
		/*********************************************************************/
		/* TODO: Perform other tasks, if necessary                           */
		
		/*********************************************************************/
	}
	
	/**
     * Event handler called when a switch joins the network.
     * @param DPID for the switch
     */
	@Override
	public void switchAdded(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d added", switchId));
		
		/*********************************************************************/
		/* TODO: Install rules to send:                                      */
		/*       (1) packets from new connections to each virtual load       */
		/*       balancer IP to the controller                               */
		/*       (2) ARP packets to the controller, and                      */
		/*       (3) all other packets to the next rule table in the switch  */
		
		/*********************************************************************/
		// packets from new connections to each virtual load balancer IP to the controller 
		for(int virtualIP : instances.keySet()){
			OFMatchField fieldEthTypeIP = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_IPv4);
			OFMatchField fieldIP = new OFMatchField(OFOXMFieldType.IPV4_DST, virtualIP);
			
			ArrayList<OFMatchField> matchFieldsIPPackets = new ArrayList<OFMatchField>();
			matchFieldsIPPackets.add(fieldEthTypeIP);
			matchFieldsIPPackets.add(fieldIP);
			
			OFMatch ofMatchIP = new OFMatch();
			ofMatchIP.setMatchFields(matchFieldsIPPackets);
			
			OFActionOutput ofActionOutput = new OFActionOutput();
			ofActionOutput.setPort(OFPort.OFPP_CONTROLLER);
	
			ArrayList<OFAction> ofActions = new ArrayList <OFAction>();
			ofActions.add(ofActionOutput);
			
			OFInstructionApplyActions applyActions = new OFInstructionApplyActions(ofActions);
			ArrayList<OFInstruction> listOfInstructions = new ArrayList<OFInstruction>();
			listOfInstructions.add(applyActions);

			SwitchCommands.installRule(sw, table, SwitchCommands.DEFAULT_PRIORITY, ofMatchIP, listOfInstructions);
		}

		// ARP packets to the controller
		for(int virtualIP : instances.keySet()){
			OFMatchField fieldEthTypeARP = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_ARP);
			OFMatchField fieldARPsIP = new OFMatchField(OFOXMFieldType.ARP_TPA, virtualIP);
			
			ArrayList<OFMatchField> matchFieldsARPPackets = new ArrayList<OFMatchField>();
			matchFieldsARPPackets.add(fieldEthTypeARP);
			matchFieldsARPPackets.add(fieldARPsIP);

			OFMatch ofMatchARP = new OFMatch();
			ofMatchARP.setMatchFields(matchFieldsARPPackets);
			
			OFActionOutput ofActionOutput = new OFActionOutput();
			ofActionOutput.setPort(OFPort.OFPP_CONTROLLER);
	
			ArrayList<OFAction> ofActions = new ArrayList <OFAction>();
			ofActions.add(ofActionOutput);
			
			OFInstructionApplyActions applyActions = new OFInstructionApplyActions(ofActions);
			ArrayList<OFInstruction> listOfInstructions = new ArrayList<OFInstruction>();
			listOfInstructions.add(applyActions);

			SwitchCommands.installRule(sw, table, SwitchCommands.DEFAULT_PRIORITY, ofMatchARP, listOfInstructions);
		}

		// all other packets to the next rule table in the switch
		{
			OFMatch ofMatchDefault = new OFMatch();
			
			OFInstructionGotoTable ofInstructionGotoTable = new OFInstructionGotoTable();
			ofInstructionGotoTable.setTableId(L3Routing.table);
			
			ArrayList<OFInstruction> listOfInstructions = new ArrayList<OFInstruction>();
			listOfInstructions.add(ofInstructionGotoTable);
			
			SwitchCommands.installRule(sw, table, (short)(SwitchCommands.DEFAULT_PRIORITY - 1), ofMatchDefault, listOfInstructions);
		}
	}
	
	/**
	 * Handle incoming packets sent from switches.
	 * @param sw switch on which the packet was received
	 * @param msg message from the switch
	 * @param cntx the Floodlight context in which the message should be handled
	 * @return indication whether another module should also process the packet
	 */
	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) 
	{
		// We're only interested in packet-in messages
		if (msg.getType() != OFType.PACKET_IN)
		{ return Command.CONTINUE; }
		OFPacketIn pktIn = (OFPacketIn)msg;
		
		// Handle the packet
		Ethernet ethPkt = new Ethernet();
		ethPkt.deserialize(pktIn.getPacketData(), 0,
				pktIn.getPacketData().length);
		
		/*********************************************************************/
		/* TODO: Send an ARP reply for ARP requests for virtual IPs; for TCP */
		/*       SYNs sent to a virtual IP, select a host and install        */
		/*       connection-specific rules to rewrite IP and MAC addresses;  */
		/*       ignore all other packets                                    */
		
		/*********************************************************************/
		if(ethPkt.getEtherType() == Ethernet.TYPE_IPv4){
			IPv4 ipv4Pkt = (IPv4)ethPkt.getPayload();
			if(ipv4Pkt.getProtocol() == IPv4.PROTOCOL_TCP){
				TCP tcpPkt = (TCP) ipv4Pkt.getPayload();
				
				if((tcpPkt.getFlags() & TCP_FLAG_SYN) != 0){
					int virtualIP = ipv4Pkt.getDestinationAddress();
					int srcIP = ipv4Pkt.getSourceAddress();
					
					if(!isVirtualIP(virtualIP)){
						return Command.CONTINUE;
					}
					
					int srcPort = tcpPkt.getSourcePort();
					int dstPort = tcpPkt.getDestinationPort();
					
					int hostIP = instances.get(virtualIP).getNextHostIP();
					byte[] hostMAC = getHostMACAddress(hostIP);
					
					if(DEBUG){
						System.out.println("***TCP SYN recieved for virtual IP " + IPv4.fromIPv4Address(virtualIP));
						System.out.println("Assigned host with IP " + IPv4.fromIPv4Address(hostIP));
					}
					
					{
						OFMatchField fieldEthTypeIP = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_IPv4);
						OFMatchField fieldSrcIP = new OFMatchField(OFOXMFieldType.IPV4_SRC, srcIP);
						OFMatchField fieldDstIP = new OFMatchField(OFOXMFieldType.IPV4_DST, virtualIP);
						OFMatchField fieldProto = new OFMatchField(OFOXMFieldType.IP_PROTO, IPv4.PROTOCOL_TCP);
						OFMatchField fieldSrcTCP = new OFMatchField(OFOXMFieldType.TCP_SRC, srcPort);
						OFMatchField fieldDstTCP = new OFMatchField(OFOXMFieldType.TCP_DST, dstPort);
						
						ArrayList<OFMatchField> matchFieldsIPPackets = new ArrayList<OFMatchField>();
						matchFieldsIPPackets.add(fieldEthTypeIP);
						matchFieldsIPPackets.add(fieldSrcIP);
						matchFieldsIPPackets.add(fieldDstIP);
						matchFieldsIPPackets.add(fieldProto);
						matchFieldsIPPackets.add(fieldSrcTCP);
						matchFieldsIPPackets.add(fieldDstTCP);
						
						OFMatch ofMatchIP = new OFMatch();
						ofMatchIP.setMatchFields(matchFieldsIPPackets);
						
						ArrayList<OFAction> ofActions = new ArrayList <OFAction>();
						ofActions.add(new OFActionSetField(OFOXMFieldType.ETH_DST, hostMAC));
						ofActions.add(new OFActionSetField(OFOXMFieldType.IPV4_DST, hostIP));
						OFInstructionApplyActions applyActions = new OFInstructionApplyActions(ofActions);
						
						OFInstructionGotoTable ofInstructionGotoTable = new OFInstructionGotoTable();
						ofInstructionGotoTable.setTableId(L3Routing.table);
						
						ArrayList<OFInstruction> listOfInstructions = new ArrayList<OFInstruction>();
						listOfInstructions.add(applyActions);
						listOfInstructions.add(ofInstructionGotoTable);
	
						SwitchCommands.installRule(sw, table, (short)(SwitchCommands.DEFAULT_PRIORITY + 1), ofMatchIP, listOfInstructions,
								SwitchCommands.NO_TIMEOUT, IDLE_TIMEOUT);
					}
					{
						OFMatchField fieldEthTypeIP = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_IPv4);
						OFMatchField fieldSrcIP = new OFMatchField(OFOXMFieldType.IPV4_SRC, hostIP);
						OFMatchField fieldDstIP = new OFMatchField(OFOXMFieldType.IPV4_DST, srcIP);
						OFMatchField fieldProto = new OFMatchField(OFOXMFieldType.IP_PROTO, IPv4.PROTOCOL_TCP);
						OFMatchField fieldSrcTCP = new OFMatchField(OFOXMFieldType.TCP_SRC, dstPort);
						OFMatchField fieldDstTCP = new OFMatchField(OFOXMFieldType.TCP_DST, srcPort);
						
						ArrayList<OFMatchField> matchFieldsIPPackets = new ArrayList<OFMatchField>();
						matchFieldsIPPackets.add(fieldEthTypeIP);
						matchFieldsIPPackets.add(fieldSrcIP);
						matchFieldsIPPackets.add(fieldDstIP);
						matchFieldsIPPackets.add(fieldProto);
						matchFieldsIPPackets.add(fieldSrcTCP);
						matchFieldsIPPackets.add(fieldDstTCP);
						
						OFMatch ofMatchIP = new OFMatch();
						ofMatchIP.setMatchFields(matchFieldsIPPackets);
						
						ArrayList<OFAction> ofActions = new ArrayList <OFAction>();
						ofActions.add(new OFActionSetField(OFOXMFieldType.ETH_SRC, instances.get(virtualIP).getVirtualMAC()));
						ofActions.add(new OFActionSetField(OFOXMFieldType.IPV4_SRC, virtualIP));
						OFInstructionApplyActions applyActions = new OFInstructionApplyActions(ofActions);

						OFInstructionGotoTable ofInstructionGotoTable = new OFInstructionGotoTable();
						ofInstructionGotoTable.setTableId(L3Routing.table);

						ArrayList<OFInstruction> listOfInstructions = new ArrayList<OFInstruction>();
						listOfInstructions.add(applyActions);
						listOfInstructions.add(ofInstructionGotoTable);
	
						SwitchCommands.installRule(sw, table, (short)(SwitchCommands.DEFAULT_PRIORITY + 1), ofMatchIP, listOfInstructions,
								SwitchCommands.NO_TIMEOUT, IDLE_TIMEOUT);
					}
				}
			}
		}
		if(ethPkt.getEtherType() == Ethernet.TYPE_ARP){
			ARP arpPkt = (ARP) ethPkt.getPayload();
			int virtualIP = IPv4.toIPv4Address(arpPkt.getTargetProtocolAddress());

			if((arpPkt.getOpCode() == ARP.OP_REQUEST) && isVirtualIP(virtualIP)){

				if(DEBUG){
					System.out.println("***ARP recieved for virtual IP " + IPv4.fromIPv4Address(virtualIP));
				}

				byte[] virtualMAC = instances.get(virtualIP).getVirtualMAC();
	
				Ethernet etherSendPacket = new Ethernet();
				ARP arpSendPacket = new ARP();
				
				etherSendPacket.setEtherType(Ethernet.TYPE_ARP);
				etherSendPacket.setSourceMACAddress(virtualMAC);
				etherSendPacket.setDestinationMACAddress(ethPkt.getSourceMACAddress());
				
				arpSendPacket.setHardwareType(ARP.HW_TYPE_ETHERNET);
				arpSendPacket.setProtocolType(ARP.PROTO_TYPE_IP);
				arpSendPacket.setHardwareAddressLength((byte)Ethernet.DATALAYER_ADDRESS_LENGTH);
				arpSendPacket.setProtocolAddressLength((byte) 4);
				arpSendPacket.setOpCode(ARP.OP_REPLY);
				arpSendPacket.setSenderHardwareAddress(virtualMAC);
				arpSendPacket.setSenderProtocolAddress(virtualIP);
				arpSendPacket.setTargetHardwareAddress(arpPkt.getSenderHardwareAddress());
				arpSendPacket.setTargetProtocolAddress(arpPkt.getSenderProtocolAddress());
				
				etherSendPacket.setPayload(arpSendPacket);
				
				SwitchCommands.sendPacket(sw, (short)pktIn.getInPort(), etherSendPacket);
				if(DEBUG){
					System.out.println("***Sent ARP reply from swtich " + sw.getId() + " on port " + pktIn.getInPort());
				}
			}
			else{
				// Do nothing
			}
			
		}
		

		
		// We don't care about other packets
		return Command.CONTINUE;
	}
	
	/**
	 * Returns the MAC address for a host, given the host's IP address.
	 * @param hostIPAddress the host's IP address
	 * @return the hosts's MAC address, null if unknown
	 */
	private byte[] getHostMACAddress(int hostIPAddress)
	{
		Iterator<? extends IDevice> iterator = this.deviceProv.queryDevices(
				null, null, hostIPAddress, null, null);
		if (!iterator.hasNext())
		{ return null; }
		IDevice device = iterator.next();
		return MACAddress.valueOf(device.getMACAddress()).toBytes();
	}
	
	private boolean isVirtualIP(int ip){
		for(int virtualIP : instances.keySet()){
			if(virtualIP== ip){
				return true;
			}
		}
		return false;
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{ /* Nothing we need to do, since the switch is no longer active */ }

	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId)
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when a port on a switch goes up or down, or is
	 * added or removed.
	 * @param DPID for the switch
	 * @param port the port on the switch whose status changed
	 * @param type the type of status change (up, down, add, remove)
	 */
	@Override
	public void switchPortChanged(long switchId, ImmutablePort port,
			PortChangeType type) 
	{ /* Nothing we need to do, since load balancer rules are port-agnostic */}

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }
	
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
	{
		return (OFType.PACKET_IN == type 
				&& (name.equals(ArpServer.MODULE_NAME) 
					|| name.equals(DeviceManagerImpl.MODULE_NAME))); 
	}

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) 
	{ return false; }
}
