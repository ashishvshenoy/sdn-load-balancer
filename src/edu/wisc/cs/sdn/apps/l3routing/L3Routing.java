package edu.wisc.cs.sdn.apps.l3routing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMatchField;
import org.openflow.protocol.OFOXMFieldType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;
import org.openflow.protocol.instruction.OFInstructionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.wisc.cs.sdn.apps.util.Host;
import edu.wisc.cs.sdn.apps.util.SwitchCommands;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.Link;

public class L3Routing implements IFloodlightModule, IOFSwitchListener, 
		ILinkDiscoveryListener, IDeviceListener
{
	public static final String MODULE_NAME = L3Routing.class.getSimpleName();
	
	// Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;

    // Interface to link discovery service
    private ILinkDiscoveryService linkDiscProv;

    // Interface to device manager service
    private IDeviceService deviceProv;
    
    // Switch table in which rules should be installed
    public static byte table;
    
    // Map of hosts to devices
    private Map<IDevice,Host> knownHosts;
	
    private Graph graph;
    
    private HashMap<ArrayList<IOFSwitch>, IOFSwitch> shortestPaths;
    
    public boolean DEBUG = false;
    
	/**
     * Loads dependencies and initializes data structures.
     */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		Map<String,String> config = context.getConfigParams(this);
        table = Byte.parseByte(config.get("table"));
        
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
        this.linkDiscProv = context.getServiceImpl(ILinkDiscoveryService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        
        this.knownHosts = new ConcurrentHashMap<IDevice,Host>();
	}

	public HashMap<ArrayList<IOFSwitch>, IOFSwitch> computeShortestPaths(){
		
		HashMap<ArrayList<IOFSwitch>, IOFSwitch> allPairsSuccesors = new HashMap<ArrayList<IOFSwitch>, IOFSwitch>();

		for(IOFSwitch srcSw : getSwitches().values()){
			HashMap<IOFSwitch, Integer> distances = new HashMap<IOFSwitch, Integer>();
			HashMap<IOFSwitch, IOFSwitch> predecessors = new HashMap<IOFSwitch, IOFSwitch>();
			
			for(IOFSwitch dstSw : getSwitches().values()){
				distances.put(dstSw, Integer.MAX_VALUE - 1);
				predecessors.put(dstSw, null);
			}
			
			distances.put(srcSw, 0);
			
			for(IOFSwitch dstSw : getSwitches().values()){
				if(dstSw == srcSw)
					continue;
				
				for(Link link: getLinks()){
					IOFSwitch sw1 = getSwitches().get(link.getSrc());
					IOFSwitch sw2 = getSwitches().get(link.getDst());
					if(distances.get(sw1) + 1 < distances.get(sw2)){
						distances.put(sw2, distances.get(sw1) + 1);
						predecessors.put(sw2, sw1);
					}

				}
			}
			for(IOFSwitch dstSw : getSwitches().values()){
				ArrayList<IOFSwitch> swTuple = new ArrayList<IOFSwitch>();
				swTuple.add(dstSw);
				swTuple.add(srcSw);
				allPairsSuccesors.put(swTuple, predecessors.get(dstSw));
			}
			
		}
		return allPairsSuccesors;
	}
	
	public int getConnectedPort(IOFSwitch sw1, IOFSwitch sw2){
		for(Link link : getLinks()){
			if((sw1.getId() == link.getSrc()) &&
					(sw2.getId() == link.getDst())){
				return link.getSrcPort();
			}

			if((sw2.getId() == link.getSrc()) &&
					(sw1.getId() == link.getDst())){
				return link.getDstPort();
			}
		}
		
		return 0;
	}
	
	public void installRulesHost(Host host){
		if(host.isAttachedToSwitch()){
			IOFSwitch connectedSwitch = host.getSwitch();
			
			OFMatchField field1 = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_IPv4);
			OFMatchField field2 = new OFMatchField(OFOXMFieldType.IPV4_DST, host.getIPv4Address());
			ArrayList<OFMatchField> matchFields = new ArrayList<OFMatchField>();
			matchFields.add(field1);
			matchFields.add(field2);
			
			OFMatch ofMatch = new OFMatch();
			ofMatch.setMatchFields(matchFields);
			
			if(DEBUG){
				System.out.println("***installing rules for with Host IP address: " + IPv4.fromIPv4Address(host.getIPv4Address()) + "\tConnected to switch " + connectedSwitch.getId());
				System.out.println();
			}
			
			for(IOFSwitch sw : getSwitches().values()){
				ArrayList<IOFSwitch> switchTuple = new ArrayList<IOFSwitch>();
				switchTuple.add(sw);
				switchTuple.add(connectedSwitch);
				
				OFActionOutput ofActionOutput = new OFActionOutput();

				if(sw.getId() != connectedSwitch.getId()){
					IOFSwitch nextSwitch = shortestPaths.get(switchTuple);
					ofActionOutput.setPort(getConnectedPort(sw, nextSwitch));
					if(DEBUG){
						System.out.println("***Host " + host.getName() + "\tInstalling for switch " + sw.getId() + "\tNext switch in path " + nextSwitch.getId());
					}
				}
				else{
					ofActionOutput.setPort(host.getPort());
				}
				
				ArrayList<OFAction> ofActions = new ArrayList <OFAction>();
				ofActions.add(ofActionOutput);
				
				OFInstructionApplyActions applyActions = new OFInstructionApplyActions(ofActions);
				ArrayList<OFInstruction> listOfInstructions = new ArrayList<OFInstruction>();
				listOfInstructions.add(applyActions);
				
				SwitchCommands.installRule(sw, table, SwitchCommands.DEFAULT_PRIORITY, ofMatch, listOfInstructions);
			}
		}
	}
	
	public void removeRulesHost(Host host){
		OFMatchField field1 = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_IPv4);
		OFMatchField field2 = new OFMatchField(OFOXMFieldType.IPV4_DST, host.getIPv4Address());
		ArrayList<OFMatchField> matchFields = new ArrayList<OFMatchField>();
		matchFields.add(field1);
		matchFields.add(field2);
		
		OFMatch ofMatch = new OFMatch();
		ofMatch.setMatchFields(matchFields);
		
		for(IOFSwitch sw : getSwitches().values()){
			SwitchCommands.removeRules(sw, table, ofMatch);
		}
	}
	
	public void installRulesAll(){
		for(Host host : getHosts()){
			installRulesHost(host);
		}
		
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
		this.linkDiscProv.addListener(this);
		this.deviceProv.addListener(this);
		
		/*********************************************************************/
		/* TODO: Initialize variables or perform startup tasks, if necessary */
		/*********************************************************************/
	}
	
    /**
     * Get a list of all known hosts in the network.
     */
    private Collection<Host> getHosts()
    { return this.knownHosts.values(); }
	
    /**
     * Get a map of all active switches in the network. Switch DPID is used as
     * the key.
     */
	private Map<Long, IOFSwitch> getSwitches()
    { return floodlightProv.getAllSwitchMap(); }
	
    /**
     * Get a list of all active links in the network.
     */
    private Collection<Link> getLinks()
    { return linkDiscProv.getLinks().keySet(); }

    /**
     * Event handler called when a host joins the network.
     * @param device information about the host
     */
	@Override
	public void deviceAdded(IDevice device) 
	{
		Host host = new Host(device, this.floodlightProv);
		// We only care about a new host if we know its IP
		if (host.getIPv4Address() != null)
		{
			log.info(String.format("Host %s added", host.getName()));
			this.knownHosts.put(device, host);
			
			/*****************************************************************/
			/* TODO: Update routing: add rules to route to new host          */
			
			/*****************************************************************/
			if(DEBUG)
				System.out.println("***Device Added : "+host.getName());
			installRulesHost(host);
		}
	}

	/**
     * Event handler called when a host is no longer attached to a switch.
     * @param device information about the host
     */
	@Override
	public void deviceRemoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{ return; }
		this.knownHosts.remove(device);
		
		log.info(String.format("Host %s is no longer attached to a switch", 
				host.getName()));
		
		/*********************************************************************/
		/* TODO: Update routing: remove rules to route to host               */
		
		/*********************************************************************/
		removeRulesHost(host);
	}

	/**
     * Event handler called when a host moves within the network.
     * @param device information about the host
     */
	@Override
	public void deviceMoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{
			host = new Host(device, this.floodlightProv);
			this.knownHosts.put(device, host);
		}
		
		if (!host.isAttachedToSwitch())
		{
			this.deviceRemoved(device);
			return;
		}
		log.info(String.format("Host %s moved to s%d:%d", host.getName(),
				host.getSwitch().getId(), host.getPort()));
		
		/*********************************************************************/
		/* TODO: Update routing: change rules to route to host               */
		
		/*********************************************************************/
		if(DEBUG)
			System.out.println("***Device Moved : "+host.getName());
		removeRulesHost(host);
		installRulesHost(host);
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
		/* TODO: Update routing: change routing rules for all hosts          */
		/*********************************************************************/
		
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d removed", switchId));
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		
		/*********************************************************************/
	}

	/**
	 * Event handler called when multiple links go up or down.
	 * @param updateList information about the change in each link's state
	 */
	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) 
	{
		for (LDUpdate update : updateList)
		{
			// If we only know the switch & port for one end of the link, then
			// the link must be from a switch to a host
			if (0 == update.getDst())
			{
				log.info(String.format("Link s%s:%d -> host updated", 
					update.getSrc(), update.getSrcPort()));
			}
			// Otherwise, the link is between two switches
			else
			{
				log.info(String.format("Link s%s:%d -> s%s:%d updated", 
					update.getSrc(), update.getSrcPort(),
					update.getDst(), update.getDstPort()));
				
			}
		}
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		
		/*********************************************************************/
		if(DEBUG)
			System.out.println("***Computing the shortest paths after link discovery");
		shortestPaths = computeShortestPaths();
		if(DEBUG)
			System.out.println("***Installing rules");
		installRulesAll();
	}

	/**
	 * Event handler called when link goes up or down.
	 * @param update information about the change in link state
	 */
	@Override
	public void linkDiscoveryUpdate(LDUpdate update) 
	{ this.linkDiscoveryUpdate(Arrays.asList(update)); }
	
	/**
     * Event handler called when the IP address of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceIPV4AddrChanged(IDevice device) 
	{ this.deviceAdded(device); }

	/**
     * Event handler called when the VLAN of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceVlanChanged(IDevice device) 
	{ /* Nothing we need to do, since we're not using VLANs */ }
	
	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId) 
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }
	
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
	{ /* Nothing we need to do, since we'll get a linkDiscoveryUpdate event */ }

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return this.MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) 
	{ return false; }

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) 
	{ return false; }
	
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
        floodlightService.add(ILinkDiscoveryService.class);
        floodlightService.add(IDeviceService.class);
        return floodlightService;
	}
}
