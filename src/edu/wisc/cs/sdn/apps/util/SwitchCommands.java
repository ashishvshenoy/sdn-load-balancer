package edu.wisc.cs.sdn.apps.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.instruction.OFInstruction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.packet.Ethernet;

public class SwitchCommands 
{
	public static final short NO_TIMEOUT = 0;
	public static final short DEFAULT_PRIORITY = 1;
	public static final short MIN_PRIORITY = Short.MIN_VALUE+1;
	public static final short MAX_PRIORITY = Short.MAX_VALUE-1;
	
	// Interface to the logging system
    private static Logger log =
            LoggerFactory.getLogger(SwitchCommands.class.getSimpleName());

	/**
     * Installs a rule in a switch's flow table.
     * @param sw the switch in which the rule should be installed
     * @param table the table in which the rule should be installed
     * @param priority the priority of the rule; larger values are higher 
     *         priority
     * @param matchCriteria the match criteria for the rule
     * @param instructions the actions to apply to packets matching the rule
     * @param hardTimeout the rule should be removed after hardTimeout seconds 
     *         have elapsed since the rule was installed; if 0, then the rule
     *         will never be removed
     * @param idleTimeout the rules should be removed after idleTimeout seconds
     *         have elapsed since a packet last matched the rule; if 0, then the
     *         rule will never be removed due to a lack of matching packets
     * @param bufferId apply the newly installed rule to the packet buffered
     *         in this provided slot on the switch
     * @return true if the rule was sent to the switch, otherwise false
     */
    public static boolean installRule(IOFSwitch sw, byte table, short priority,
            OFMatch matchCriteria, List<OFInstruction> instructions, 
            short hardTimeout, short idleTimeout, int bufferId)
    {
        OFFlowMod rule = new OFFlowMod();
        rule.setHardTimeout(hardTimeout);
        rule.setIdleTimeout(idleTimeout);
        rule.setPriority(priority);
        rule.setTableId(table);
        rule.setBufferId(bufferId);

        rule.setMatch(matchCriteria.clone());
        
        rule.setInstructions(instructions);
        
        int length = OFFlowMod.MINIMUM_LENGTH;
        for (OFInstruction instruction : instructions)
        { length += instruction.getLengthU(); }
        rule.setLength((short)length);

        try
        {
            sw.write(rule, null);
            sw.flush();
            log.debug("Installing rule: "+rule);
        }
        catch (IOException e)
        {
            log.error("Failed to install rule: "+rule);
            return false;
        }

        return true;
    }
    
    /**
     * Installs a rule in a switch's flow table.
     * @param sw the switch in which the rule should be installed
     * @param table the table in which the rule should be installed
     * @param priority the priority of the rule; larger values are higher 
     *         priority
     * @param matchCriteria the match criteria for the rule
     * @param instructions the actions to apply to packets matching the rule
     * @param hardTimeout the rule should be removed after hardTimeout seconds 
     *         have elapsed since the rule was installed; if 0, then the rule
     *         will never be removed
     * @param idleTimeout the rules should be removed after idleTimeout seconds
     *         have elapsed since a packet last matched the rule; if 0, then the
     *         rule will never be removed due to a lack of matching packets
     * @return true if the rule was sent to the switch, otherwise false
     */
    public static boolean installRule(IOFSwitch sw, byte table, short priority,
            OFMatch matchCriteria, List<OFInstruction> instructions, 
            short hardTimeout, short idleTimeout)
    {
    	return installRule(sw, table, priority, matchCriteria, instructions, 
    			hardTimeout, idleTimeout, OFPacketOut.BUFFER_ID_NONE);
    }
    
    /**
     * Installs a rule with no timeout in a switch's flow table.
     * @param sw the switch in which the rule should be installed
     * @param table the table in which the rule should be installed
     * @param priority the priority of the rule; larger values are higher 
     *         priority
     * @param matchCriteria the match criteria for the rule
     * @param instructions the actions to apply to packets matching the rule
     * @return true if the rule was sent to the switch, otherwise false
     */
    public static boolean installRule(IOFSwitch sw, byte table, short priority,
            OFMatch matchCriteria, List<OFInstruction> instructions)
    {
    	return installRule(sw, table, priority, matchCriteria, instructions, 
    			NO_TIMEOUT, NO_TIMEOUT);
    }
    
    /**
     * Remove a rule from a switch's flow table.
     * @param sw the switch from which the rule should be removed
     * @param table the table from which the rule should be removed
     * @param matchCriteria match criteria specifying the rules to delete
     * @return true if the delete was sent to the switch, otherwise false
     */
    public static boolean removeRules(IOFSwitch sw, byte table, 
    		OFMatch matchCriteria)
    {
        OFFlowMod rule = new OFFlowMod();
        rule.setCommand(OFFlowMod.OFPFC_DELETE);
        rule.setTableId(table);

        rule.setMatch(matchCriteria.clone());
        rule.setLength((short)OFFlowMod.MINIMUM_LENGTH);

        try
        {
            sw.write(rule, null);
            sw.flush();
            log.debug("Removing rule: "+rule);
        }
        catch (IOException e)
        {
            log.error("Failed to remove rule: "+rule);
            return false;
        }

        return true;
    }
    
	/**
	 * Sends a packet out of a switch.
	 * @param outSw the switch out which the packet should be forwarded
	 * @param outPort the switch port out which the packet should be forwarded
	 * @param eth the Ethernet packet to forward 
	 * @return true if the packet was sent to the switch, otherwise false
	 */
	public static boolean sendPacket(IOFSwitch outSw, short outPort, 
			Ethernet eth) 
    {
		// Create an OFPacketOut for the packet
        OFPacketOut pktOut = new OFPacketOut();        
        
        // Update the buffer ID
        pktOut.setBufferId(OFPacketOut.BUFFER_ID_NONE);
                
        // Set the actions to apply for this packet
        OFAction output = new OFActionOutput(outPort);
        pktOut.setActions(Arrays.asList(output));
        pktOut.setActionsLength((short)OFActionOutput.MINIMUM_LENGTH);
	        
        // Set packet data
        byte[] packetData = eth.serialize();
        pktOut.setPacketData(packetData);
        pktOut.setLength((short)(OFPacketOut.MINIMUM_LENGTH
                + pktOut.getActionsLength() + packetData.length));
        
        // Send the packet to the switch
        try 
        {
            outSw.write(pktOut, null);
            outSw.flush();
            log.info("Forwarding packet: "+eth.toString());
        }
        catch (IOException e) 
        {
        	log.error("Failed to forward packet: "+eth.toString());
			return false;
        }
        
        return true;
	}
}
