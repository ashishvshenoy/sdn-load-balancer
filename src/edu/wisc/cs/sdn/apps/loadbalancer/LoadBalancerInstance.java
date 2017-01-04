package edu.wisc.cs.sdn.apps.loadbalancer;

import java.util.ArrayList;
import java.util.List;

import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.util.MACAddress;

public class LoadBalancerInstance 
{
	// Virtual IP for this load balancer instance
	private int virtualIP;
	
	// Virtual MAC address for this load balancer instance
	private byte[] virtualMAC;
	
	// IPs for the hosts to which a flow could be sent
	private List<Integer> hostIPs;
	
	// The index for the last host to which a flow was sent;
	private int lastHost;
	
	/**
	 * Create a load balancer instance.
	 * @param virtualIP virtual IP address for the load balancer instance
	 * @param virtualMAC virtual MAC address for the load balancer instances
	 * @param hostIPs IPs for hosts to which requests should be balanced 
	 */
	public LoadBalancerInstance(int virtualIP, byte[] virtualMAC, 
			List<Integer> hostIPs)
	{
		this.virtualIP = virtualIP;
		this.virtualMAC = virtualMAC;
		this.hostIPs = hostIPs;
		this.lastHost = -1;
	}
	
	/**
	 * Create a load balancer instance.
	 * @param virtualIP virtual IP address for the load balancer instance
	 * @param virtualMAC virtual MAC address for the load balancer instances
	 * @param hostIPs IPs for hosts to which requests should be balanced 
	 */
	public LoadBalancerInstance(String virtualIP, String virtualMAC,
			String[] hostIPs)
	{
		this.virtualIP = IPv4.toIPv4Address(virtualIP);
		this.virtualMAC = MACAddress.valueOf(virtualMAC).toBytes();
		this.hostIPs = new ArrayList<Integer>();
		for (String hostIP : hostIPs)
		{ this.hostIPs.add(IPv4.toIPv4Address(hostIP)); }
		this.lastHost = -1;
	}
	
	/**
	 * Get the virtual IP address for this load balancer instance.
	 */
	public int getVirtualIP()
	{ return this.virtualIP; }
	
	/**
	 * Get the virtual MAC address for this load balancer instance.
	 */
	public byte[] getVirtualMAC()
	{ return this.virtualMAC; }
	
	/**
	 * Get the IP address for the next host in round-robin order.
	 * @return the IP address for the next host
	 */
	public int getNextHostIP()
	{
		lastHost++;
		if (lastHost >= hostIPs.size())
		{ lastHost = 0; }
		return hostIPs.get(lastHost);
	}
	
	@Override
	public String toString()
	{
		String result = IPv4.fromIPv4Address(this.virtualIP);
		result += " " + MACAddress.valueOf(this.virtualMAC).toString() + " ";
		for (Integer hostIP : this.hostIPs)
		{ result += IPv4.fromIPv4Address(hostIP) + ","; }
		if (',' == result.charAt(result.length()-1))
		{ result = result.substring(0, result.length()-1); }
		return result;
	}
}
