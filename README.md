# Software Defined Networking (SDN)
In this project I implemented two control applications for a software defined network (SDN). 

* A layer-3 routing application that installs rules in SDN switches to forward traffic to hosts using the shortest, valid path through the network. 
* A distributed load balancer application that redirects new TCP connections to hosts in a round-robin order.

This application runs atop the Floodlight OpenFlow controller.

## Layer-3 routing application

* The code for the layer-3 routing application resides in L3Routing.java in the edu.wisc.cs.sdn.apps.l3routing package.
* Bellman-Ford algorithm was used to compute the shortest paths to reach a host h from every other host h’ ∈ H, h ≠ h’ (H  is the set of all hosts).
* There are two link objects between pairs of switches, one in each direction. Due to the way links are discovered, there may be a short period of time (tens of milliseconds) where the controller has a link object only in one direction.
* When a host joins the network, both the deviceAdded(...) and linkDiscoveryUpdate(...) event handlers will be called. There are no guarantees on which order these event handlers are called.  Thus, a host may be added but we may not yet know which switch it is linked to. 
* The isAttachedToSwitch() method in the Host class will return true if we know the switch to which a host is connected, otherwise it will return false. 
* The following is assumed to hold true in the network:
	* The network is a connected graph.  In other words, there will always be at least one possible path between every pair of switches.
	* There is only one physical link between a pair of switches.
	* Links are undirected. However, be aware that Floodlight maintains a Link object for each direction (i.e., there are two Link objects for each physical link).
