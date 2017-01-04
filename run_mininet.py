#!/usr/bin/env python
"""
Custom topologies for CS640, Spring 2016, Programming Assignment 4
"""

import sys
from mininet.cli import CLI
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.topo import Topo,SingleSwitchTopo,LinearTopo
from mininet.topolib import TreeTopo
from mininet.log import setLogLevel, info
from mininet.util import customConstructor,quietRun

class AssignOneTopo(Topo):
    def __init__(self, **opts):
        Topo.__init__(self, **opts)
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')
        h3 = self.addHost('h3')
        h4 = self.addHost('h4')
        h5 = self.addHost('h5')
        h6 = self.addHost('h6')
        h7 = self.addHost('h7')
        h8 = self.addHost('h8')
        h9 = self.addHost('h9')
        h10 = self.addHost('h10')
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')
        s5 = self.addSwitch('s5')
        s6 = self.addSwitch('s6')
        self.addLink(h1, s1)
        self.addLink(h7, s1)
        self.addLink(h8, s1)
        self.addLink(h2, s2)
        self.addLink(h3, s3)
        self.addLink(h4, s4)
        self.addLink(h9, s4)
        self.addLink(h10, s4)
        self.addLink(h5, s5)
        self.addLink(h6, s6)
        self.addLink(s1, s2)
        self.addLink(s2, s3)
        self.addLink(s3, s4)
        self.addLink(s2, s5)
        self.addLink(s3, s6)
 
class TriangleTopo(Topo):
    def __init__(self, **opts):
        Topo.__init__(self, **opts)
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')
        h3 = self.addHost('h3')
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        self.addLink(h1, s1)
        self.addLink(h2, s2)
        self.addLink(h3, s3)
        self.addLink(s1, s2)
        self.addLink(s2, s3)
        self.addLink(s3, s1)

class SomeLoopsTopo(Topo):
    def __init__(self, **opts):
        Topo.__init__(self, **opts)
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')
        h3 = self.addHost('h3')
        h4 = self.addHost('h4')
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')
        s5 = self.addSwitch('s5')
        s6 = self.addSwitch('s6')
        self.addLink(h1, s1)
        self.addLink(h2, s5)
        self.addLink(h3, s4)
        self.addLink(h4, s6)
        self.addLink(s1, s2)
        self.addLink(s2, s3)
        self.addLink(s3, s4)
        self.addLink(s3, s6)
        self.addLink(s2, s5)
        self.addLink(s5, s4)
        self.addLink(s4, s6)
        self.addLink(s6, s1)

class MeshTopo(Topo):
    def __init__(self, n=4, **opts):
        Topo.__init__(self, **opts)
        switches = []
        for i in range(1,n+1):
            h = self.addHost('h%d' % i)
            s = self.addSwitch('s%d' % i)
            self.addLink(h, s)
            switches.append(s)
        for i in range(0,n-1):
            for j in range(i+1,n):
                self.addLink(switches[i], switches[j])
     
def starthttp(host):
    "Start simple Python web server on hosts"
    info( '*** Starting SimpleHTTPServer on host', host, '\n' )
    command = 'mkdir /tmp/%s; cp ./webserver.py /tmp/%s; pushd /tmp/%s/; echo WEB PAGE SERVED BY: %s > index.html; nohup python2.7 ./webserver.py &' % (host.name, host.name, host.name, host.name)
#    command = 'nohup python2.7 ./webserver.py'
    host.cmd(command)

def stophttp():
    "Stop simple Python web servers"
    info( '*** Shutting down stale SimpleHTTPServers', 
          quietRun( "pkill -9 -f SimpleHTTPServer" ), '\n' )    
    info( '*** Shutting down stale webservers', 
          quietRun( "pkill -9 -f webserver.py" ), '\n' )    
   
if __name__ == '__main__':
    setLogLevel( 'info' )

    # Create network
    if (len(sys.argv) != 2):
        print 'Specify topology single, tree, linear, assign1, triangle, mesh, or someloops'
        sys.exit(1)
    topoName = sys.argv[1]
    topoParts = topoName.split(',')
    if (topoParts[0] == "single"):
        if (len(topoParts) != 2 or not topoParts[1].isdigit()):
            sys.exit(1)
        topo = SingleSwitchTopo(k=int(topoParts[1]))
    elif (topoParts[0] == "tree"):
        if (len(topoParts) != 2 or not topoParts[1].isdigit()):
            sys.exit(1)
        topo = TreeTopo(depth=int(topoParts[1]))
    elif (topoParts[0] == "linear"):
        if (len(topoParts) != 2 or not topoParts[1].isdigit()):
            sys.exit(1)
        topo = LinearTopo(k=int(topoParts[1]))
    elif (topoParts[0] == "assign1"):
        topo = AssignOneTopo()
    elif (topoParts[0] == "triangle"):
        topo = TriangleTopo()
    elif (topoParts[0] == "mesh"):
        if (len(topoParts) != 2 or not topoParts[1].isdigit()):
            sys.exit(1)
        topo = MeshTopo(n=int(topoParts[1]))
    elif (topoParts[0] == "someloops"):
        topo = SomeLoopsTopo()
    else:
        print 'Unknown topology'
        sys.exit(1) 

    net = Mininet(topo=topo, autoSetMacs=True, controller=RemoteController,
            switch=customConstructor({'ovsk' : OVSSwitch}, "ovsk,protocols=OpenFlow13"))

    # Run network
    net.start()
    for h in net.hosts: 
        info('*** ARPing from host %s\n' % (h.name))
        h.cmd('arping -c 2 -A -I '+h.name+'-eth0 '+h.IP())
        starthttp(h)
    CLI( net )
    stophttp()
    net.stop()

