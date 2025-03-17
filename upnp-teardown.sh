#!/bin/bash
# Script to clean up UPnP port forwarding

echo "Cleaning up UPnP port forwarding..."
upnpc -q -d 30001 TCP
upnpc -q -d 30303 TCP
upnpc -q -d 30303 UDP
upnpc -q -d 9000 TCP
upnpc -q -d 9000 UDP
echo "UPnP port forwarding cleanup complete" 
