#!/bin/bash
# Script to clean up UPnP port forwarding

echo "Cleaning up UPnP port forwarding..."

upnpc -d 30001 TCP
upnpc -d 30303 TCP
upnpc -d 30303 UDP
upnpc -d 9909 TCP
upnpc -d 9909 UDP
upnpc -d 9910 UDP

echo "UPnP port forwarding cleanup complete" 
