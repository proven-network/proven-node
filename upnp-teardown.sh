#!/bin/bash
# Script to clean up UPnP port forwarding

echo "Cleaning up UPnP port forwarding..."

upnpc -d 30001 TCP
upnpc -d 30304 TCP
upnpc -d 30304 UDP
upnpc -d 10109 TCP
upnpc -d 10109 UDP
upnpc -d 10110 UDP

echo "UPnP port forwarding cleanup complete"
