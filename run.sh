#!/usr/bin/env bash
sudo python3 sync-swap.py 
sudo python3 sync-transfer.py 
sudo python3 sync-pls.py 
sudo python3 sync-wrap.py 
wait 
sudo python3 combine.py
sudo python3 makecsv.py