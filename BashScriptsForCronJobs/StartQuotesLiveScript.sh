#!/bin/bash
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/ETFLiveAnalysisProdWS/ || exit
python QuotesLive.py
