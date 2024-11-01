import sys  # Remove in production - KTZ
import traceback

# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer', '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalyzer', '/home/ubuntu/ETFAnalyzer/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalyzer/HoldingsDataScripts', '/home/ubuntu/ETFAnalyzer/CommonServices',
                 '/home/ubuntu/ETFAnalyzer/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
from CommonServices.EmailService import EmailSender
import pandas as pd
from datetime import datetime
from datetime import timedelta
from CalculateETFArbitrage.Control import ArbitrageCalculation
from MongoDB.SaveArbitrageCalcs import SaveCalculatedArbitrage
from CalculateETFArbitrage.GetRelevantHoldings import RelevantHoldings
from MongoDB.FetchArbitrage import FetchArbitrage
import logging
import os
path = os.path.join(os.getcwd(), "Logs/")
if not os.path.exists(path):
    os.makedirs(path)

filename = path + datetime.now().strftime("%Y%m%d") + "-ArbEventLog.log"
filename2 = path + datetime.now().strftime("%Y%m%d") + "-ArbErrorLog.log"
handler = logging.FileHandler(filename)
handler2 = logging.FileHandler(filename2)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='w')
# logger = logging.getLogger("EventLogger")
logger = logging.getLogger(__name__)
logger2 = logging.getLogger("ArbErrorLogger")
logger.setLevel(logging.DEBUG)
logger2.setLevel(logging.ERROR)
logger.addHandler(handler)
logger2.addHandler(handler2)

etfwhichfailed = []
# MAKE A LIST OF WORKING ETFs.
workingdf = pd.read_csv("../CSVFiles/250M_WorkingETFs.csv")
# workinglist = workingdf['Symbol'].to_list()
workinglist = workingdf.columns.to_list()
print("List of working ETFs:")
print(workinglist)
print(len(workinglist))
date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# date = '2020-04-03'

# CHECK ARBITRAGE COLLECTION FOR ETFs ALREADY PRESENT.
arb_db_data = FetchArbitrage().fetch_arbitrage_data(date)
arb_db_data_etflist = [arbdata['ETFName'] for arbdata in arb_db_data]
arb_db_data_etflist = list(set(arb_db_data_etflist))
print("List of ETFs whose arbitrage calculation is present in DB:")
print(arb_db_data_etflist)
print(len(arb_db_data_etflist))

# REMOVE THE ETFs, FROM WORKING ETF LIST, WHOSE ARBITRAGE HAS ALREADY BEEN CALCULATED.
print("Updated etflist:")
workingset = set(workinglist)
doneset = set(arb_db_data_etflist)
etflist = list(workingset.difference(doneset))
print(etflist)
print(len(etflist))

for etfname in etflist:
    try:
        print("Doing Analysis for ETF= " + etfname)
        logger.debug("Doing Analysis for ETF= {}".format(etfname))
        data = ArbitrageCalculation().calculateArbitrage(etfname, date)

        if data is None:
            print("Holding Belong to some other Exchange, No data was found")
            logger.error("Holding Belong to some other Exchange, No data was found for {}".format(etfname))
            etfwhichfailed.append(etfname)
            continue
        else:
            data.reset_index(inplace=True)
            data['ETFName'] = etfname
            data['dateOfAnalysis'] = datetime.strptime(date, '%Y-%m-%d')
            data['dateWhenAnalysisRan'] = datetime.now()
            SaveCalculatedArbitrage().insertIntoCollection(data=data.to_dict(orient='records'))

    except Exception as e:
        etfwhichfailed.append(etfname)
        print("exception in {} etf, not crawled".format(etfname))
        print(e)
        traceback.print_exc()
        logger.warning("exception in {} etf, not crawled".format(etfname))
        logger.exception(e)
        logger2.warning("exception in {} etf, not crawled".format(etfname))
        logger2.exception(e)
        emailobj = EmailSender()
        msg = emailobj.message(subject=e,
                               text="Exception Caught in ETFAnalysis/CalculateETFArbitrage/Caller.py for etf: {} \n {}".format(etfname,
                                   traceback.format_exc()))
        emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
        continue
if len(etfwhichfailed) > 0:
    RelevantHoldings().write_to_csv(etfwhichfailed, "etfwhichfailed.csv")

print(etflist)
print(etfwhichfailed)
