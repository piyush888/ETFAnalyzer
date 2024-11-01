import traceback

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datetime import datetime
import time
from CommonServices.EmailService import EmailSender
from CommonServices.WebdriverServices import masterclass
# Logging
###############################################################################################################
import logging

import os
path = os.path.join(os.getcwd(), "Logs/HoldingsScraperLogs/")
if not os.path.exists(path):
    os.makedirs(path)
filename = path + datetime.now().strftime("%Y%m%d") + "-HoldingsDataLogs.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logger.addHandler(handler)
################################################################################################################

class Download523TickersList(masterclass):

    def fetchTickerDataDescription(self, url):
        # initialise driver and login to ETFdb
        retries = 1
        while retries >=0:
            try:
                super().initialisewebdriver()
                super().logintoetfdb()
                # Fetch List using this url
                # url = 'https://etfdb.com/etfs/country/us/'
                self.driver.get(url)
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//td/a[@class='btn btn-medium btn-primary' and 2]")))
                e = self.driver.find_element_by_xpath("//td/a[@class='btn btn-medium btn-primary' and 2]")
                e.click()
                if url == 'https://etfdb.com/etfs/country/us/':
                    time.sleep(25)
                else:
                    time.sleep(10)
                self.driver.quit()
                # if successfully downloaded, no retries needed
                retries = -1
            except Exception as e:
                print("Exception in Download523TickersList.py")
                print(e)
                logger.exception("Exception in Download523TickersList.py")
                logger.info("Retrying once more")
                traceback.print_exc()
                retries -= 1
                self.driver.quit()
                # send email on every failure
                emailobj = EmailSender()
                msg = emailobj.message(subject=e,
                                       text="Exception Caught in ETFAnalysis/ETFsList_Scripts/Download523TickersList.py {}".format(
                                           traceback.format_exc()))
                emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
                pass
