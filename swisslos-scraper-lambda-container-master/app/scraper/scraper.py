import time as t
import hashlib
import io
import logging
import os
#import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime


logging.basicConfig(format='%(asctime)s %(levelname)s %(process)d --- %(name)s %(funcName)20s() : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

class OddsScraper:
    logger = logging.getLogger('OddsScraper')

    def __init__(self):
        self._tmp_folder = '/tmp/img-scrpr-chrm/'
        self.driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver', options=self.__get_default_chrome_options())

    def __get_link(self,sport = 'Basketball', country = 'USA',league = 'NBA'):
        self.driver.get('https://www.swisslos.ch/en/sporttip/sportsbetting/bet/')    
        t.sleep(3)
        tags = self.driver.find_elements(By.TAG_NAME,"a")
        links = []
        for tag in tags:
            if tag.text == sport:
                tag.click()
                t.sleep(4)
                tags2 = self.driver.find_elements(By.TAG_NAME,"a")
                for tag2 in tags2:
                    #print(tag2.text)
                    if tag2.text == country:
                        tag2.click()
                        t.sleep(3)
                        tags3 = self.driver.find_elements(By.TAG_NAME,"a")
                        for tag3 in tags3:
                            if tag3.text == league:
                                tag3.click()
                                t.sleep(3)
                                url_curr = self.driver.current_url
                                print(url_curr)
                                return url_curr
            #break
        #links.append([tag.get_attribute('href'),tag.text])
        return 'nothing'#'https://www.swisslos.ch/en/sporttip/sportsbetting/bet/sport/2/group/15/competition/1353'

    def get_odds(self,sport1 = 'Basketball', country1 = 'USA',league1 = 'NBA'):
        #search_url = "https://www.google.com/search?safe=off&site=&tbm=isch&source=hp&q={q}&oq={q}&gs_l=img"
        #Temporary: PASTE IN LINK MANUALLY
        urlnew = self.__get_link(sport = sport1, country = country1,league = league1)
        print(urlnew)
        self.driver.get(urlnew)
        t.sleep(4.5)
        
        ### Get Raw Info
        bet_type = self.driver.find_elements(By.CLASS_NAME,'o_bet-row__actions')[0].text.split('\n')[0]
        odds = self.driver.find_elements(By.CLASS_NAME,'m_bet-group')
        dates = self.driver.find_elements(By.CLASS_NAME,'o_bet-row__info')
        
        ### Dates
        dates_final = []
        date_curr = ''
        for i in range(1,len(dates)):
            if(len(dates[i].text.split(' ')) > 1):
                date_curr = dates[i].text.split(' ')[1]
                continue
            dates_final.append(date_curr)
        
        ### Odds
        odds_final = []
        for odd in odds:
            odds_final.append(odd.text.split('\n'))
    
        ### Compile odds, dates, and bet type
        final = []
        for k in range(0,len(dates_final)):
            final.append([dates_final[k],bet_type,odds_final[k]])
        return final

    def close_connection(self):
        self.driver.quit()

    def __scroll_to_end(self, sleep_time):
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        t.sleep(sleep_time)

    def __get_default_chrome_options(self):
        chrome_options = webdriver.ChromeOptions()

        lambda_options = [
            '--autoplay-policy=user-gesture-required',
            '--disable-background-networking',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-breakpad',
            '--disable-client-side-phishing-detection',
            '--disable-component-update',
            '--disable-default-apps',
            '--disable-dev-shm-usage',
            '--disable-domain-reliability',
            '--disable-extensions',
            '--disable-features=AudioServiceOutOfProcess',
            '--disable-hang-monitor',
            '--disable-ipc-flooding-protection',
            '--disable-notifications',
            '--disable-offer-store-unmasked-wallet-cards',
            '--disable-popup-blocking',
            '--disable-print-preview',
            '--disable-prompt-on-repost',
            '--disable-renderer-backgrounding',
            '--disable-setuid-sandbox',
            '--disable-speech-api',
            '--disable-sync',
            '--disk-cache-size=33554432',
            '--hide-scrollbars',
            '--ignore-gpu-blacklist',
            '--ignore-certificate-errors',
            '--metrics-recording-only',
            '--mute-audio',
            '--no-default-browser-check',
            '--no-first-run',
            '--no-pings',
            '--no-sandbox',
            '--no-zygote',
            '--password-store=basic',
            '--use-gl=swiftshader',
            '--use-mock-keychain',
            '--single-process',
            '--headless']

        #chrome_options.add_argument('--disable-gpu')
        for argument in lambda_options:
            chrome_options.add_argument(argument)
        chrome_options.add_argument('--user-data-dir={}'.format(self._tmp_folder + '/user-data'))
        chrome_options.add_argument('--data-path={}'.format(self._tmp_folder + '/data-path'))
        chrome_options.add_argument('--homedir={}'.format(self._tmp_folder))
        chrome_options.add_argument('--disk-cache-dir={}'.format(self._tmp_folder + '/cache-dir'))
        chrome_options.add_argument("--window-size=1920,1080")

        return chrome_options