import time as t
import hashlib
import io
import logging
import os
import requests
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime
import random 


logging.basicConfig(format='%(asctime)s %(levelname)s %(process)d --- %(name)s %(funcName)20s() : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

class OddsScraper:
    logger = logging.getLogger('OddsScraper')

    def __init__(self):
        try:
            self.driver.quit()
        except:
            print('No running driver found')
        self._tmp_folder = '/tmp/img-scrpr-chrm/'
        self.service = Service(executable_path='/usr/bin/chromedriver')
        self.driver = webdriver.Chrome(service = self.service, options=self.__get_default_chrome_options())

    def class_finder(self,content):
        soup = BeautifulSoup(content , 'html.parser') 
        classes = []
        for i in soup.find_all( 'div' ):
            if i.has_attr( "class" ):
                if len(i['class']) > 0:
                    if ('container' in i['class'][0]) & (i['class'][0] not in classes):
                        classes.append(i['class'][0])
        return classes

    def find_odds(self,classes):
        for i in range(0,len(classes)):
            content = self.driver.find_elements(By.CLASS_NAME,classes[i])
            for j in range(0,len(content)):
                if all(elem in content[j].text.split('\n') for elem in ['1','X','2','HANDICAP','OVER','UNDER']):
                    return content[j].text.split('\n')
        return print ('No Odds found')


    def find_odds(self,classes):
        for i in range(0,len(classes)):
            content = self.driver.find_elements(By.CLASS_NAME,classes[i])
            for j in range(0,len(content)):
                if all(elem in content[j].text.split('\n') for elem in ['1','X','2','HANDICAP','OVER','UNDER']):
                    return content[j].text.split('\n')
        return print ('No Odds found')

    def get_odds(self,sport1 = 'Basketball', country1 = 'USA',league1 = 'NBA'):
        #search_url = "https://www.google.com/search?safe=off&site=&tbm=isch&source=hp&q={q}&oq={q}&gs_l=img"
        urlnew = f'''https://www.pinnacle.com/en/{sport1}/{league1}/matchups/'''
        print(urlnew)
        self.driver.get(urlnew)
        ### Time to load
        t.sleep(3)
        p_links = self.driver.find_elements(By.TAG_NAME,'p')
        for p in range(0,len(p_links)):
                #print(p_links[p].text)
            if p_links[p].text == 'See more':
               p_links[p].click()
               break

        ### More loading time
        t.sleep(3)

        ### Get Raw Info
        classes = self.class_finder(self.driver.page_source)
        c_refined = self.find_odds(classes)
        print(c_refined)

        #Cut off parts of result we don't want
        for i in range(0,len(c_refined)):
            if len(c_refined) > 21:
                end = max([i for i,x in enumerate(c_refined) if '+' in x])
        c_refined2 = c_refined[:end]
        return c_refined2

    def close_connection(self):
        self.driver.quit()

    def __scroll_to_end(self, sleep_time):
        num = random.choice([3,4,5,6])
        self.driver.execute_script(f'''window.scrollTo(0, document.body.scrollHeight/{num});''')
        t.sleep(sleep_time)

    def __get_default_chrome_options(self):
        chrome_options = webdriver.ChromeOptions()

        lambda_options = [
            #'--autoplay-policy=user-gesture-required',
            #'--disable-background-networking',
            #'--disable-background-timer-throttling',
            #'--disable-backgrounding-occluded-windows',
            #'--disable-breakpad',
            #'--disable-client-side-phishing-detection',
            #'--disable-component-update',
            #'--disable-default-apps',
            '--disable-dev-shm-usage',
            #'--disable-domain-reliability',
            #'--disable-extensions',
            #'--disable-features=AudioServiceOutOfProcess',
            #'--disable-hang-monitor',
            #'--disable-ipc-flooding-protection',
            #'--disable-notifications',
            #'--disable-offer-store-unmasked-wallet-cards',
            #'--disable-popup-blocking',
            #'--disable-print-preview',
            #'--disable-prompt-on-repost',
            #'--disable-renderer-backgrounding',
            '--disable-setuid-sandbox',
            #'--disable-speech-api',
            #'--disable-sync',
            #'--disk-cache-size=33554432',
            #'--hide-scrollbars',
            #'--ignore-gpu-blacklist',
            #'--ignore-certificate-errors',
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
            '--headless'
            ]

        #chrome_options.add_argument('--disable-gpu')
        for argument in lambda_options:
            chrome_options.add_argument(argument)
        chrome_options.add_argument('--user-data-dir={}'.format(self._tmp_folder + '/user-data'))
        chrome_options.add_argument('--data-path={}'.format(self._tmp_folder + '/data-path'))
        chrome_options.add_argument('--homedir={}'.format(self._tmp_folder))
        chrome_options.add_argument('--disk-cache-dir={}'.format(self._tmp_folder + '/cache-dir'))
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--headless')

        return chrome_options