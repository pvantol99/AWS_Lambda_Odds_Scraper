import time as t
import hashlib
import io
import logging
import os
#import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

# Optional: use webdriver-manager for local dev when chromedriver is not at /usr/bin/chromedriver
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    ChromeDriverManager = None


logging.basicConfig(format='%(asctime)s %(levelname)s %(process)d --- %(name)s %(funcName)20s() : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

class OddsScraper:
    logger = logging.getLogger('OddsScraper')

    def __init__(self):
        self._tmp_folder = '/tmp/img-scrpr-chrm/'
        chromedriver_path = '/usr/bin/chromedriver'
        if os.path.isfile(chromedriver_path):
            self.service = Service(executable_path=chromedriver_path)
        elif ChromeDriverManager is not None:
            self.service = Service(executable_path=ChromeDriverManager().install())
        else:
            raise RuntimeError(
                'ChromeDriver not found. Install webdriver-manager: pip install webdriver-manager'
            )
        self.driver = webdriver.Chrome(service=self.service, options=self.__get_default_chrome_options())

    def _slug(self, s):
        """Lowercase, strip, replace spaces with hyphens."""
        return s.lower().strip().replace(' ', '-')

    def __get_link(self, sport='Football', country='Switzerland', league='Super League'):
        # New URL format: https://www.swisslos.ch/en/sporttip/sports/football/switzerland/super-league/
        sport_slug = self._slug(sport)
        if sport_slug == 'soccer':
            sport_slug = 'football'
        country_slug = self._slug(country)
        league_slug = self._slug(league)
        url = f'https://www.swisslos.ch/en/sporttip/sports/{sport_slug}/{country_slug}/{league_slug}/'
        return url

    def get_odds(self, sport1='Football', country1='Switzerland', league1='Super League', include_player_markets=False, debug=False):
        urlnew = self.__get_link(sport=sport1, country=country1, league=league1)
        print(urlnew)
        self.driver.get(urlnew)
        wait = WebDriverWait(self.driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "asw-sports-betting")))
        t.sleep(2)
        rows = self.driver.find_elements(By.CSS_SELECTOR, "asw-sports-grid-row-event")
        # First pass: collect row data and event URLs from league page (avoid stale elements)
        rows_data = []
        for row in rows:
            try:
                link = row.find_element(By.CSS_SELECTOR, "a[title]")
                title = link.get_attribute("title") or ""
                teams = [s.strip() for s in title.split(":")] if title else []
                if len(teams) < 2:
                    comps = row.find_elements(By.CSS_SELECTOR, "asw-mini-scoreboard-competitors span.underline-text")
                    teams = [c.text.strip() for c in comps[:2]]
                event_href = link.get_attribute("href") or ""
                time_el = row.find_elements(By.CSS_SELECTOR, "asw-time-info time")
                time_str = time_el[0].text.strip() if time_el else ""
                markets = row.find_elements(By.CSS_SELECTOR, "asw-sports-grid-row-market")
                odds_1x2 = []
                odds_ou = []
                ou_line = None
                for m in markets:
                    sel_buttons = m.find_elements(By.CSS_SELECTOR, "button.btn-selection")
                    values = []
                    for btn in sel_buttons:
                        spans = btn.find_elements(By.CSS_SELECTOR, "div.d-flex.flex-column span")
                        found = None
                        for sp in spans:
                            txt = sp.text.strip()
                            if txt and txt.replace(".", "", 1).replace(",", "").isdigit():
                                found = txt
                                break
                        values.append(found or btn.text.strip() or "")
                    values = [v for v in values if v]
                    if not values:
                        continue
                    if len(odds_1x2) == 0 and len(values) >= 3:
                        odds_1x2 = values[:3]
                    elif len(odds_ou) == 0:
                        odds_ou = values
                        # Over/Under line is in a separator div (e.g. "3.5") between Over and Under buttons
                        sep = m.find_elements(By.CSS_SELECTOR, "div[id*='separator'], div.ms-1.w-33.text-center.text-muted")
                        if sep:
                            ou_line = sep[0].text.strip()
                # Build Over/Under with line when we have it
                if odds_ou and len(odds_ou) >= 2:
                    over_under = {"line": ou_line or "", "over": odds_ou[0], "under": odds_ou[1]}
                else:
                    over_under = odds_ou if odds_ou else None
                row_data = {
                    "time": time_str,
                    "teams": teams,
                    "1X2": odds_1x2,
                    "Over/Under": over_under,
                }
                rows_data.append((row_data, event_href))
            except Exception as e:
                if debug:
                    self.logger.warning("Skip row: %s", e)
                continue
        # Second pass: optionally fetch player markets per event (navigates to each event page)
        final = []
        for row_data, event_href in rows_data:
            if include_player_markets and event_href:
                try:
                    row_data["player_markets"] = self.get_player_markets(event_href)
                except Exception as e:
                    if debug:
                        self.logger.warning("Player markets for %s: %s", event_href, e)
                    row_data["player_markets"] = []
            final.append(row_data)
        return final

    def close_connection(self):
        self.driver.quit()

    def __scroll_to_end(self, sleep_time):
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        t.sleep(sleep_time)

    def __get_default_chrome_options(self):
        chrome_options = webdriver.ChromeOptions()
        is_lambda = os.path.isfile('/usr/bin/chromedriver')

        if is_lambda:
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
            for argument in lambda_options:
                chrome_options.add_argument(argument)
            chrome_options.add_argument('--user-data-dir={}'.format(self._tmp_folder + '/user-data'))
            chrome_options.add_argument('--data-path={}'.format(self._tmp_folder + '/data-path'))
            chrome_options.add_argument('--homedir={}'.format(self._tmp_folder))
            chrome_options.add_argument('--disk-cache-dir={}'.format(self._tmp_folder + '/cache-dir'))
            chrome_options.add_argument("--window-size=1920,1080")
        else:
            # Local dev: minimal options that work on macOS/Windows/Linux
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--headless')

        return chrome_options

    def get_player_markets(self, event_url: str) -> list:
        """
        Scrape player markets (e.g. Goalscorer) from an event detail page.
        Opens the event URL, clicks the "Player" tab by name, then scrapes markets that have selections.
        event_url: full or relative URL of the match (e.g. .../fc-basel-vs-grasshoppers?t=1772739)
        Returns list of {"market": str, "selections": [{"name": str, "odds": str}, ...]}.
        """
        base = "https://www.swisslos.ch"
        if event_url.startswith("/"):
            event_url = base + event_url
        elif not event_url.startswith("http"):
            event_url = base + "/" + event_url
        self.driver.get(event_url)
        wait = WebDriverWait(self.driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "asw-marketboard-market")))
        t.sleep(2)
        # Click the "Player" tab by name (title is "Player (n)" where n = number of markets; we match "Player" only)
        try:
            for el in self.driver.find_elements(
                By.XPATH, "//*[starts-with(normalize-space(text()), 'Player')]"
            ):
                try:
                    el.click()
                    t.sleep(1.5)
                    break
                except Exception:
                    continue
        except Exception:
            pass
        result = []
        markets = self.driver.find_elements(By.CSS_SELECTOR, "asw-marketboard-market")
        for m in markets:
            try:
                header_divs = m.find_elements(By.CSS_SELECTOR, "div.px-3.sports-grid-py")
                market_name = "Unknown"
                if header_divs:
                    market_name = header_divs[0].text.split("\n")[0].strip() or "Unknown"
                selections = []
                sel_els = m.find_elements(By.CSS_SELECTOR, "asw-marketboard-selection button")
                for btn in sel_els:
                    name_span = btn.find_elements(By.CSS_SELECTOR, "span.text-muted")
                    odds_span = btn.find_elements(By.CSS_SELECTOR, "span.fw-bold")
                    name = name_span[0].text.strip() if name_span else ""
                    odds = odds_span[0].text.strip() if odds_span else ""
                    if name or odds:
                        selections.append({"name": name, "odds": odds})
                # Only include markets that have actual selections (skip collapsed/empty headers)
                if selections:
                    result.append({"market": market_name, "selections": selections})
            except Exception as e:
                self.logger.warning("Skip market block: %s", e)
                continue
        return result