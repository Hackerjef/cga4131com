import asyncio
import logging

from lxml import html as lxmlhtml, etree
import aiohttp
import urllib3
from bs4 import BeautifulSoup

from src.config import Config


class Main:
    def __init__(self):
        self.config = Config()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.task = None
        self.session = None

        if not self.config.Getcfgvalue("modem.verify_ssl", False):
            urllib3.disable_warnings()

    async def runnable(self):
        logging.info("Starting Script")

        logging.info("Creating session")
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=self.config.Getcfgvalue("modem.verify_ssl", False)),
            cookie_jar=aiohttp.CookieJar(unsafe=True))
        await self.ensure_login()
        while True:
            await self.scrape_modem()
            await asyncio.sleep(self.config.Getcfgvalue("general.sleep_interval", 60))

    async def scrape_modem(self):
        res = await self.http(self.session.get, f'{self.router_url}/comcast_network.jst')
        logging.info("Grabbed status page")
        html = await res.text()
        tree = etree.HTML(html)
        # soup = BeautifulSoup(html, 'html.parser')
        stats = {"downstream": [], "upstream": []}

        # TODO: trying to get data out of it

    #  # downstream
    #  # /html/body/div[1]/div[3]/div[3]/div[5]/table
    #  downstream = list(tree.xpath('//*[@id="content"]/div[5]/table')[0].iterchildren())[1].findall('tr')
    # # print(etree.tostring(downstream[0]).decode('utf-8'))
    #  print(downstream)
    #  # upstream
    #  upstream = tree.xpath('//*[@id="content"]/div[6]/table')
    #  # /html/body/div[1]/div[3]/div[3]/div[6]/table

    async def ensure_login(self):
        if not self.config.Getcfgvalue("modem.require_login", False):
            return True

        if any('DUKSID' == e[0] for e in self.session.cookie_jar.filter_cookies(self.router_url).items()):
            res = await self.http(self.session.get, f'{self.router_url}/at_a_glance.jst', raise_for_status=False)
            if res.status == 200:
                return True
            logging.info("Auth session invalidated")
        return await self.do_login()

    async def do_login(self):
        params = {}
        if self.config.Getcfgvalue("modem.username", False):
            params['username'] = self.config.Getcfgvalue("modem.username")
        if self.config.Getcfgvalue("modem.password", False):
            params['password'] = self.config.Getcfgvalue("modem.password")
        res = await self.session.post(f'{self.router_url}/check.jst', data=params, allow_redirects=False)
        if res.status == 302:
            logging.info("Auth session Created")
            return True
        else:
            logging.info("Failed to get Auth session")
            return False

    async def http(self, func, url, raise_for_status=True, **kwargs):
        current_retry_num = 0
        retry = True
        while retry:
            try:
                data = await func(url, **kwargs, allow_redirects=False)
                if raise_for_status:
                    data.raise_for_status()
                return data
            except (aiohttp.ClientConnectorError, ConnectionRefusedError) as ex:
                logging.warning(f"Connection to server rejected retry: {current_retry_num}/{self.config.Getcfgvalue('general.max_retry', 0) if self.config.Getcfgvalue('general.max_retry', 0) != -1 else '∞'}")
            except aiohttp.ClientResponseError as ex:
                if ex.status == 403:
                    logging.warning('Got 403 status code, reAuth')
                    await self.ensure_login()
                else:
                    logging.warning(f"Got Bad status code ({ex.status}) retry: {current_retry_num}/{self.config.Getcfgvalue('general.max_retry', 0) if self.config.Getcfgvalue('general.max_retry', 0) != -1 else '∞'}")
                current_retry_num += 1
                if current_retry_num > self.config.Getcfgvalue('general.max_retry', 0) != -1:
                    raise Exception(f"Max Request retry for url {url}") from ex
            await asyncio.sleep(self.config.Getcfgvalue('general.wait_retry', 0))

    @property
    def router_url(self, url=None):
        base = f"{self.config.Getcfgvalue('modem.proto', 'http')}://{self.config.Getcfgvalue('modem.host', '169.0.0.1')}:{self.config.Getcfgvalue('modem.port', 420)}"
        return base if not url else base + "/" + url


if __name__ == '__main__':
    logging_fmt = '%(asctime)s %(levelname)-8s %(message)s'

    main = Main()
    try:
        root_logger = logging.getLogger()
        root_logger.setLevel(main.config.Getcfgvalue('general.log_level', 'DEBUG'))
        root_handler = root_logger.handlers[0]
        root_handler.setFormatter(logging.Formatter(logging_fmt))
    except IndexError:
        logging.basicConfig(level=main.config.Getcfgvalue('general.log_level', 'DEBUG'), format=logging_fmt)

    try:
        asyncio.run(main.runnable())
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("shutting down...")
        asyncio.run(main.session.close())
        main.loop.run_until_complete(main.loop.shutdown_asyncgens())
        main.loop.close()
