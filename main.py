import asyncio
import json
import logging
from asyncio import CancelledError
from datetime import timedelta
from functools import reduce

import aiohttp
from aiohttp import web
import urllib3
import pandas as pd
import yaml
from aiohttp.web import _run_app, _cancel_tasks
from bs4 import BeautifulSoup


class Main:
    def __init__(self):
        with open('config.yaml', 'r') as f:
            self.cfg = yaml.load(f.read(), Loader=yaml.FullLoader)
            f.close()

        if not self.Getcfgvalue("modem.verify_ssl", False):
            urllib3.disable_warnings()

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.shutdown_event = asyncio.Event()
        self.session = None
        self.export_data = {}

        self.webapp = None
        self.webapp_task = None

    async def webserver(self):
        def _print(msg):
            logging.info(
                f"Webserver started on: http://{self.Getcfgvalue('webserver.host', '127.0.0.1')}:{self.Getcfgvalue('webserver.port', 418)}")

        async def wrapper(awaitable):
            try:
                return await awaitable
            except CancelledError:
                pass

        self.webapp = web.Application()
        self.webapp.add_routes([web.get('/json', self.ws_json)])
        self.webapp_task = self.loop.create_task(wrapper(_run_app(self.webapp, print=_print, host=self.Getcfgvalue('webserver.host', '127.0.0.1'), port=self.Getcfgvalue('webserver.port', 418))))

    async def ws_json(self, response):
        return web.json_response(self.export_data)

    async def runnable(self):
        logging.info("Creating session")
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=self.Getcfgvalue("modem.verify_ssl", False)),
            cookie_jar=aiohttp.CookieJar(unsafe=True))
        await self.ensure_login()
        while not self.shutdown_event.is_set():
            await self.scrape_modem()
            await asyncio.sleep(self.Getcfgvalue("general.sleep_interval", 60))
        logging.warning("Application shutdown")

    async def scrape_modem(self):
        res = await self.http(self.session.get, f'{self.router_url}/comcast_network.jst')
        logging.info("Grabbed status page")

        soup = BeautifulSoup(await res.text(), 'html.parser')
        uptime_stamp = soup.select_one('#content > div:nth-child(3) > div:nth-child(4) > span.value').text.replace(
            ' days', '').replace('h:', '').replace('m:', '').replace('s', '').split(' ')
        uptime = timedelta(days=int(uptime_stamp[0]), hours=int(uptime_stamp[1]), minutes=int(uptime_stamp[2]),
                           seconds=int(uptime_stamp[3])).seconds
        dstb = soup.select_one('#content > div:nth-of-type(5) > table > tbody')
        ustb = soup.select_one('#content > div:nth-of-type(6) > table > tbody')
        ertb = soup.select_one('#content > div:nth-of-type(7) > table > tbody')

        downstream = [[] for x in dstb.find_all("tr")]
        upstream = [[] for x in ustb.find_all("tr")]
        error = [[] for x in ertb.find_all("tr")]

        # downstream
        for table_rowid, table_row in enumerate(dstb.find_all("tr")):
            for td_rowid, td_row in enumerate(table_row.find_all("td")):
                downstream[table_rowid].append(td_row.select_one('td > div').text)

        for table_rowid, table_row in enumerate(ustb.find_all("tr")):
            for td_rowid, td_row in enumerate(table_row.find_all("td")):
                upstream[table_rowid].append(td_row.select_one('td > div').text)

        for table_rowid, table_row in enumerate(ertb.find_all("tr")):
            for td_rowid, td_row in enumerate(table_row.find_all("td")):
                error[table_rowid].append(td_row.select_one('td > div').text)

        dfds = pd.concat([pd.DataFrame(x) for x in downstream], axis=1).set_axis(
            ['channel', 'lock_status', 'frequency', 'snr', 'power_level', 'modulation'], axis=1, copy=False).to_dict(
            'records')
        dfus = pd.concat([pd.DataFrame(x) for x in upstream], axis=1).set_axis(
            ['channel', 'lock_status', 'frequency', 'symbol_rate', 'power_level', 'modulation', 'channel_type'], axis=1,
            copy=False).to_dict('records')
        dfer = pd.concat([pd.DataFrame(x) for x in error], axis=1).set_axis(
            ['unerrored', 'correctable', 'uncorrectable'], axis=1, copy=False).to_dict('records')
        self.export_data = {"downstream": self.clean_list(dfds), "upstream": self.clean_list(dfus),
                            "error": self.clean_list(dfer), 'uptime': uptime}
        logging.info("Updated export data")

    async def ensure_login(self):
        if not self.Getcfgvalue("modem.require_login", False):
            return True

        if any('DUKSID' == e[0] for e in self.session.cookie_jar.filter_cookies(self.router_url).items()):
            res = await self.http(self.session.get, f'{self.router_url}/at_a_glance.jst', raise_for_status=False)
            if res.status == 200:
                return True
            logging.info("Auth session invalidated")
        return await self.do_login()

    async def do_login(self):
        params = {}
        if self.Getcfgvalue("modem.username", False):
            params['username'] = self.Getcfgvalue("modem.username")
        if self.Getcfgvalue("modem.password", False):
            params['password'] = self.Getcfgvalue("modem.password")
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
                logging.warning(
                    f"Connection to server rejected retry: {current_retry_num}/{self.Getcfgvalue('general.max_retry', 0) if self.Getcfgvalue('general.max_retry', 0) != -1 else '∞'}")
            except aiohttp.ClientResponseError as ex:
                if ex.status == 403:
                    logging.warning('Got 403 status code, reAuth')
                    await self.ensure_login()
                else:
                    logging.warning(
                        f"Got Bad status code ({ex.status}) retry: {current_retry_num}/{self.Getcfgvalue('general.max_retry', 0) if self.Getcfgvalue('general.max_retry', 0) != -1 else '∞'}")
                current_retry_num += 1
                if current_retry_num > self.Getcfgvalue('general.max_retry', 0) != -1:
                    raise Exception(f"Max Request retry for url {url}") from ex
            await asyncio.sleep(self.Getcfgvalue('general.wait_retry', 0))

    def Getcfgvalue(self, keys, default=None):
        return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), self.cfg)

    def clean_list(self, _list):
        result = []
        for i, v in enumerate(_list):
            _dict = {}
            for key, value in v.items():
                match key:
                    case "frequency":
                        _dict[key] = value.replace(" MHz", "")
                    case "snr":
                        _dict[key] = value.replace(" dB", "")
                    case "power_level":
                        _dict[key] = value.replace(" dBmV", "")
                    case "modulation" | "channel_type":
                        pass
                    case _:
                        _dict[key] = value
            result.append(_dict)
        return result

    @property
    def router_url(self, url=None):
        base = f"{self.Getcfgvalue('modem.proto', 'http')}://{self.Getcfgvalue('modem.host', '169.0.0.1')}:{self.Getcfgvalue('modem.port', 420)}"
        return base if not url else base + "/" + url


if __name__ == '__main__':
    logging_fmt = '%(asctime)s %(levelname)-8s %(message)s'

    main = Main()
    try:
        root_logger = logging.getLogger()
        root_logger.setLevel(main.Getcfgvalue('general.log_level', 'DEBUG'))
        root_handler = root_logger.handlers[0]
        root_handler.setFormatter(logging.Formatter(logging_fmt))
    except IndexError:
        logging.basicConfig(level=main.Getcfgvalue('general.log_level', 'DEBUG'), format=logging_fmt)

    main.loop.create_task(main.runnable())
    main.loop.create_task(main.webserver())

    try:
        main.loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logging.warning("shutting down...")
        main.webapp_task.cancel()
        main.shutdown_event.set()
        asyncio.run(main.session.close())
        main.loop.run_until_complete(main.loop.shutdown_asyncgens())
        main.loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop=main.loop)))
    main.loop.close()
