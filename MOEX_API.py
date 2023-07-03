import requests
from requests.auth import HTTPBasicAuth


USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 ' \
             'Safari/537.36'
HEADERS = {'User-Agent': USER_AGENT}
AUTH_URL = "https://passport.moex.com/authenticate"


class MoexSession:
    def __init__(self):
        self.session = requests.Session()
        self.last_query = ""

    def authorize(self, login, password):
        res = self.session.get(AUTH_URL, headers=HEADERS, auth=HTTPBasicAuth(login, password))
        if res.status_code == 200:
            # micex_passport_cert = self.session.cookies.get('MicexPassportCert')
            # self.session.cert = micex_passport_cert
            return "OK"
        elif res.status_code == 403:
            return "Invalid login or password"
        else:
            return "Something went wrong"

    def get_securities(self, data_type="xml", metadata="off", start=0):
        """
        By default, returns 100 securities. If you want to get them all you should call this function in the loop
        with increased the value of the start parameter.
        """
        self.last_query = f"http://iss.moex.com/iss/securities.{data_type}?iss.meta={metadata}&start={start}"
        return self.session.get(self.last_query, verify=True).text

    def get_iss_guid(self, data_type="xml", metadata="off"):
        self.last_query = f"http://iss.moex.com/iss/index.{data_type}?iss.meta={metadata}"
        return self.session.get(self.last_query, verify=True).text

    def get_engines(self, data_type="xml", metadata="off", start=0):
        self.last_query = f"http://iss.moex.com/iss/engines.{data_type}?iss.meta={metadata}&start={start}"
        return self.session.get(self.last_query, verify=True).text

    def get_markets(self, data_type="xml", metadata="off", start=0, engine_name="stock"):
        self.last_query = f"https://iss.moex.com/iss/engines/{engine_name}/markets.{data_type}?iss.meta={metadata}&start={start}"
        return self.session.get(self.last_query, verify=True).text

    def get_market(self, data_type="xml", metadata="off", engine_name="stock", market_name="shares"):
        self.last_query = f"https://iss.moex.com/iss/engines/{engine_name}/markets/{market_name}.{data_type}?iss.meta={metadata}"
        return self.session.get(self.last_query, verify=True).text

    def get_sessions_for_shares(self, data_type="xml", metadata="off", market_name="shares"):
        self.last_query = f"https://iss.moex.com/iss/history/engines/stock/markets/{market_name}/sessions.{data_type}?iss.meta={metadata}"
        return self.session.get(self.last_query, verify=True).text

    def get_broads(self, data_type="xml", metadata="off", engine_name="stock", market_name="shares"):
        self.last_query = f"https://iss.moex.com/iss/engines/{engine_name}/markets/{market_name}/boards.{data_type}?iss.meta={metadata}"
        return self.session.get(self.last_query, verify=True).text

    def get_shares_by_broad(self, data_type="xml", start=0, metadata="off", engine_name="stock", market_name="shares", broad_id="TQBR"):
        self.last_query = f"https://iss.moex.com/iss/engines/{engine_name}/markets/{market_name}/boards/{broad_id}/securities.{data_type}?iss.meta={metadata}&start={start}"
        return self.session.get(self.last_query, verify=True).text

    def get_share_info_in_date_interval(self, share_name, start_date, end_date, data_type="xml", metadata="off", engine_name="stock", market_name="shares", broad_id="TQBR"):
        self.last_query = f"https://iss.moex.com/iss/history/engines/{engine_name}/markets/{market_name}/boards/{broad_id}/securities/{share_name}.{data_type}?iss.meta={metadata}&from={start_date}&till={end_date}"
        return self.session.get(self.last_query, verify=True).text

    def get_actual_share_info(self, share_name, data_type="xml", metadata="off", engine_name="stock", market_name="shares", broad_id="TQBR"):
        self.last_query = f"https://iss.moex.com/iss/engines/{engine_name}/markets/{market_name}/boards/{broad_id}/securities/{share_name}.{data_type}?iss.meta={metadata}"
        return self.session.get(self.last_query, verify=True).text
