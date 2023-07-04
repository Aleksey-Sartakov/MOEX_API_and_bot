from MOEX_API import MoexSession
from lxml import objectify, etree
from datetime import timedelta, date
from threading import Timer
from time import sleep
import pandas as pd
import numpy as np
import json
import tabulate

LOGIN = "sartakov.aleksey.d@gmail.com"
PASSWORD = "Sfadnca05"
BOARD_ID = "TQBR"
ENGINE_NAME = "stock"
MARKET_NAME = "shares"
START_TIME_INTERVAL_DELTA = timedelta(days=31)
END_TIME_INTERVAL_DELTA = timedelta(days=1)
TRADING_SESSION_NAME = "main"
GROWTH_RATE = 3


def convert_source_data_to_python_xml(source_xml):
    end_od_encoding_declaration = source_xml.find("\n") + 1
    source_xml = source_xml[end_od_encoding_declaration:]
    # print(source_xml)
    xml = objectify.fromstring(source_xml)
    return xml


def print_xml_in_tables_form(xml):
    for data in xml.getchildren():
        table_name = data.get("id")
        data_info = data.rows.getchildren()
        columns_names = []
        try:
            for attribute in data_info[0].items():
                columns_names.append(attribute[0])
            table = pd.DataFrame(columns=columns_names)

            for i, row in enumerate(data_info):
                row_data = []
                for attribute in row.items():
                    row_data.append(attribute[1])
                row = dict(zip(columns_names, row_data))
                s_row = pd.Series(row)
                s_row.name = i
                table = table._append(s_row)

            print("------------" + table_name.upper() + "------------")
            print(table.to_markdown())
            print()
            print()

        except IndexError:
            pass


def get_some_real_traded_engines(count):
    start = 0
    securities = session.get_securities(start=start)
    xml = convert_source_data_to_python_xml(securities)
    securities = xml.data.rows.getchildren()

    columns_names = []
    for attribute in securities[0].items():
        columns_names.append(attribute[0])

    table = pd.DataFrame(columns=columns_names)
    i = 1
    while start <= count:
        for security in securities:
            security_data = []
            if security.get("emitent_okpo") != "":
                for attribute in security.items():
                    security_data.append(attribute[1])
                row = dict(zip(columns_names, security_data))
                s_row = pd.Series(row)
                s_row.name = i
                table = table._append(s_row)
                i += 1

        start += 100
        securities = session.get_securities(start=start)
        xml = convert_source_data_to_python_xml(securities)
        securities = xml.data.rows.getchildren()

    print(table.to_markdown())


def get_all_market_info():
    market_info = session.get_market()
    xml = convert_source_data_to_python_xml(market_info)
    print_xml_in_tables_form(xml)


def get_all_ru_shares_info():
    stocks = session.get_shares_by_broad()
    xml = convert_source_data_to_python_xml(stocks)
    print_xml_in_tables_form(xml)


def get_all_ru_shares_id():
    shares = session.get_shares_by_broad()
    xml = convert_source_data_to_python_xml(shares)
    shares_id = {}
    for data in xml.getchildren():
        if data.get("id") == "marketdata":
            data_info = data.rows.getchildren()
            for row in data_info:
                if row.get("SECID") is not None:
                    shares_id[row.get("SECID")] = {"detected": 0}
            break
    return shares_id


def get_shares_average_and_last_day_trading_volumes(shares):
    current_date = date.today()
    start_date = current_date - START_TIME_INTERVAL_DELTA
    end_date = current_date - END_TIME_INTERVAL_DELTA

    shares_to_delete = []
    for share in shares.keys():
        last_month_share_info = session.get_share_info_in_date_interval(share, start_date, end_date)
        xml = convert_source_data_to_python_xml(last_month_share_info)
        days = xml.data.rows.getchildren()
        num_of_days = len(days)

        if num_of_days == 0:
            shares_to_delete.append(share)
        elif days[-1].get("VALUE") == "0":
            shares_to_delete.append(share)
    for share in shares_to_delete:
        del shares[share]

    for share in shares.keys():
        last_month_share_info = session.get_share_info_in_date_interval(share, start_date, end_date)
        xml = convert_source_data_to_python_xml(last_month_share_info)
        days = xml.data.rows.getchildren()
        num_of_days = len(days)
        total_trading_volume = 0

        for day in days:
            total_trading_volume += int(float(day.get("VALUE")))

        last_day_trading_volume = int(float(days[-1].get("VALUE")))
        average_trading_volume = total_trading_volume // num_of_days
        shares[share]["average_trading_volume"] = average_trading_volume
        shares[share]["last_day_trading_volume"] = last_day_trading_volume



def get_actual_share_data(share):
    actual_share_data = session.get_actual_share_info(share)
    xml = convert_source_data_to_python_xml(actual_share_data)
    for data in xml.getchildren():
        if data.get("id") == "marketdata":
            share_info = data.rows.getchildren()[0]

            try:
                opening_price = float(share_info.get("OPEN"))
                last_price = float(share_info.get("LAST"))
                current_trading_volume = int(share_info.get("VALTODAY"))
                trend = last_price / opening_price
            except ValueError:
                last_price = 0.0
                current_trading_volume = 0
                trend = 0
            update_time = share_info.get("UPDATETIME")

            return [trend, current_trading_volume, last_price, update_time]


def find_necessary_shares(shares,found_shares):
    for share in shares.keys():
        trend, current_trading_volume, last_price, update_time = get_actual_share_data(share)
        if (trend > 1 and
            current_trading_volume >= shares[share]["average_trading_volume"] * GROWTH_RATE and
            current_trading_volume >= shares[share]["last_day_trading_volume"] * GROWTH_RATE and
            shares[share]["detected"] == 0 and
            share not in found_shares
        ):
            found_shares.append(share)
            print(f"share: {share},   current: {current_trading_volume},   last day: {shares[share]['last_day_trading_volume']},   average trading volume: {shares[share]['average_trading_volume']},   update_time: {update_time}")
            with open("necessary_shares.txt", "a+") as file:
                file.write(f"{share}: {{last_price: {last_price}, current_trading_volume: {current_trading_volume},   average trading volume: {shares[share]['average_trading_volume']},   update_time: {update_time}}}\n")


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def hello(name):
    with open("test.txt", "a+") as file:
        file.write(f"\n===================={name}====================\n")


if __name__ == "__main__":
    session = MoexSession()
    res = session.authorize(LOGIN, PASSWORD)

    if res == "OK":
        # get_some_real_traded_engines(50000)
        # print(session.get_engines())
        # print(session.get_iss_guid())
        # print(session.get_markets())

        # get_all_market_info()

        # get_all_ru_shares_info()

        # print(session.get_actual_share_info("YNDX"))

        current_date = date.today()
        with open("necessary_shares.txt", "a+") as file:
            file.write(f"\n===================={current_date}====================\n")

        shares = get_all_ru_shares_id()
        found_shares = []
        get_shares_average_and_last_day_trading_volumes(shares)

        find_necessary_shares(shares, found_shares)
        repeated_timer = RepeatedTimer(1800, find_necessary_shares, shares, found_shares)
        # try:
        #     sleep(5)
        # finally:
        #     repeated_timer.stop()
