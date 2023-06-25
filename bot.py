from MOEX_API import MoexSession
from lxml import objectify, etree
from datetime import timedelta, date
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
"/iss/history/engines/[engine]/markets/[market]/sessions/[session]/securities/[security]"
"/iss/engines/[engine]/markets/[market]/securities/[security]"


def convert_source_data_to_python_xml(source_xml):
    end_od_encoding_declaration = source_xml.find("\n") + 1
    source_xml = source_xml[end_od_encoding_declaration:]
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


def get_all_ru_stocks_info():
    stocks = session.get_shares_by_broad()
    xml = convert_source_data_to_python_xml(stocks)
    print_xml_in_tables_form(xml)


if __name__ == "__main__":
    session = MoexSession()
    res = session.authorize(LOGIN, PASSWORD)

    if res == "OK":
        # get_some_real_traded_engines(50000)
        # print(session.get_engines())
        # print(session.get_iss_guid())
        # print(session.get_markets())

        # get_all_market_info()
        # print(session.get_sessions_for_shares())
        # print(session.get_broads())
        get_all_ru_stocks_info()


        # current_date = datetime.today()
        # print(session.get_share_info_in_date_interval("YNDX", start_date, end_date))


        # current_date = date(2023, 4, 10)
        # start_date = current_date - START_TIME_INTERVAL_DELTA
        # end_date = current_date - END_TIME_INTERVAL_DELTA
        # print(current_date)
        # print(start_date)
        # print(end_date)
        # print()
        # last_month_share_info = session.get_share_info_in_date_interval("YNDX", start_date, end_date)
        # xml = convert_source_data_to_python_xml(last_month_share_info)
        # num_of_days = len(xml.data.rows.getchildren())
        # total_trading_volume = 0
        # for day in xml.data.rows.getchildren():
        #     total_trading_volume += int(float(day.get("VALUE")))
        #
        # average_value = total_trading_volume // num_of_days
        # pretty_average_value = ""
        # while average_value % 1000 > 0:
        #     pretty_average_value += str(average_value % 1000)[::-1] + " "
        #     average_value = average_value // 1000
        # pretty_average_value = pretty_average_value[::-1].strip()
        # print(f"average trading volume value:   {pretty_average_value}")
