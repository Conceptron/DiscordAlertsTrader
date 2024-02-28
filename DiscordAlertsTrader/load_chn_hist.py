import subprocess
import os.path as op
import os
import sys
import pandas as pd
from DiscordAlertsTrader.configurator import cfg
import pandas as pd
from datetime import date
from typing import List
from datetime import datetime, timedelta
from thetadata import ThetaClient, OptionReqType, OptionRight, DateRange
from thetadata import DataType
from DiscordAlertsTrader.marketdata.thetadata_api import ThetaClientAPI
from DiscordAlertsTrader.message_parser import parse_symbol, parse_trade_alert
from DiscordAlertsTrader.configurator import cfg
from DiscordAlertsTrader.alerts_tracker import AlertsTracker
from DiscordAlertsTrader.read_hist_msg import parse_hist_msg
from DiscordAlertsTrader.port_sim import get_hist_quotes
import time
import re
from colorama import init
from colorama.initialise import wrap_stream
from DiscordAlertsTrader.utils.log_to_file import Tee

init(autoreset=True)

original_stdout = sys.stdout
original_stderr = sys.stderr

# parameters
use_theta_rest_api = True
is_mac = True
after, date_after = "--after 2023-06-01", "2023-06-01"
get_date_after_from_port = True
re_download = False
delete_port = False
tracker_logging_config = {
    "portfolio": False
}
close_expired = False

author = "demon"
chan_ids = {
    "theta_warrior_elite": 897625103020490773,
    "demon": 904396043498709072,
    "eclipse": 1196385162490032128,
    "moneymotive": 1012144319282556928,
    "bishop": 1195073059770605568,
    "makeplays": 1164747583638491156,
    "kingmaker": 1152082112032292896,
    "oculus": 1005221780941709312,
    "em_alerts": 1126325195301462117,
    "tpe_team": 1136674041122529403,
    "em_challenge": 1161371386191822870,
}

path_exp = cfg["general"]["data_dir"] + "\..\..\DiscordChatExporter.Cli"
path_out_exp = cfg["general"]["data_dir"] + "\exported"
path_parsed = cfg["general"]["data_dir"] + "\parsed"

if is_mac:
    path_exp = cfg["general"]["data_dir"] + "/../../DiscordChatExporter.Cli"
    path_out_exp = cfg["general"]["data_dir"] + "/exported"
    path_parsed = cfg["general"]["data_dir"] + "/parsed"

os.makedirs(path_out_exp, exist_ok=True)
os.makedirs(path_parsed, exist_ok=True)

port_fname = f"{cfg['general']['data_dir']}\\{author}_port.csv"
if is_mac:
    backtest_ports_dir = f"{cfg['general']['data_dir']}/backtest_ports"
    os.makedirs(backtest_ports_dir, exist_ok=True)
    port_fname = f"{backtest_ports_dir}/{author}_port.csv"

def get_timestamp(row):
    date_time = row[DataType.DATE] + timedelta(milliseconds=row[DataType.MS_OF_DAY])
    return date_time.timestamp()


def add_past_year(alert, year):

    pattern = r"([0-1]?[0-9]\/[0-3]?[0-9])"
    matches = re.findall(pattern, alert)
    current_date = datetime.now()

    modified_dates = []
    for match in matches:
        try:
            date_obj = datetime.strptime(match + f"/{year}", "%m/%d/%Y")
        except ValueError:
            continue

        # Check if the date is within one year from now
        if date_obj <= current_date:
            modified_dates.append(date_obj)
            alert = alert.replace(match, date_obj.strftime("%m/%d/%Y"))

    return alert


def save_or_append_quote(quotes, symbol, path_quotes, overwrite=False):
    fname = f"{path_quotes}/{symbol}.csv"
    if overwrite:
        quotes.to_csv(fname, index=False)
        return
    try:
        df = pd.read_csv(fname)
        df = pd.concat([df, quotes], ignore_index=True)
        df = df.sort_values(by=["timestamp"]).drop_duplicates(subset=["timestamp"])
    except FileNotFoundError:
        df = quotes
    df.to_csv(fname, index=False)


chan_id = chan_ids[author]
if not use_theta_rest_api:
    client = ThetaClient(
        username=cfg["thetadata"]["username"], passwd=cfg["thetadata"]["passwd"]
    )
    client.connect()
else:
    client = ThetaClientAPI()

token = cfg["discord"]["discord_token"]

if delete_port and op.exists(port_fname):
    # ask for confirmation
    response = input(f"Delete {port_fname}? (y/n)")
    if response.lower() == "y":
        os.remove(port_fname)

if op.exists(port_fname) and get_date_after_from_port:
    port = pd.read_csv(port_fname)
    max_date_stc = port["STC-Date"].dropna().max()
    max_date_bto = port["Date"].max()
    # Find the maximum date between the two
    if pd.isna(max_date_stc):
        max_date = max_date_bto
    else:
        max_date = max(max_date_stc, max_date_bto)

    # add 1 second to the max date
    max_date = datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S.%f") + timedelta(
        seconds=1
    )
    after = "--after " + str(max_date).replace(" ", "T")
    date_after = str(max_date).replace(" ", "_").replace(":", "_")

if date_after == "nan":
    date_after = ""

# redirect output to log file also
if is_mac:
    log_file = wrap_stream(
        stream=open(f"{backtest_ports_dir}/{author}_log_{date_after}.txt", "w"),
        convert=None,
        strip=None,
        autoreset=True,
        wrap=True,
    )
    
    sys.stdout = Tee(log_file, original_stdout)
    sys.stderr = Tee(log_file, original_stderr)

print(f"Getting messages after {date_after}.")

fname_out = op.join(path_out_exp, f"{author}_export_{date_after}.json")
if re_download or not op.exists(fname_out):
    command = f"cd {path_exp} && .\DiscordChatExporter.Cli.exe export  -t {token} -f Json -c {chan_id} -o {fname_out} {after}"
    if is_mac:
        command = f"docker run --rm -it -v ~/Documents/code/stocks/DiscordAlertsTrader/data/exported:/out tyrrrz/discordchatexporter:stable export -f Json --channel {chan_id} -t {token} {after} -o {author}_export_{date_after}.json"
    try:
        print("Executing command:", command)
        input("Press Enter to continue.")
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode == 0:
            print("Command executed successfully:")
            print(result.stdout)
        else:
            print("Command failed with error:")
            print(result.stderr)
            input("Press Enter to continue.")
    except Exception as e:
        print("An error occurred:", str(e))
        input("Press Enter to continue.")

msg_hist = parse_hist_msg(fname_out, author)
msg_hist.to_csv(op.join(path_parsed, f"{author}_parsed_{date_after}.csv"), index=False)

tracker = AlertsTracker(
    brokerage=None,
    portfolio_fname=port_fname,
    dir_quotes=cfg["general"]["data_dir"] + "/hist_quotes",
    cfg=cfg,
    logging_config=tracker_logging_config,
)
dt = None
order_date_past = []
no_data = []
no_time_match = []
empty_data = []
outside_market_hours = []
no_date_match = []

for ix, row in msg_hist.iterrows():  # .loc[ix:].iterrows(): #
    print(ix)
    alert = row["Content"]
    if pd.isnull(alert) or not len(alert) or alert in ["@everyone", "@Elite Options"]:
        continue

    year = datetime.strptime(row["Date"][:26], "%m/%d/%Y %H:%M:%S.%f").year
    if year != datetime.now().year:
        alert = add_past_year(alert, year)
    
    pars, order = parse_trade_alert(alert)
    if order is None or order.get("expDate") is None:
        continue

    print(f"Processing {pars}")

    order["Trader"] = row["Author"]
    dt = datetime.strptime(row["Date"], "%m/%d/%Y %H:%M:%S.%f")  # + timedelta(hours=2)
    order["Date"] = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    tsm = round(pd.to_datetime(order["Date"]).timestamp())

    full_date = (
        order["expDate"] + f"/{year}"
        if len(order["expDate"].split("/")) == 2
        else order["expDate"]
    )
    dt_fm = (
        "%m/%d/%y"
        if len(full_date.split("/")) == 2
        else "%m/%d/%Y" if len(full_date.split("/")[2]) == 4 else "%m/%d/%y"
    )
    if datetime.strptime(full_date, dt_fm).date() < dt.date():
        print("Order date in the past, skipping", order["expDate"], order["Date"])
        order_date_past.append(order)
        resp = tracker.trade_alert(order, live_alert=False, channel=author)
        continue

    try:
        if use_theta_rest_api:
            out = client.get_hist_quotes(order["Symbol"], [dt.date()])
        else:
            out = get_hist_quotes(order["Symbol"], [dt.date()], client)
        # save_or_append_quote(out, order['Symbol'], 'data/hist_quotes')
    except Exception as e:
        print(f"row {ix}, No data for", order["Symbol"], order["Date"], e)
        no_data.append(order)
        resp = tracker.trade_alert(order, live_alert=False, channel=author)
        continue
    if not len(out):
        print("0 data for", order["Symbol"], order["Date"])
        empty_data.append(order)
        resp = tracker.trade_alert(order, live_alert=False, channel=author)
        continue

    if tsm > out.iloc[-1]["timestamp"]:
        print(
            "Order time outside of market hours",
            order["action"],
            order["expDate"],
            order["Date"],
            order["Symbol"],
            ix,
        )
        outside_market_hours.append(order)
        resp = tracker.trade_alert(order, live_alert=False, channel=author)
        continue

    try:
        order["price_actual"] = out.iloc[out[out["timestamp"] == tsm].index[0] + 1]["ask"]
    except IndexError:
        print("No time match for", order["Symbol"], order["Date"])
        no_time_match.append(order)

    bto_date = tracker.portfolio[
        (tracker.portfolio["Symbol"] == order["Symbol"]) & (tracker.portfolio["isOpen"])
    ]
    if len(bto_date):
        bto_date = bto_date["Date"]

    try:
        resp = tracker.trade_alert(order, live_alert=False, channel=author)
    except:
        print("No date match for", order["Symbol"], order["Date"])
        no_date_match.append(order)
        continue

    if order["action"] == "STC":
        if resp == "STC without BTO":
            print("STC without BTO", order["Symbol"], order["Date"])

if close_expired:
    tracker.close_expired()

# report errors
print(f"Order date in the past: {len(order_date_past)}")
print(f"No data: {len(no_data)}")
print(f"Empty data: {len(empty_data)}")
print(f"Outside market hours: {len(outside_market_hours)}")
print(f"No time match: {len(no_time_match)}")
print(f"No date match: {len(no_date_match)}")
print(f"Total trades in portfolio: {len(tracker.portfolio)}")

# save orders with errors
if len(order_date_past):
    pd.DataFrame(order_date_past).to_csv(
        op.join(path_parsed, f"{author}_order_date_past_{date_after}.csv"), index=False
    )
if len(no_data):
    pd.DataFrame(no_data).to_csv(
        op.join(path_parsed, f"{author}_no_data_{date_after}.csv"), index=False
    )
if len(empty_data):
    pd.DataFrame(empty_data).to_csv(
        op.join(path_parsed, f"{author}_empty_data_{date_after}.csv"), index=False
    )
if len(outside_market_hours):
    pd.DataFrame(outside_market_hours).to_csv(
        op.join(path_parsed, f"{author}_outside_market_hours_{date_after}.csv"), index=False
    )
if len(no_time_match):
    pd.DataFrame(no_time_match).to_csv(
        op.join(path_parsed, f"{author}_no_time_match_{date_after}.csv"), index=False
    )
if len(no_date_match):
    pd.DataFrame(no_date_match).to_csv(
        op.join(path_parsed, f"{author}_no_date_match_{date_after}.csv"), index=False
    )



tracker.portfolio.to_csv(tracker.portfolio_fname, index=False)
