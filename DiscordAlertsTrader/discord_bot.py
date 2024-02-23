
import os
import time
import pandas as pd
from datetime import datetime, timezone, date
import threading
from colorama import Fore, init
import discord # this is discord.py-self package not discord
import pytz

from DiscordAlertsTrader.message_parser import parse_trade_alert
from DiscordAlertsTrader.configurator import cfg
from DiscordAlertsTrader.configurator import channel_ids
from DiscordAlertsTrader.alerts_trader import AlertsTrader
from DiscordAlertsTrader.alerts_tracker import AlertsTracker
from DiscordAlertsTrader.server_alert_formatting import server_formatting
try:
    from .custom_msg_format import msg_custom_formated
    print("custom message format loaded")
    custom = True
except ImportError:
    custom = False


init(autoreset=True)

class dummy_queue():
    def __init__(self, maxsize=10):
        self.maxsize = maxsize
        self.queue = []

    def put(self, item):
        if len(self.queue) >= self.maxsize:
            self.queue.pop(0)
        self.queue.append(item)

def split_strip(string):
    lstr = string.split(",")
    lstr = [s.strip().lower() for s in lstr]
    return lstr

def append_message(msg, chn, chn_hist, chn_hist_fname):
    if chn_hist.get(chn) is not None:
        chn_hist[chn] = pd.concat([chn_hist[chn], msg.to_frame().transpose()],axis=0, ignore_index=True)
        chn_hist[chn].to_csv(chn_hist_fname[chn], index=False)

class DiscordBot(discord.Client):
    def __init__(
            self,
            queue_prints=dummy_queue(maxsize=10),
            live_quotes=True,
            brokerage=None,
            tracker_portfolio_fname=cfg['portfolio_names']["tracker_portfolio_name"],
            cfg = cfg,
            track_old_alerts=False,
        ):
        super().__init__()
        self.channel_IDS = channel_ids
        self.time_strf = "%Y-%m-%d %H:%M:%S.%f"
        self.queue_prints = queue_prints
        self.bksession = brokerage
        self.live_quotes = live_quotes
        self.cfg = cfg
        self.track_old_alerts = track_old_alerts
        self.lc = {}

        if self.track_old_alerts:
            from DiscordAlertsTrader.marketdata.thetadata_api import ThetaClientAPI
            self.old_trades_config_by_channel = {
                "real-time-alerts": {
                    "mode": "use_message_history_csv",
                    "from_time": "2024-01-01 0:0:0.0", # timezone is the one used in the message history
                },
                "1k-challenge-alerts": {
                    "mode": "use_message_history_csv",
                    "from_time": "2024-01-01 0:0:0.0",
                },
                "team-alerts": {
                    "mode": "use_message_history_csv",
                    "from_time": "2024-01-01 0:0:0.0",
                },
            }
            tracker_portfolio_fname = os.path.join(cfg['general']['data_dir'], "analysts_portfolio_backtesting.csv")
            self.theta_client = ThetaClientAPI()
            # logging config for backtesting
            self.lc = {
                "portfolio": True
            }
            self.live_quotes = False
            self.bksession = brokerage = None

            delete_existing_backtest_portfolio_message_history = True
            if delete_existing_backtest_portfolio_message_history:
                for ch in self.old_trades_config_by_channel.keys():
                    dt_fname = f"{self.cfg['general']['data_dir']}/{ch}_message_history_backtest.csv"
                    if os.path.exists(dt_fname):
                        os.remove(dt_fname)
                        input(f"Deleted {dt_fname}")
                if os.path.exists(tracker_portfolio_fname):
                    os.remove(tracker_portfolio_fname)
                    input(f"Deleted {tracker_portfolio_fname}")

        if brokerage is not None:
            self.trader = AlertsTrader(queue_prints=self.queue_prints, brokerage=brokerage, cfg=self.cfg)
        
        self.tracker = AlertsTracker(brokerage=brokerage, portfolio_fname=tracker_portfolio_fname, cfg=self.cfg, track_old_alerts=self.track_old_alerts, logging_config=self.lc)
        
        self.load_data()

        if ((live_quotes and brokerage is not None and brokerage.name != 'webull')
            or (brokerage is not None and brokerage.name == 'webull'
                and cfg['general'].getboolean('webull_live_quotes'))):
            self.thread_liveq =  threading.Thread(target=self.track_live_quotes)
            self.thread_liveq.start()

    def close_bot(self):
        if self.bksession is not None:
            self.trader.update_portfolio = False
            self.live_quotes = False

    def track_live_quotes(self):
        dir_quotes = self.cfg['general']['data_dir'] + '/live_quotes'
        os.makedirs(dir_quotes, exist_ok=True)

        while self.live_quotes:
            # Skip closed market
            now = datetime.now()
            weekday, hour = now.weekday(), now.hour
            after_hr, before_hr = self.cfg['general']['off_hours'].split(",")
            if  weekday >= 5 or (hour < int(before_hr) or hour >= int(after_hr)):
                time.sleep(60)
                continue

            # get unique symbols  from portfolios, either options or all, open or alerted today
            tk_day = pd.to_datetime(self.tracker.portfolio['Date']).dt.date == date.today()
            td_day = pd.to_datetime(self.trader.portfolio['Date']).dt.date == date.today()
            msk_tk = ((self.tracker.portfolio['isOpen']==1) | tk_day)
            msk_td = ((self.trader.portfolio['isOpen']==1) | td_day)

            if self.cfg['general'].getboolean('live_quotes_options_only'):
                msk_tk = msk_tk & (self.tracker.portfolio['Asset']=='option')
                msk_td = msk_td & (self.trader.portfolio['Asset']=='option')

            track_symb = set(self.tracker.portfolio.loc[msk_tk, 'Symbol'].to_list() + \
                self.trader.portfolio.loc[msk_td, 'Symbol'].to_list())
            if not len(track_symb):
                time.sleep(10)
                continue
            # save quotes to file
            try:
                quote = self.bksession.get_quotes(track_symb)
            except Exception as e:
                print('error during live quote:', e)
                continue
            if quote is None:
                continue

            for q in quote:
                if quote[q]['description'] == 'Symbol not found' or q =='' or quote[q]['bidPrice'] == 0:
                    continue
                timestamp = quote[q]['quoteTimeInLong']//1000  # in ms

                # Read the last line of the file and get the last recorded timestamp for the symbol
                file_path = f"{dir_quotes}/{quote[q]['symbol']}.csv"
                last_line = ""
                do_header = True
                if os.path.exists(file_path):
                    do_header = False
                    with open(file_path, "r") as f:
                        lines = f.readlines()
                        if lines:
                            last_line = lines[-1].strip()

                #if last recorded timestamp is the same as current, skip
                if last_line.split(",")[0] == str(timestamp):
                    continue

                # Write the new line to the file
                with open(file_path, "a+") as f:
                    if do_header:
                        f.write(f"timestamp, quote, quote_ask\n")
                    f.write(f"{timestamp}, {quote[q]['bidPrice']}, {quote[q]['askPrice']}\n")

            # Sleep for up to X secs
            toc = (datetime.now() - now).total_seconds()
            if toc < float(cfg['general']['sampling_rate_quotes']) and self.live_quotes:
                time.sleep(float(cfg['general']['sampling_rate_quotes'])-toc)

    def load_data(self):
        self.chn_hist= {}
        self.chn_hist_fname = {}
        for ch in self.channel_IDS.keys():
            dt_fname = f"{self.cfg['general']['data_dir']}/{ch}_message_history.csv"
            if not os.path.exists(dt_fname):
                ch_dt = pd.DataFrame(columns=self.cfg['col_names']['chan_hist'].split(","))
                ch_dt.to_csv(dt_fname, index=False)
                ch_dt.to_csv(f"{self.cfg['general']['data_dir']}/{ch}_message_history_temp.csv", index=False)
            else:
                ch_dt = pd.read_csv(dt_fname)

            if self.track_old_alerts:
                dt_fname = f"{self.cfg['general']['data_dir']}/{ch}_message_history_backtest.csv"
                ch_dt = pd.DataFrame(columns=self.cfg['col_names']['chan_hist'].split(","))
                ch_dt.to_csv(dt_fname, index=False)
            
            self.chn_hist[ch] = ch_dt
            self.chn_hist_fname[ch] = dt_fname

    async def on_ready(self):
        print('Logged on as', self.user , '\n loading previous messages')
        await self.load_previous_msgs()

    async def on_message(self, message):
        if self.track_old_alerts:
            return
        # only respond to channels in config or authorwise subscription
        author = f"{message.author.name}#{message.author.discriminator}"
        if message.channel.id not in self.channel_IDS.values() and \
            author.lower() not in split_strip(self.cfg['discord']['auhtorwise_subscription']):
            return
        if message.content == 'ping':
            await message.channel.send('pong')

        message = server_formatting(message)
        if custom:
            # await msg_custom_formated2(message)
            alert = msg_custom_formated(message)
            if alert is not None:
                for msg in alert:
                    self.new_msg_acts(msg, False)
                return
        
        if not len(message.content):
            return
        self.new_msg_acts(message)

    # async def on_message_edit(self, before, after):
    #     # Ignore if the message is not from a user or if the bot itself edited the message
    #     if after.channel.id not in self.channel_IDS.values() or  before.author.bot:
    #         return

    #     str_prt = f"Message edited by {before.author}: '{before.content}' -> '{after.content}'"
    #     self.queue_prints.put([str_prt, "black"])
    #     print(Fore.BLUE + str_prt)

    async def load_previous_msgs(self):
        await self.wait_until_ready()
        for ch, ch_id in self.channel_IDS.items():
            channel = self.get_channel(ch_id)
            if channel is None:
                print("channel not found:", ch)
                continue
            print("In", channel)

            if not self.track_old_alerts:
                if len(self.chn_hist[ch]):
                    msg_last = self.chn_hist[ch].iloc[-1]
                    date_After = datetime.strptime(msg_last.Date, self.time_strf)
                    iterator = channel.history(after=date_After, oldest_first=True)
                else:
                    # iterator = channel.history(oldest_first=True)
                    continue

                async for message in iterator:
                    message = server_formatting(message)
                    if message is None:
                        continue
                    if custom:
                        alert = msg_custom_formated(message)
                        if alert is not None:
                            for msg in alert:
                                self.new_msg_acts(msg, False)
                    else:
                        self.new_msg_acts(message)
                if custom:
                    await msg_custom_formated2(message)
            else:
                if not ch in self.old_trades_config_by_channel:
                    continue
                if self.old_trades_config_by_channel[ch]["mode"] == "use_message_history_csv":
                    self.load_previous_msgs_from_csv(ch)

        print("Done")    
        if not self.track_old_alerts:
            self.tracker.close_expired()

    def load_previous_msgs_from_csv(self, ch):
        print(f"Loading previous messages for {ch}")
        ch_dt = pd.read_csv(f"{self.cfg['general']['data_dir']}/{ch}_message_history.csv")
        from_time = datetime.strptime(self.old_trades_config_by_channel[ch]["from_time"], self.time_strf)
        # convert 'Date' column to datetime using same format as time_strf
        ch_dt['Date_ts'] = pd.to_datetime(ch_dt['Date'], format=self.time_strf)
        ch_dt = ch_dt[ch_dt['Date_ts'] > from_time]
        # sort by date in ascending order
        ch_dt = ch_dt.sort_values(by='Date_ts', ascending=True)
        for i in range(len(ch_dt)):
            message = ch_dt.iloc[i]
            message = pd.Series(message)
            self.new_msg_acts(message, from_disc=False)

    def new_msg_acts(self, message, from_disc=True):
        if from_disc:
            msg_date = message.created_at.replace(tzinfo=timezone.utc).astimezone(tz=None)
            msg_date_f = msg_date.strftime(self.time_strf)
            if message.channel.id in self.channel_IDS.values():
                chn_ix = list(self.channel_IDS.values()).index(message.channel.id)
                chn = list(self.channel_IDS.keys())[chn_ix]
            else:
                chn = None
            msg = pd.Series({'AuthorID': message.author.id,
                            'Author': f"{message.author.name}#{message.author.discriminator}".replace("#0", ""),
                            'Date': msg_date_f,
                            'Content': message.content,
                            'Channel': chn
                            })
        else:
            msg = message
        chn = msg['Channel']
        shrt_date = datetime.strptime(msg["Date"], self.time_strf).strftime('%Y-%m-%d %H:%M:%S')
        self.queue_prints.put([f"\n{shrt_date} {msg['Channel']}: \n\t{msg['Author']}: {msg['Content']} ", "blue"])
        print(Fore.BLUE + f"{shrt_date} \t {msg['Author']}: {msg['Content']} ")

        pars, order =  parse_trade_alert(str(msg['Content']))
        if pars is None:
            if self.chn_hist.get(chn) is not None:
                msg['Parsed'] = ""
                self.chn_hist[chn] = pd.concat([self.chn_hist[chn], msg.to_frame().transpose()],axis=0, ignore_index=True)
                self.chn_hist[chn].to_csv(self.chn_hist_fname[chn], index=False)
            return
        else:
            if order['asset'] == "option":
                try:
                    # get option date with year (get year from msg if missing)
                    year = datetime.strptime(msg["Date"], self.time_strf).year
                    if len(order['expDate'].split("/")) ==2:
                        exp_dt = datetime.strptime(f"{order['expDate']}/{year}" , "%m/%d/%Y").date()
                    else:
                        if len(order['expDate'].split("/")[-1]) == 2:
                            exp_dt = datetime.strptime(f"{order['expDate']}" , "%m/%d/%y").date()
                        else:
                            exp_dt = datetime.strptime(f"{order['expDate']}", "%m/%d/%Y").date()
                except ValueError:
                    str_msg = f"Option date is wrong: {order['expDate']}"
                    self.queue_prints.put([f"\t {str_msg}", "red"])
                    print(Fore.RED + f"\t {str_msg}")
                    msg['Parsed'] = str_msg
                    if self.chn_hist.get(chn) is not None:
                        self.chn_hist[chn] = pd.concat([self.chn_hist[chn], msg.to_frame().transpose()],axis=0, ignore_index=True)
                        self.chn_hist[chn].to_csv(self.chn_hist_fname[chn], index=False)
                    return


                dt = datetime.now().date()
                if self.track_old_alerts:
                    dt = datetime.strptime(msg["Date"], self.time_strf).date()
                order['dte'] =  (exp_dt - dt).days
                if order['dte']<0:
                    str_msg = f"Option date in the past: {order['expDate']}"
                    self.queue_prints.put([f"\t {str_msg}", "red"])
                    print(Fore.RED + f"\t {str_msg}")
                    msg['Parsed'] = str_msg
                    if self.chn_hist.get(chn) is not None:
                        self.chn_hist[chn] = pd.concat([self.chn_hist[chn], msg.to_frame().transpose()],axis=0, ignore_index=True)
                        self.chn_hist[chn].to_csv(self.chn_hist_fname[chn], index=False)
                    return

            order['Trader'], order["Date"] = msg['Author'], msg["Date"]
            order_date = datetime.strptime(order["Date"], self.time_strf)
            date_diff = abs(datetime.now() - order_date)
            print(f"time difference is {date_diff.total_seconds()}")

            live_alert = True if date_diff.seconds < 90 else False
            str_msg = pars
            if ((live_alert and self.bksession is not None) or self.track_old_alerts) and (order.get('price') is not None):
                if self.track_old_alerts:
                    # get timestamp of message in market time along with added buffer
                    # todo: this assumes python 3.6 and that the market tz is NY
                    market_tz = pytz.timezone('US/Eastern')
                    order_date_market = order_date.astimezone(market_tz)
                    order_time = order_date_market + pd.Timedelta(seconds=2)
                    order_time = order_time.timestamp()
                    quote, time_diff = self.theta_client.get_price_at_time(order['Symbol'], order_time, order["action"])
                    if quote == -1:
                        str_msg = f"No price found, skipping alert"
                        self.queue_prints.put([f"\t {str_msg}", "red"])
                        print(Fore.RED + f"\t {str_msg}")
                        msg['Parsed'] = str_msg
                        append_message(msg, chn, self.chn_hist, self.chn_hist_fname)
                        return
                    if time_diff > 60:
                        str_msg = f"No price found for next 60s, skipping alert"
                        self.queue_prints.put([f"\t {str_msg}", "red"])
                        print(Fore.RED + f"\t {str_msg}")
                        msg['Parsed'] = str_msg
                        append_message(msg, chn, self.chn_hist, self.chn_hist_fname)
                        return
                else:
                    quote = self.trader.price_now(order['Symbol'], order["action"], pflag=1)
                act_diff = -1
                if quote:
                    if quote > 0:
                        order['price_actual'] = quote
                    if order['price'] == 0:
                        str_msg = f"ALerted price is 0, skipping alert "
                        self.queue_prints.put([f"\t {str_msg}", "red"])
                        print(Fore.RED + f"\t {str_msg}")
                        return
                    act_diff = max(((quote - order['price'])/order['price']), (order['price'] - quote)/ quote)
                    # Check if actual price is too far (100) from alerted price
                    if abs(act_diff) > 1 and order.get('action') == 'BTO':
                        str_msg = f"Alerted price is {act_diff} times larger than current price of {quote}, skipping alert"
                        self.queue_prints.put([f"\t {str_msg}", "red"])
                        print(Fore.RED + f"\t {str_msg}")
                        msg['Parsed'] = str_msg
                        if self.chn_hist.get(chn) is not None:
                            self.chn_hist[chn] = pd.concat([self.chn_hist[chn], msg.to_frame().transpose()],axis=0, ignore_index=True)
                            self.chn_hist[chn].to_csv(self.chn_hist_fname[chn], index=False)
                        return

                str_msg += f" Actual:{quote}, diff {round(act_diff*100)}%"
            self.queue_prints.put([f"\t {str_msg}", "green"])
            print(Fore.GREEN + f"\t {str_msg}")
            #Tracker
            if chn != "GUI_user":
                track_out = self.tracker.trade_alert(order, live_alert, chn)
                self.queue_prints.put([f"{track_out}", "red"])
                print(f"{Fore.GREEN} {track_out}")
            # Trader
            do_trade, order = self.do_trade_alert(msg['Author'], msg['Channel'], order)
            if do_trade and date_diff.seconds < 120:
                order["Trader"] = msg['Author']
                self.trader.new_trade_alert(order, pars, msg['Content'])

        if self.chn_hist.get(chn) is not None:
            msg['Parsed'] = pars
            self.chn_hist[chn] = pd.concat([self.chn_hist[chn], msg.to_frame().transpose()],axis=0, ignore_index=True)
            self.chn_hist[chn].to_csv(self.chn_hist_fname[chn], index=False)

    def do_trade_alert(self, author, channel, order):
        "Decide if alert should be traded"
        if self.bksession is None or channel == "GUI_analysts":
            return False, order

        # in authors subs list or channel subs list
        if author.lower() in split_strip(self.cfg['discord']['authors_subscribed']) or \
            channel.lower() in split_strip(self.cfg['discord']['channelwise_subscription']):
            # ignore if no STC
            if not self.cfg['general'].getboolean('DO_STC_TRADES') and order['action'] == "STC" \
            and channel not in ["GUI_user", "GUI_both"]:
                str_msg = f"STC not accepted by config options: DO_STC_TRADES = False"
                print(Fore.GREEN + str_msg)
                self.queue_prints.put([str_msg, "", "green"])
                return False, order
            else:
                if order['action'] == "BTO" and order['asset'] == 'option':
                    min_price = cfg['order_configs']['min_opt_price']
                    if len(min_price) and order['price'] *100 < float(cfg['order_configs']['min_opt_price']):
                        str_msg = f"Option price is too small as per config: {order['price']}"
                        print(Fore.GREEN + str_msg)
                        self.queue_prints.put([str_msg, "", "green"])
                        return False, order
                return True, order

        # in authors shorting list
        elif author.lower() in split_strip(self.cfg['shorting']['authors_subscribed']):
            if order['asset'] != "option":
                return False, order
            # BTC order sent manullay from gui
            if order["action"] in ["BTC", "STO"] and channel in ["GUI_user", "GUI_both"]:
                return True, order
            # Make it shorting order
            order["action"] = "STO" if order["action"] == "BTO" else "BTC" if order["action"] == "STC" else order["action"]
            # reject if cfg do BTO or STC is false
            if (order["action"] == "BTC" and not self.cfg['shorting'].getboolean('DO_BTC_TRADES')) \
                or (order["action"] == "STO" and not self.cfg['shorting'].getboolean('DO_STO_TRADES')):
                return False, order
            if len(self.cfg['shorting']['max_dte']):
                if order['dte'] <= int(self.cfg['shorting']['max_dte']):
                    return True, order

        return False, order

def run_in_backtest_mode():
    from DiscordAlertsTrader.configurator import cfg

    client = DiscordBot(brokerage=None, cfg=cfg, track_old_alerts=True)
    client.login(cfg['discord']['discord_token']) # use this if you need to fetch old messages
    client.load_previous_msgs()

if __name__ == '__main__':
    # from DiscordAlertsTrader.configurator import cfg, channel_ids
    # from DiscordAlertsTrader.brokerages import get_brokerage

    # bksession = get_brokerage()
    # client = DiscordBot(brokerage=None, cfg=cfg, track_old_alerts=True)
    # client.run(cfg['discord']['discord_token'])

    run_in_backtest_mode()