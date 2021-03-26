#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 17:47:06 2021

@author: adonay
"""

import os
import numpy as np
import subprocess
import time 
import re
import pandas as pd
from datetime import datetime, timedelta
from message_parser import parser_alerts
from option_message_parser import option_alerts_parser
from config import (path_dll, data_dir, CHN_NAMES, channel_IDS, discord_token, UPDATE_PERIOD)
import config as cfg
from disc_trader import AlertTrader
import threading
from colorama import Fore, Back, Style, init
import itertools
import json 

init(autoreset=True)

def updt_chan_hist(df_hist, path_update, path_hist):
    
    last_date = df_hist['Date'].max()
    
    new_msg = pd.read_csv(path_update)
    new_msg = new_msg.loc[new_msg['Date']>last_date]
    
    df_hist = df_hist.append(new_msg, ignore_index=True)
    df_hist.to_csv(path_hist, index=False)
    return df_hist, new_msg


def update_edited_msg(df_hist, json_msg):
    
    msg_old = []
    for jmsg in json_msg:
        inx, = np.where(df_hist['Date']== jmsg['timestamp'])
        if df_hist.loc[inx, 'Content'].values == jmsg['content']:
            continue
        msg_old.append((inx, df_hist.loc[inx, 'Content']))
        df_hist.loc[inx, 'Content']  = jmsg['content']

    return df_hist, msg_old


def msg_update_alert(df_hist, json_msg, asset):
    
    df_hist, msg_old = update_edited_msg(df_hist, json_msg)
    
    if msg_old == []:
        return None

    if asset == "stock":
        parser =  parser_alerts
    else:
         parser =  option_alerts_parser

    new_alerts=[]
    for msg in msg_old:
        
        _, order_old =  parser(msg[1])
        msg_content = df_hist.loc[msg[0], "Content"]
        pars, order_upd = parser(msg_content)
        
        if order_old == order_upd:
            continue
        
        ex_old = [order_old[f"PT{i}"] for i in range(1,4)]
        ex_upd = [order_old[f"PT{i}"] for i in range(1,4)]
        
        if ex_old != ex_upd:
            order_upd['action'] = "ExitUpdate"
            pars.replace("BTO", "ExitUpdate")
            new_alerts.append([pars,order_upd, msg_content])
        else:
            raise NotImplementedError ("Check what to do with msg update")
            
    return new_alerts, msg_old



def closest_fullname_match(name, names_all):
    """Match first substring in a list from a list of strings,
    eg, name = ["Name"]
    """
    
    if name is None:
        return name
       
    candidate = [ n for n in names_all if name[0] in n.lower()]
    
    if candidate == []:
        "print name not matched"
        return None
    
    # df unique returned in order of appearence
    # if candidate > 1, return last most recent
    return candidate[-1]
    
    
def dm_message(msg, names_all):
    
    if "author:" in msg or "A:" in msg:
        re_author = re.compile("(?:author|A):[ ]?(\w+)")
        auth_inf = re_author.search(msg)
        
        author = closest_fullname_match(auth_inf.groups(), names_all)
        
        re_author = re.compile("ms:[ ]?(\w+)")
                
        content = msg[auth_inf.span()[1]+1:]
        
    else:
        author = "Me"
        
        content = msg
    
    if "asset:" in msg or "AS:" in msg:        
        re_ass = re.compile("(?:asset|AS):[ ]?(\w+)")
        ass_inf = re_ass.search(msg)
        asset = ass_inf.groups()[0]
    else:
        asset = None
        
    return author, asset, content
    

def send_sh_cmd(cmd):
    "takes command string, returns true if no error"
    spro = subprocess.Popen(cmd, shell=True, 
                            stderr=subprocess.PIPE, 
                            stdout=subprocess.PIPE
                            )        
    # Capture if read new messages 
    spro_err = str(spro.communicate()[1])

    return False if "ERROR" in spro_err else True

def read_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def disc_json_time_corr(time_json):
    """ Mesages from json are 4hs forward
    
        Original format: '2021-03-19T18:31:01.609+00:00'
        output: dateime object - 4hs !"""
        
    date = datetime.strptime(time_json.split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
    
    return  date + timedelta(hours=-4)


class AlertsListner():
    
    def __init__(self):

        self.UPDATE_PERIOD = cfg.UPDATE_PERIOD
        self.CHN_NAMES = cfg.CHN_NAMES
                
        self.cmd = f'dotnet {path_dll} export' + ' -c {} ' + \
            f'  -t {discord_token}' + \
                ' -f Csv --after "{}" --dateformat "yyyy-MM-dd HH:mm:ss.ffffff" -o {}'
                
        self.time_strf = "%Y-%m-%d %H:%M:%S.%f"
                
        self.Altrader = AlertTrader()
        
        self.listening = False
        
        # self.thread =  threading.Thread(target=self.listent_trade_alerts)
        # self.thread.start()


    def close(self):
        self.Altrader.update_portfolio = False


    def get_edited_msgs(self, chn_id, time_after_last, out_file,  hours=1):
        
        out_json = out_file.replace("csv", "json")

        date = time_after_last - timedelta(hours=hours)
        date = date.strftime(self.time_strf)
        
        cmd = self.cmd.format( chn_id, date, out_json)
        cmd = cmd.replace("Csv", "Json")
        
        if not send_sh_cmd(cmd):
            return False
        
        data = read_json(out_json)["messages"]
        msg_edit = []
        for msg in data:
            if msg['timestampEdited']:
                aux = disc_json_time_corr(msg['timestamp'])
                msg['timestamp'] = aux.strftime(self.time_strf)
                msg_edit.append(msg)

        return msg_edit


    def listent_trade_alerts(self):
        
        self.listening = True 
        
        chn_hist_f = {c:f"{data_dir}/{c}_message_history.csv" for c in self.CHN_NAMES}
        chn_hist = {c: pd.read_csv(chn_hist_f[c]) for c in self.CHN_NAMES}
        
        time_strf = self.time_strf
        
        time_after = [None]*len(self.CHN_NAMES)
        
        last_time = [d['Date'].max() for d in chn_hist.values()]
        last_time = [datetime.strptime(d, time_strf) for d in last_time]
        
        while self.listening:

            tic = datetime.now()
            for chn_i in range(len(self.CHN_NAMES)):  
                chn_name = self.CHN_NAMES[chn_i]
                
                out_file = f"{data_dir}/{chn_name}_temp.csv"
                        
                time_diff = (datetime.now() - last_time[chn_i]) 
                time_after[chn_i] = (datetime.now() - time_diff).strftime(time_strf)  
                
                cmd_sh = self.cmd.format(channel_IDS[chn_name],
                                    time_after[chn_i], 
                                    out_file
                                    )
                new_msgs = send_sh_cmd(cmd_sh)

                if new_msgs:
                    
                    last_time[chn_i] = datetime.now()
        
                    df_update, new_msg = updt_chan_hist(chn_hist[chn_name], 
                                               out_file, chn_hist_f[chn_name])
                    
                    chn_hist[chn_name] = df_update
                    nmsg = len(new_msg)
                    if nmsg:
                        print(Style.DIM + f"{last_time[chn_i]} | {chn_name}: got {nmsg} new msgs:")
                    
                    # Loop over new msg and take action
                    for ix, msg in new_msg.iterrows():

                        if msg['Author'] != "Xcapture#0190" and not pd.isnull(msg['Content']):  
                            
                            if chn_name.split("_")[0] == "stock":
                                asset = "stock"
                            elif chn_name.split("_")[0] == "option":
                                asset = "option"
                            else:
                                asset = None
                                
                            if chn_name == 'DM_xcapture': 
                                # Get authors names from non-DM servers
                                names_all = [chn_hist[n]['Author'].unique() for n in self.CHN_NAMES if n[:2] != "DM"]
                                names_all = list(itertools.chain(*names_all))                            
                                                                              
                                author, asset, content = dm_message(msg["Content"], names_all)
                                print("DM: ", author, asset, content)                                
                                msg['Author'] = author
                                msg["Content"] = content
                            
                            shrt_date = datetime.strptime(msg["Date"], time_strf
                                                          ).strftime('%H:%M:%S')
                            print(f"{shrt_date} \t {msg['Author']}: {msg['Content']} ")


                            if asset == "stock":
                                pars, order =  parser_alerts(msg['Content'])
                            elif asset == "option":
                                pars, order =  option_alerts_parser(msg['Content'])


                            if pars is None:
                                # Check if msg alerting exitUpdate in prev msg
                                re_upd = re.compile("trade plan (?:.*?)\*\*([A-Z]*?)\*\*(?:.*?) updated")
                                upd_inf = re_upd.search(msg['Content'])
                                if upd_inf is None:
                                    print(Style.DIM + "\t \t MSG NOT UNDERSTOOD")
                                    continue

                                json_msg = self.get_edited_msgs(
                                    channel_IDS[chn_name], time_after[chn_i],
                                    out_file)

                                new_alerts, _ = msg_update_alert(chn_hist[chn_name], json_msg, asset)

                                for alert in new_alerts:
                                    self.Altrader.new_trade_alert(alert[0], alert[1], alert[2])
                                    print(f"Updated exit plan: \n \t {alert[0]}")

                                    
                            elif pars == 'not an alert':
                                print(Style.DIM + "\t \tnot for @everyone")
                                
                            else:
                                print(Fore.RED +f"\t \t {pars}")
                                
                                if msg['Author'] == "Kevin (Momentum)#8888":
                                    msg['Author'] =  "Kevin (Momentum)#4441"
                                    
                                if msg['Author'] in [ "ScaredShirtless#0001", "Kevin (Momentum)#4441"]:
                                    order["Trader"] = msg['Author']
                                    self.Altrader.new_trade_alert(order, pars,\
                                         msg['Content'])
            
            toc = datetime.now() 
            tictoc = (toc-tic).total_seconds()
        
            # wait UPDATE_PERIOD
            if tictoc < self.UPDATE_PERIOD:
                time.sleep(min(self.UPDATE_PERIOD-tictoc, self.UPDATE_PERIOD))


alistner = AlertsListner()
alistner.listent_trade_alerts()

SL_2buyprice = ['Move SL to buy price',]