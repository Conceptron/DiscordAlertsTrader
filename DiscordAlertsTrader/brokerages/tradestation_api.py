from datetime import datetime
import json
import pandas as pd

from DiscordAlertsTrader.configurator import cfg
from DiscordAlertsTrader.brokerages.tradestation import auth as tsa
from DiscordAlertsTrader.brokerages import BaseBroker


class TS(BaseBroker):
    def __init__(self, accountId=None):
        """
        accountId: id of the account
        """
        self.name = 'ts'
        self.accountId = accountId

    def get_session(self):       
        if len(cfg['tradestation']['client_secret']) < 10:
            raise ValueError( "No TradeStation secret key found, fill it in the config.ini file")
        # Create a new session
        self.session = tsa.easy_client(cfg['tradestation']['client_id'], 
                           cfg['tradestation']['client_secret'], 
                           cfg['tradestation']['redirect_url'], 
                           paper_trade=cfg['tradestation']['papertrade'])
        
        if self.accountId is None:
            resp = self.session.get_accounts('adonunes12').json()
            self.accountId, = [ k['Name'] for k in resp if k['TypeDescription'] == cfg['tradestation']['acct_type']]
            
        success = self.session._logged_in
        if success:
            print("Logged in TradeStation successfully")
        else:
            print("Failed to log in TradeStation")
        return success

    def get_quotes(self, symbol:list):
        symbol = [self._convert_option_tots(s) for s in symbol]
        
        resp = self.session.get_quote_snapshots(symbol).json()
        
        quotes = {}
        for quote in resp['Quotes']:
            ticker = self._convert_option_fromts(quote['Symbol'])
            
            timestmp = datetime.fromisoformat(quote.get("TradeTime").replace('Z', '+00:00'))       
            quoteTimeInLong = timestmp.timestamp()*1000
        
            quotes[ticker] = {
                            'symbol' : ticker,
                            'description': "",
                            'askPrice': float(quote.get("Ask")),  
                            'bidPrice': float(quote.get("Bid")),    
                            'quoteTimeInLong': quoteTimeInLong,
                            "status": ''
                            }
        return quotes
    
    def _convert_option_fromts(self, ticker):
        """
        Convert ticker from 'SPX yearmonthdayC110' to 'SPX_monthdayyearC110' format.
        
        Parameters:
        ticker (str): Ticker in the original format.
        
        Returns:
        str: Ticker in the desired format.
        """
        symb, date_part = ticker.split()  # Split the ticker by spaces
        formatted_date = date_part[2:6] +date_part[:2]   # Reformat the date part
        return f"{symb}_{formatted_date}{date_part[6:]}"  # Combine the parts in the desired format
    
    def _convert_option_tots(self, ticker):
            """
            Convert ticker from 'SPX_monthdayyearC110' to 'SPX yearmonthdayC110' format.
            
            Parameters:
            ticker (str): Ticker in the original format.
            
            Returns:
            str: Ticker in the desired format.
            """
            symb, date_part = ticker.split("_")  # Split the ticker by spaces
            formatted_date = date_part[4:6] + date_part[:4]   # Reformat the date part
            return f"{symb} {formatted_date}{date_part[6:]}"  # Combine the parts in the desired format
        
    def get_orders(self):
        pass

    def get_order_status(self, order_id):
        pass
    
    def get_account_info(self):        
        resp = self.session.get_balances([self.accountId]).json()
        
        acc_inf ={
            'securitiesAccount':{   
                'positions':[],
                'accountId' : str(self.accountId),
                'currentBalances':{
                    'liquidationValue': resp['Balances'][0]['Equity'],    
                    'cashBalance': resp['Balances'][0]['CashBalance'],
                    'availableFunds': resp['Balances'][0]['CashBalance'],  
                    },
        }}

        # get positions of the account
        positions, _ = self.get_positions_orders()
        for _, pos in positions.iterrows():
            pos_d = {
                "longQuantity" : pos['Qty'], 
                "symbol": pos['symbol'],       
                "marketValue": pos['marketValue'], 
                "assetType": pos['asset'],   
                "averagePrice":pos['Avg Price'],    
                "currentDayProfitLoss": pos['PnL'], 
                "currentDayProfitLossPercentage": pos['PnL %'], 
                'instrument': {'symbol': pos['symbol'],   
                                'assetType': pos['asset'],     
                                }
                }            
            acc_inf['securitiesAccount']['positions'].append(pos_d)

        # checks if account has no open pos   
        if not len(positions):
            acc_inf['securitiesAccount']['positions'] = []
            print("No portfolio")

        # get orders and add them to acc_inf
        orders = self.session.get_orders([self.accountId]).json()
        orders_inf =[]  
        for order in orders['Orders']:
            order_status = order['StatusDescription'].upper()
            if order_status in ['CANCELLED', 'REJECTED']:
                continue
            orders_inf.append(self.format_order(order))
        acc_inf['securitiesAccount']['orderStrategies'] = orders_inf
        return acc_inf

    def get_order_info(self, order_id):  
        """
        order_status = 'REJECTED' | "FILLED" | "WORKING"
        """   
        if not isinstance(order_id, str):
            order_id = str(int(order_id))        
        order_info = self.session.get_orders([self.accountId], order_ids=[order_id]).json()
        if order_info.get('Error'):
            print(order_info)
        
        if len(order_info['Orders']) > 1: # bracket orders
            ix = [i for i, order in enumerate(order_info['Orders']) if order['StatusDescription'].upper() == 'FILLED']
            ix = 0 if len(ix) == 0 else ix[0]
            order_info = self.format_order(order_info['Orders'][ix])
            order_info['order_id'] = f"{order_info['order_id']},{order_info['order_id']}"   
        else:
            order_info = self.format_order(order_info['Orders'][0])
        order_status = order_info['status']
        return order_status, order_info

    def format_order(self, order:dict):
        """ output format for order_response. Order, mimicks the order_info from TDA API"""

        price = order.get('FilledPrice', '0')
        if price == '0':
            price = order.get('LimitPrice','0')
            if price == '0':
                price = order.get('StopPrice','0')

        timestmp = datetime.fromisoformat(order['OpenedDateTime'].replace('Z', '+00:00'))       
        enteredTime = datetime.fromtimestamp(timestmp.timestamp()).strftime("%Y-%m-%dT%H:%M:%S+00")
        if order.get("ClosedDateTime") is not None:
            timestmp = datetime.fromisoformat(order['ClosedDateTime'].replace('Z', '+00:00'))
        closeTime = datetime.fromtimestamp(timestmp.timestamp()).strftime("%Y-%m-%dT%H:%M:%S+00")

        symbol = order['Legs'][0]['Symbol']
        if order['Legs'][0]['AssetType'] == 'STOCKOPTION':
            symbol = self._convert_option_fromts(symbol)
            
        if order.get('ConditionalOrders') is not None:
            orderStrategyType = 'OCO'
        else:
            orderStrategyType = 'SINGLE'
            
        order_info = {
            'status': order['StatusDescription'].replace('Received', 'WORKING').upper(),
            'quantity': int(order['Legs'][0]['QuantityOrdered']),
            'filledQuantity': abs(int(order['Legs'][0]['ExecQuantity'])),
            'price': float(price) if price else float(order['Legs'][0]['ExecutionPrice']),
            'orderStrategyType': orderStrategyType,
            "order_id" : order['OrderID'], #
            "orderId": order['OrderID'], #
            "stopPrice": order.get('stpPrice'),
            'orderType':  order['OrderType'], #
            'enteredTime': enteredTime, #
            "closeTime": closeTime, #
            'orderLegCollection':[{
                'instrument':{'symbol': symbol},
                'instruction': order['Legs'][0]['BuyOrSell'].upper(),
                'quantity':  abs(int(order['Legs'][0]['ExecQuantity'])),
            }]             
        }    
        return order_info

    def get_positions_orders(self):
        positions = self.session.get_positions([self.accountId]).json()
        
        df_pos = pd.DataFrame(columns=["symbol", "asset", "type", "Qty", "Avg Price", "PnL", "PnL %"])
        for pos in positions['Positions']:
            pos_inf = {
                "symbol":pos["Symbol"],
                "asset":pos["AssetType"],
                "type": pos["LongShort"],
                "Avg Price": pos['AveragePrice'],
                "PnL": pos["UnrealizedProfitLoss"],
                "Qty": eval(pos["Quantity"]),
                "PnL %": pos["UnrealizedProfitLossPercent"],
                "marketValue": pos['MarketValue'],
                }
            df_pos =pd.concat([df_pos, pd.DataFrame.from_records(pos_inf, index=[0])], ignore_index=True)

        df_ordr = pd.DataFrame(columns=["symbol", "asset", "type", "Qty",
                                        "Price", "action"])
        return df_pos, df_ordr

    def send_order(self, new_order):  
        if new_order.get("Type") is not None:
            resp = self.session.place_group_order(new_order).json()
        else:
            resp = self.session.place_order(new_order).json()
            
        print(resp)
        if resp.get('Errors'):
            return None, None
        if len(resp['Orders']) > 1:
            order_id = f"{resp['Orders'][0]['OrderID']},{resp['Orders'][1]['OrderID']}"
        else:
            order_id = resp['Orders'][0]['OrderID']
        order_info = self.session.get_orders([self.accountId], order_ids=[order_id]).json()
        return order_info, order_id

    def cancel_order(self, order_id):
        if not isinstance(order_id, str):
            order_id = str(int(order_id))
        for ordid in str(order_id).split(','):
            resp = self.session.cancel_order(str(ordid)).json()
        print(resp)
        return resp
    
    def make_BTO_lim_order(self, Symbol:str, Qty:int, price:float, action="BTO", **kwarg):
        # if trailing stop in STO, do a STO with trailstop
        if action == 'STO' and "trail_stop_const" in kwarg:
            print("STO with trail_stop_const")
            return self.make_STC_SL_trailstop(Symbol, Qty, action=action, **kwarg)

        new_order ={
            "AccountID": self.accountId,            
            "OrderType": "Limit",
            "TimeInForce": {"Duration": 'GTC'},
            'LimitPrice': str(price),
            'Quantity': str(Qty),
            "Route": "Intelligent",
            }
        if len(Symbol.split("_")) > 1:
            new_order["Symbol"] = self._convert_option_tots(Symbol)
            if action == "BTO":
                new_order['TradeAction'] = "BUYTOOPEN"
            elif action == "STO":
                new_order['TradeAction'] = "SELLTOOPEN"
        else:
            new_order["Symbol"] = Symbol
            if action == "BTO":
                new_order['TradeAction'] = "BUY"
            elif action == "STO":
                new_order['TradeAction'] = "SELLSHORT"
            
        return new_order

    def make_Lim_SL_order(self, Symbol:str, Qty:int,  PT:float, SL:float, 
                            action="STC", **kwarg):        
        if len(Symbol.split("_")) > 1:
            Symbol = self._convert_option_tots(Symbol)
            if action == "STC":
                action_name = "SELLTOCLOSE"
            elif action == "BTC":
                action_name = "BUYTOCLOSE"
        else:            
            if action == "STC":
                action_name = "SELL"
            elif action == "BTC":
                action_name = "BUYTOCOVER"
            
        order ={
            "Type": "BRK",
            "Orders":[
                    {"AccountID": self.accountId,
                    "Symbol": Symbol,
                    "Quantity": str(Qty),
                    "OrderType": "Limit",
                    "TradeAction": action_name,
                    'LimitPrice' : str(PT),
                    "TimeInForce": {"Duration": "GTC"},
                    "Route": "Intelligent",
                    },
                    {"AccountID": self.accountId,
                    "Symbol": Symbol,
                    "Quantity": str(Qty),
                    "OrderType": "StopMarket",
                    "TradeAction": action_name,
                    'StopPrice' : str(SL),
                    "TimeInForce": {"Duration": "GTC"},
                    "Route": "Intelligent",
                    }
                ]
            }
        return order

    def make_STC_lim(self, Symbol:str, Qty:int, price:float, action="STC", **kwarg):
        if len(Symbol.split("_")) > 1:
            Symbol = self._convert_option_tots(Symbol)
            if action == "STC":
                action_name = "SELLTOCLOSE"
            elif action == "BTC":
                action_name = "BUYTOCLOSE"
        else:            
            if action == "STC":
                action_name = "SELL"
            elif action == "BTC":
                action_name = "BUYTOCOVER"
                
        order = {"AccountID": self.accountId,
                "Symbol": Symbol,
                "Quantity": str(Qty),
                "OrderType": "Limit",
                "TradeAction": action_name,
                'LimitPrice' : str(price),
                "TimeInForce": {"Duration": "GTC"},
                "Route": "Intelligent",
                }
        return order

    def make_STC_SL(self, Symbol:str, Qty:int, SL:float, action="STC", **kwarg):
        if len(Symbol.split("_")) > 1:
            Symbol = self._convert_option_tots(Symbol)
            if action == "STC":
                action_name = "SELLTOCLOSE"
            elif action == "BTC":
                action_name = "BUYTOCLOSE"
        else:            
            if action == "STC":
                action_name = "SELL"
            elif action == "BTC":
                action_name = "BUYTOCOVER"
                
        order = {"AccountID": self.accountId,
                "Symbol": Symbol,
                "Quantity": str(Qty),
                "OrderType": "StopMarket",
                "TradeAction": action_name,
                'StopPrice' : str(SL),
                "TimeInForce": {"Duration": "GTC"},
                "Route": "Intelligent",
                }
        return order

    def make_STC_SL_trailstop(self, Symbol:str, Qty:int,  trail_stop_const:float, action="STC", **kwarg):
        if len(Symbol.split("_")) > 1:
            Symbol = self._convert_option_tots(Symbol)
            if action == "STC":
                action_name = "SELLTOCLOSE"
            elif action == "BTC":
                action_name = "BUYTOCLOSE"
            elif action == "STO":
                action_name = "SELLTOOPEN"
        else:            
            if action == "STC":
                action_name = "SELL"
            elif action == "BTC":
                action_name = "BUYTOCOVER"
            elif action == "STO":
                action_name = "SELLSHORT"
                
        order = {"AccountID": self.accountId,
                "Symbol": Symbol,
                "Quantity": str(Qty),
                "OrderType": "StopMarket",
                "TradeAction": action_name,
                "TimeInForce": {"Duration": "GTC"},
                "Route": "Intelligent",
                "AdvancedOptions": {"TrailingStop": {"Amount": str(abs(trail_stop_const))}}
                }
        return order
        
