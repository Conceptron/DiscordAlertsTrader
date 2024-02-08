import json
import asyncio
import re
import time
from tastytrade import ProductionSession
from tastytrade import instruments
from tastytrade import utils
from tastytrade import DXLinkStreamer
from tastytrade.dxfeed import EventType
from DiscordAlertsTrader.configurator import cfg
import os


class TastytradeAPI:
    def __init__(self):
        self.token_path = os.path.join(cfg['root']['dir'], cfg['tastytrade']['token_fname'])
        self.username = cfg['tastytrade']['username']
        self.password = cfg['tastytrade']['password']
        self.valid_symbols = {}


    def get_session(self):
        if os.path.exists(self.token_path):
            with open(self.token_path, 'r') as f:
                remember_token = json.load(f)
            try:
                self.session = ProductionSession(self.username, remember_token=remember_token)
            except utils.TastytradeError as e:
                print("Error: ", e)
                print("Remember token is invalid. Logging in with password...")
                self.session = ProductionSession(self.username, self.password, remember_me=True)
            if self.session.remember_token is not None:
                remember_token = self.session.remember_token
                with open(self.token_path, 'w') as f:
                    json.dump(remember_token, f)
            return self.session

        self.session = ProductionSession(self.username, self.password, remember_me=True)
        remember_token = self.session.remember_token
        with open(self.token_path, 'w') as f:
            json.dump(remember_token, f)
        return self.session

    def map_option_symbol(self, symbol: str, occ_format=False) -> str:
        """
        From ticker_monthdayyear[callput]strike to .tickeryearmonthday[call|put]strike
        """
        match = re.search(r'(\w+)_(\d{2})(\d{2})(\d{2})([CP])([\d.]+)', symbol, re.IGNORECASE)
        if match:
            ticker, month, day, year, callput, strike = match.groups()
            if occ_format:
                strike_price = f"{int(float(strike) * 1000):08d}"
                return f"{ticker.ljust(6)}{year}{month}{day}{callput}{strike_price}"


            return f".{ticker}{year}{month}{day}{callput}{strike}"

    def get_option_info(self, symbol: str) -> dict:
        if symbol in self.valid_symbols:
            return self.valid_symbols[symbol]
        option_symbol = self.map_option_symbol(symbol, occ_format=True)
        try:
            option = instruments.Option.get_option(self.session, option_symbol)
            self.valid_symbols[symbol] = option
            return option
        except Exception as e:
            print(f"Error getting option info for {symbol}: ", e)
            return None

    def get_symbol_info(self, symbol: str) -> dict:
        if symbol in self.valid_symbols:
            return self.valid_symbols[symbol]
        try:
            if '_' in symbol:
                option_info = self.get_option_info(symbol)
                return option_info

            equity = instruments.Equity.get_equity(self.session, symbol)
            self.valid_symbols[symbol] = equity
            return equity
        except Exception as e:
            print(f"Error getting symbol info for {symbol}: ", e)
            return None

    async def get_quotes_async(self, symbols: list) -> dict:
        symbol_map = {}
        mapped_symbols = []
        for symbol in symbols:
            if "_" in symbol:
                option_symbol = self.map_option_symbol(symbol)
                symbol_map[option_symbol] = symbol
                mapped_symbols.append(option_symbol)
            else:
                symbol_map[symbol] = symbol
                mapped_symbols.append(symbol)

        streamer = await DXLinkStreamer.create(self.session)
        await streamer.subscribe(EventType.QUOTE, mapped_symbols)
        quotes = {}
        async for quote in streamer.listen(EventType.QUOTE):
            print(quote)
            quotes[symbol_map[quote.eventSymbol]] = {
                'symbol': symbol_map[quote.eventSymbol],
                'description': quote.eventSymbol,
                'askPrice': quote.askPrice,
                'bidPrice': quote.bidPrice,
                'quoteTimeInLong': round(time.time() * 1000),
            }
            if len(quotes) == len(mapped_symbols):
                break
        # print_json(quotes)

        await streamer.close()

        return quotes

    def get_quotes(self, symbols: list) -> dict:
        all_quotes = {}
        try:
            symbols_to_fetch = []
            for symbol in symbols:
                symbol_info = self.get_symbol_info(symbol)
                if symbol_info is None:
                    all_quotes[symbol] = {
                        'symbol': symbol,
                        'description': 'Symbol not found',
                    }
                else:
                    symbols_to_fetch.append(symbol)
            quotes = asyncio.run(self.get_quotes_async(symbols_to_fetch))
            for symbol in quotes:
                all_quotes[symbol] = quotes[symbol]
        except Exception as e:
            print("Error getting quotes: ", e)
            for symbol in symbols:
                all_quotes[symbol] = {
                    'symbol': symbol,
                    'description': 'Error getting quote',
                }

        return all_quotes

    def get_account_info(self):
        pass

    def get_account(self):
        # Get the account information
        pass

    def place_order(self, order):
        # Place an order
        pass

    def cancel_order(self, order_id):
        # Cancel an order
        pass

from datetime import date
from fastapi.encoders import jsonable_encoder

def print_json(data):
    print(json.dumps(data, default=jsonable_encoder, indent=4))


async def main():
    tt = TastytradeAPI()
    session = tt.get_session()

    # get equity info
    appl = instruments.Equity.get_equity(session, 'AAPL')
    print_json(appl)

    # get option info
    # print_json(instruments.Option.get_option(session, 'INVALID  240209C00095000'))

    # get option chain
    # option_chain = instruments.get_option_chain(session, 'SPX')
    # for i, (key, val) in enumerate(option_chain.items()):
    #     print(key)
    #     print_json(val)
    #     if i == 0:
    #         break

    # get quotes
    from tastytrade.dxfeed import EventType
    from datetime import datetime

    """create streamer & subscribe to quotes"""
    from tastytrade import DXLinkStreamer
    streamer = await DXLinkStreamer.create(session)

    # subs_list = ['AAPL', '.AAPL240209C95']
    # await streamer.subscribe(EventType.QUOTE, subs_list)
    # quotes = {}
    # async for quote in streamer.listen(EventType.QUOTE):
    #     print(quote)
    #     quotes[quote.eventSymbol] = quote
    #     if len(quotes) == len(subs_list):
    #         break
    # print_json(quotes)

    # await streamer.unsubscribe(EventType.QUOTE, subs_list)

    """subscribe to candles"""
    await streamer.subscribe_candle(['.AAPL240315C185'], interval='1h', start_time=datetime(2024, 2, 8, 15, 30))
    async for candle in streamer.listen(EventType.CANDLE):
        print(candle)
    await streamer.close()

    """subscribe to candles using DXFeedStreamer"""
    # from tastytrade import DXFeedStreamer
    # feed_streamer = await DXFeedStreamer.create(session)
    # await feed_streamer.subscribe_candle(['.AAPL240209C95'], interval='1m', start_time=datetime(2024, 2, 8, 15, 30), end_time=datetime(2024, 2, 8, 15, 40))
    # async for candle in feed_streamer.listen(EventType.CANDLE):
    #     print(candle)







if __name__ == "__main__":
    import asyncio
    quotes = asyncio.run(main())
    # print_json(quotes)
