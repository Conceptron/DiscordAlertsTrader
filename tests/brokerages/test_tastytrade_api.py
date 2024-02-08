import unittest
from DiscordAlertsTrader.brokerages.tastytrade_api import TastytradeAPI

class TestTastytradeAPI(unittest.TestCase):

    def setUp(self):
        self.api = TastytradeAPI()
        self.api.get_session()

    def test_map_option_symbol(self):
        symbol = "AAPL_080521C100.5"
        expected_result = ".AAPL210805C100.5"
        result = self.api.map_option_symbol(symbol)
        self.assertEqual(result, expected_result)

        result = self.api.map_option_symbol(symbol, True)
        self.assertEqual(result, "AAPL  210805C00100500")

    def test_get_option_info(self):
        symbol = "AAPL_020924C202.5"
        result = self.api.get_option_info(symbol)
        self.assertEqual(result.instrument_type, 'Equity Option')
        self.assertEqual(result.symbol, 'AAPL  240209C00202500')

    def test_get_symbol_info(self):
        symbol = "AAPL"
        result = self.api.get_symbol_info(symbol)
        self.assertEqual(result.symbol, 'AAPL')
        result = self.api.get_symbol_info(symbol)
        self.assertEqual(result.symbol, 'AAPL')

    def test_get_quotes(self):
        symbols = ["AAPL", "AAPL_020924C188"]
        result = self.api.get_quotes(symbols)
        print(result)
        self.assertEqual(result[symbols[0]]['symbol'], symbols[0])
        self.assertEqual(result[symbols[1]]['symbol'], symbols[1])


if __name__ == '__main__':
    unittest.main()
