import unittest
from DiscordAlertsTrader.marketdata.thetadata_api import ThetaClientAPI

class TestThetaDataAPI(unittest.TestCase):

    def setUp(self):
        self.api = ThetaClientAPI()

    def test_get_price_at_time_existing_quote(self):
        symbol = "QQQ_111623P384"
        unixtime = 1700062200
        price_type = "BTO"
        expected_price = 0.67
        actual_price = self.api.get_price_at_time(symbol, unixtime, price_type)
        self.assertEqual(actual_price, expected_price)

if __name__ == '__main__':
    unittest.main()
