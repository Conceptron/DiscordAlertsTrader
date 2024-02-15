# ref: https://thetadata-api.github.io/thetadata-python/tutorials/

import pandas as pd
from datetime import date
from thetadata import ThetaClient, OptionReqType, OptionRight, DateRange
from DiscordAlertsTrader.configurator import cfg



def end_of_day(client) -> pd.DataFrame:
    # Make any requests for data inside this block. Requests made outside this block won't run.
    with client.connect():
        # Make the request
        out = client.get_hist_option(
            req=OptionReqType.QUOTE,  # End of day data
            root="QQQ",
            exp=date(2023, 11, 16),
            strike=384,
            right=OptionRight.PUT,
            date_range=DateRange(date(2023, 11, 10), date(2023, 11, 17))
        )
    # We are out of the client.connect() block, so we can no longer make requests.
    return out


def get_hist_option_example(client: ThetaClient) -> pd.DataFrame:
    # QQQ_111623P384
    with client.connect():
        data = client.get_hist_option(
                        req=OptionReqType.QUOTE,
                        root="QQQ",
                        exp=date(2023, 11, 16),
                        strike=384,
                        right=OptionRight.PUT,
                        date_range=DateRange(date(2023, 11, 15), date(2023, 11, 15)),
                        interval_size=60000
                    )
    return data

def get_hist_option_REST_example(client: ThetaClient) -> pd.DataFrame:
    data = client.get_hist_option_REST(
        req=OptionReqType.QUOTE,
        root="QQQ",
        exp=date(2023, 11, 16),
        strike=384,
        right=OptionRight.PUT,
        date_range=DateRange(date(2023, 11, 15), date(2023, 11, 15)),
        interval_size=60000
    )
    return data


if __name__ == "__main__":
    client = ThetaClient(username=cfg['thetadata']['username'], passwd=cfg['thetadata']['password'], launch=False)
    data = get_hist_option_REST_example(client)
    print(data.to_string())
