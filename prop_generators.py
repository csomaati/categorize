import pandas as pd
import re
import datetime

def get_default_params(row: pd.Series):
    return row.to_dict()

comment_re = re.compile(r"(?P<card>\S+)\s+"  # card id string
                        # An optional id. Not always presented but always(?) contains a number. Its length may vary
                        r"(?:(?P<id>(?:\S*[0-9]\S*))\s+)?"
                        # The place of purchase. Usually a string to identify the other pary (e.g the name of the shop)
                        r"(?P<place>.+)"
                        # purchase date in the format YYMMDDHH:MM
                        r"(?P<date>[0-9]{2}[0-9]{2}[0-9]{2})(?P<time>[0-9]{2}:[0-9]{2})\s+"
                        # some optional string. Seen only on currency change and its value was .00 Have no ide what it is
                        r"(.+)?"
                        # some static string. Looks like always presented
                        r"vásár\.\s*"
                        # exchange infomation. Optional. Presented only when currency exchange was involved
                        r"(?P<exchange>(?P<amount>\S+)\s+(?P<currency>\S+)\s+(?P<rate>\S+))?",
                        re.UNICODE)

def get_erste_comment_params(row: pd.Series):
    comment = row[COMMENT]
    if pd.isna(comment):
        return {}
    match = comment_re.match(comment)
    if not match:
        raise ValueError(f"Invalid erste comment: {comment}")
    params = match.groupdict()
    params['comment_date'] = datetime.datetime.strptime(
        match['date']+match["time"], '%y%m%d%H:%M').strftime("%Y-%m-%d %H:%M")
    return params
