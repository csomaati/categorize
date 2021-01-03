#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Expense categorizer
Usage: categorize.py [-h --version] [--quiet | --verbose] -r RULES -f FILE

Options:
  -h --help             Show this screen.
  --version             Show app version
  -f FILE, --file FILE  Expense csv file path
  -r RULES, --rules RULES   Rule definitions in yaml format
  --quiet               print less text
  --verbose             print more text
"""

import pandas as pd
import sys
from docopt import docopt
import logging
from functools import reduce, partial
import re
import datetime
import yaml


logger = logging.getLogger("hu.csomaati."+__name__)

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
COMMENT = "Jegyzet"
PARTY = "Megnevezés"
CATEGORY = "Kategória"
DATE = "Dátum"

ERSTE_COMMENT = "erste_comment"


def check_matching(params, matcher):
    if len(matcher) != 1:
        raise ValueError(
            "Invalid structure for matcher. One k:v pair allowed/matcher element")
    k, v = list(matcher.items())[0]
    try:
        return re.match(v, params[k])
    except KeyError:
        return False


def get_default_params(row: pd.Series):
    return row.to_dict()


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


param_map = {
    "default": get_default_params,
    "erste_comment": get_erste_comment_params
}


def rule_params(required_params: list, row: pd.Series):
    params = param_map['default'](row)
    required_params = required_params if required_params else []
    for required_param in required_params:
        params.update(param_map[required_param](row))
    return params


def rule_matcher(matchers, params):
    check_matcher = partial(check_matching, params)
    return all(map(check_matcher, matchers))


def apply_rule(rule: dict, row: pd.Series):
    name = rule['name']
    logger.debug(f"Checking rule {name}")

    try:
        params = rule_params(rule.get('properties', None), row)
    except ValueError as e:
        logger.error(e)
        return row

    matching = rule_matcher(rule['matcher'], params)

    if not matching:
        return row

    for row_name, row_content in rule["modifications"].items():
        row[row_name] = row_content.format(**params)
    return row


def row_categorizer(row: pd.Series, rules: list):
    for rule in rules:
        row = apply_rule(rule, row)
    return row


def load_rules(rule_file):
    with open(rule_file) as f:
        rules = yaml.load(f, Loader=yaml.FullLoader)

    try:
        rule_list = rules["rules"]
    except KeyError:
        raise ValueError(
            "Invalid rule structure. Must containe a 'rule' root element")

    return rule_list


def categorzier(file, rules):
    logger.info(f"Loading expenses from {file}")
    table = pd.read_csv(file)
    logger.info(f"Loaded {len(table)} line(s)")

    rules = load_rules(rules)
    logger.info(f"Loaded {len(rules)} rule(s)")

    row_updater = partial(row_categorizer, rules=rules)
    table = table.apply(row_updater, axis=1)
    print(table)


def main(doc=None, argv=None):
    doc = __doc__ if not doc else doc
    argv = sys.argv[1:] if not argv else argv
    arguments = docopt(doc, argv, version='Expense Categorizer 0.1')

    if(arguments["--quiet"]):
        logger.setLevel(logging.ERROR)
    elif(arguments["--verbose"]):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)

    categorzier(file=arguments["--file"], rules=arguments["--rules"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
