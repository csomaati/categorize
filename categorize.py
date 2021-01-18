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
import contextlib
import random
import string



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

def rnd(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def extend(df: pd.DataFrame, *args, **kwargs):
    row_name = rnd(8)
    df[row_name] = range(1, len(df.index)+1)
    try:
        df = df.groupby(row_name).apply(*args, **kwargs)
    finally:
        df.drop(row_name, axis=1, inplace=True)
    return df

def action_update(group, action, properties):
    # waiting for one line DF's from groupby apply calls
    assert len(group.index) == 1
    row = group.iloc[0].copy()
    for row_name, row_content in action.items():
        row[row_name] = row_content.format(**properties)
    new_df = pd.DataFrame([row,])
    return new_df

ACTIONS = {
    'update': action_update,
}


def action_route(action_type):
    try:
      action = ACTIONS[action_type]
    except KeyError:
        logger.warning(f"There is no defined action for type {action_type}")
        raise ValueError("Skipping row")
    return action

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

def build_properties(properties: list, row: pd.Series):
    properties = [] if not properties else properties[:]
    properties.insert(0, 'default')
    logger.debug(f"Building properties for the following required properties {properties}")
    try:
      built_properties = map(lambda prop_name: param_map[prop_name](row), properties)
    except (KeyError, ValueError) as e:
        logger.warning(f"Cannot generate all requested properties. Skipping this row!")
        logger.error(e)
        raise ValueError("Skipping row")
    merged_properties = reduce(lambda x, y: {**x, **y}, built_properties, {})
    logger.debug(f"Merged properties: {merged_properties}")
    return merged_properties

def check_rule(matchers: list, properties: dict):
    check_matcher = partial(check_matching, properties)
    return all(map(check_matcher, matchers))

def row_checker(group: pd.DataFrame, rule:dict):
    group = group.copy()
    # waiting for one line DF's from groupby apply calls
    assert len(group.index) == 1
    row = group.iloc[0]
    logger.debug(f"Checking rule {rule['name']}")

    original_group = group.copy()

    try:
      properties = build_properties(rule.get('properties', None), row)
    except ValueError:
        logger.warning("Cannot build required properties, skipping row!")
        return original_group

    matched = check_rule(rule['matchers'], properties)

    if not matched:
        return original_group

    actions = rule['actions']

    try:
      for action_type, action in actions.items():
          action_fn = action_route(action_type)
          group = extend(group, action_fn, action, properties)
    except ValueError:
        logger.warning("Cannot apply every action, skipping row!")
        return original_group
    
    return group
    

def row_categorizer(group: pd.DataFrame, rules: list):
    group = group.copy()
    for rule in rules:
        group = extend(group, row_checker, rule=rule)
    return group


def load_rules(rule_file):
    with open(rule_file) as f:
        rules = yaml.load(f, Loader=yaml.FullLoader)

    try:
        rule_list = rules["rules"]
    except KeyError:
        raise ValueError(
            "Invalid rule structure. Must containe a 'rule' root element")

    active_rules = list(filter(lambda r: ('active' not in r) or r['active'], rule_list))
    logger.info(f"Found {len(active_rules)} active rule(s)")

    return active_rules


def categorizer(file, rules_path):
    logger.info(f"Loading expenses from {file}")
    table = pd.read_csv(file)
    logger.info(f"Loaded {len(table)} line(s)")

    rules = load_rules(rules_path)
    logger.info(f"Loaded {len(rules)} rule(s)")

    table = extend(table, row_categorizer, rules=rules)

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

    categorizer(file=arguments["--file"], rules_path=arguments["--rules"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
