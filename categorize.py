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
import yaml

import pandas_extensions

logger = logging.getLogger("hu.csomaati."+__name__)

COMMENT = "Jegyzet"
PARTY = "Megnevezés"
CATEGORY = "Kategória"
DATE = "Dátum"

ERSTE_COMMENT = "erste_comment"

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
            "Invalid structure for matcher. One k:v pair allowed per matcher element")
    k, v = list(matcher.items())[0]
    try:
        return re.match(v, params[k])
    except KeyError:
        return False

param_map = {
    "default": get_default_params,
    "erste_comment": get_erste_comment_params
}

def build_properties(properties: list, row: pd.Series):
    if 'default' not in properties: properties.insert(0, 'default')
    logger.debug(f"Building properties for the following required properties {properties}")
    try:
        def prop_func(name): return param_map[name](row)
        built_properties = map(prop_func, properties)
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

@pandas_extensions.extendable
def row_apply_rule(group: pd.DataFrame, rule:dict):
    # waiting for one line DF's from groupby apply calls
    assert len(group.index) == 1
    group = group.copy()
    row = group.iloc[0].copy()

    original_group = group.copy()

    try:
        required_properties = rule.get('properties', [])[:]
        properties = build_properties(required_properties, row)
    except ValueError:
        logger.warning(f"Cannot build required properties, skipping row {row.index}!")
        return original_group

    matched = check_rule(rule.get('matchers', []), properties)

    if not matched:
        return original_group

    actions = rule['actions']

    try:
        def reducer(_group, _action):
            action_fn = action_route(_action[0])
            new_group = action_fn(_group, _action[1], properties)
            return new_group

        new_group = reduce(reducer, actions.items(), group)
    except ValueError:
        logger.warning("Cannot apply every action, skipping row!")
        return original_group
    
    return new_group
    

def rule_applier(table: pd.DataFrame, rules: list):
    def reducer(_table, _rule):
        logger.info(f"Applying rule {_rule['name']}")
        # apply a rule on a group and return with the new group
        return row_apply_rule(_table, _rule)
    new_table = reduce(reducer, rules, table)
    return new_table


def load_rules(rule_file):
    with open(rule_file) as f:
        rules = yaml.load(f, Loader=yaml.FullLoader)

    try:
        rule_list = rules["rules"]
    except KeyError:
        raise ValueError(
            "Invalid rule structure. Must containe a 'rule' root element")

    active_rules = list(filter(lambda r: ('active' not in r) or r['active'], rule_list))
    logger.debug(f"Found {len(active_rules)} active rule(s)")

    return active_rules


def categorizer(file, rules_path):
    logger.info(f"Loading expenses from {file}")
    table = pd.read_csv(file)    
    logger.info(f"Loaded {len(table)} line(s)")

    logger.info(f"Loading rules from {rules_path}")
    rules = load_rules(rules_path)
    logger.info(f"Loaded {len(rules)} rule(s)")

    table = rule_applier(table, rules)

    print(table)

def get_loglevel(arguments):
    level = logging.WARNING
    if(arguments["--quiet"]):
        level = logging.ERROR
    elif(arguments["--verbose"]):
        level =  logging.DEBUG

    return level

def main(doc=None, argv=None):
    doc = __doc__ if not doc else doc
    argv = sys.argv[1:] if not argv else argv
    arguments = docopt(doc, argv, version='Expense Categorizer 0.1')
    
    log_level = get_loglevel(arguments)
    logger.setLevel(log_level)

    categorizer(file=arguments["--file"], rules_path=arguments["--rules"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
