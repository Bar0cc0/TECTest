#!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# utils.py
# This module provides utility functions for the application.

import argparse
import traceback
from datetime import datetime
from itertools import product
from typing import Dict, List, Any, Union, Optional, Tuple


def display_header(version:str, endpoint: str) -> None:
	"""Display application header with useful information."""
	current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	
	header = f"""
╔══════════════════════════════════════════════════════════════════╗
║                      TEC DATA PIPELINE API                       ║
╠══════════════════════════════════════════════════════════════════╣
║ Version:    {version:<52} ║
║ Runtime:    {current_time:<52} ║
║ Endpoint:   {endpoint:<52} ║
╠══════════════════════════════════════════════════════════════════╣
║ Press Ctrl+C to stop                                             ║
╚══════════════════════════════════════════════════════════════════╝
"""
	print(header)


def create_arg_parser() -> argparse.ArgumentParser:
	"""
	Creates and returns an argument parser for command line arguments.
	
	Returns:
		argparse.ArgumentParser: Configured argument parser
	"""
	parser = argparse.ArgumentParser(description='TW Data API Clien')
	parser.add_argument('endpoint', nargs='?', help='Endpoint (e.g., capacity/operationally-available)')
	parser.add_argument('--params', nargs='+', help='Filters as key=value pairs', default=[])
	parser.add_argument('--loglevel', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
						default='INFO', help='Set the logging level')
	parser.add_argument('--test', action='store_true', help='Test mode (no API calls)')
	return parser

def parse_params_to_dict(param_list: List[str]) -> Dict[str, str]:
	"""
	Helper function to convert a list of parameters in the format 'key=value' to a dictionary.
	
	Args:
		param_list: List of strings in 'key=value' format
		
	Returns:
		Dictionary with parsed parameters
	"""
	params_dict = {}
	for param in param_list:
		try:
			key, value = param.split('=', 1)
			params_dict[key] = value
		except ValueError as e:
			print(f"Error parsing parameter '{param}': {e}")
			traceback.print_exc()
	return params_dict

def parse_list_param(value: Any) -> Union[List[Union[int, str]], Any]:
	"""
	Helper function to parse a parameter value that might be in list format [val1,val2,...]
	
	Args:
		value: The value to parse, potentially in list format
		
	Returns:
		List of values if input was a list format string, otherwise the original value
	"""
	if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
		# Remove brackets and split by comma
		values = value[1:-1].split(',')
		# Convert numeric values to integers
		result = []
		for val in values:
			val = val.strip()
			if val.isdigit():
				result.append(int(val))
			else:
				result.append(val)
		return result
	return value

def process_multi_value_params(params: Dict[str, Any]) -> List[Dict[str, Any]]:
	"""
	Helper function to process parameters with multiple values and generate all combinations.
	
	Args:
		params: Dictionary of parameters where some values might be lists
		
	Returns:
		List of parameter dictionaries to use in separate API calls
	"""
	# Separate fixed and multi-value parameters
	fixed_params = {k: v for k, v in params.items() if not isinstance(v, list)}
	multi_params = {k: v for k, v in params.items() if isinstance(v, list)}
	
	# No multi-value params case
	if not multi_params:
		return [params]
	
	# Generate all combinations using itertools
	keys = multi_params.keys()
	values = multi_params.values()
	combinations = product(*values)
	
	# Merge with fixed parameters
	result = []
	for combo in combinations:
		param_dict = fixed_params.copy()
		param_dict.update(dict(zip(keys, combo)))
		result.append(param_dict)
		
	return result
