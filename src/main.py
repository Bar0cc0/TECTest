#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# main.py
# This script serves as the entry point for the application.
# It orchestrates the data ingestion pipeline by downloading and processing the data.
# It also includes a test mode for running unit tests.

__author__ = 'Michael Garancher'
__email__ = 'michaelgarancher1@gmail.com'
__version__ = '1.0.0'

import sys
import traceback
import signal
from importlib.util import spec_from_file_location, module_from_spec
import concurrent.futures
from pathlib import Path
import asyncio

from configuration import Configuration
from connectors import WebConnector
from scheduler import DataScheduler, CycleMonitor


from utils import (
	display_header,
	create_arg_parser,
	parse_params_to_dict,
	parse_list_param,
	process_multi_value_params
)


async def run_data_pipeline(config, endpoint, params=None):
	# Initialize components
	connector = WebConnector(config)
	scheduler = DataScheduler(config, connector)
	monitor = CycleMonitor(config, connector, endpoint=endpoint, params=params)
	logger = config.get_logger()
	
	try:
		# Start the scheduler
		if not scheduler.start():
			logger.error("Failed to start scheduler")
			return False
		
		# Schedule regular retrieval
		scheduler.schedule_endpoint(endpoint, interval_hours=24.0, params=params)

		# Start the cycle monitor
		if not monitor.start():
			logger.error("Failed to start cycle monitor")
			scheduler.stop()
			return False
		
		# Keep the pipeline running until interrupted
		running = True
		while running:
			try:
				# Sleep in small intervals to allow for clean shutdown
				await asyncio.sleep(5)
			except asyncio.CancelledError:
				# If the task is cancelled, we stop the pipeline
				running = False
		
		return True
	finally:
		# Clean up
		scheduler.stop()
		monitor.stop()

async def shutdown(signal, loop, task, logger):
	"""Handle shutdown gracefully."""
	logger.info(f"Shutting down due to signal: {signal.name}")
	
	# Cancel the pipeline task
	task.cancel()
	
	# Allow the pipeline to clean up
	try:
		await task
	except asyncio.CancelledError:
		pass

def run_tests() -> int:
	"""Run unit tests using the main_unittest.py orchestrator."""
	return 0

def main(endpoint: str, loglevel: str, **params ) -> int:
	"""
	Main function that orchestrates the data ingestion pipeline.
	
	Args:
		endpoint: endpoint to call
		loglevel: Logging level (DEBUG, INFO, WARNING, ERROR)
		**params: Optional parameters to pass to the API
	
	Returns:
		int: Return code (0 for success, 1 for failure)
	"""

	# Display application header
	display_header(__version__, endpoint)

	# Initialize the configuration
	config = Configuration(loglevel=loglevel)
		
	# Get the logger
	logger = config.get_logger()

	# Parse any list parameters to get all parameter combinations
	parsed_params = {}
	for key, value in params.items():
		parsed_params[key] = parse_list_param(value)

	param_combinations = process_multi_value_params(parsed_params)

	# Run the async pipeline
	loop = asyncio.new_event_loop()
	asyncio.set_event_loop(loop)
	
	# Create the pipeline task
	pipeline_task = asyncio.ensure_future(
		run_data_pipeline(config, endpoint, param_combinations),
		loop=loop
	)
	
	# Handle keyboard interrupts
	try:
		# Setup signal handlers for graceful shutdown
		for sig in (signal.SIGINT, signal.SIGTERM):
			loop.add_signal_handler(
				sig,
				lambda s=sig: asyncio.create_task(shutdown(s, loop, pipeline_task, logger))
			)
		
		# Run until the task completes or is cancelled
		success = loop.run_until_complete(pipeline_task)
		
	except KeyboardInterrupt:
		# Cancel the pipeline task
		pipeline_task.cancel()
		
		# Wait for the task to be cancelled
		try:
			loop.run_until_complete(pipeline_task)
		except asyncio.CancelledError:
			pass
		
		success = False
	finally:
		loop.close()
	
	# Report results
	if success:
		logger.info(f"Data retrieval for endpoint '{endpoint}' completed successfully.")
		return 0
	else:
		logger.error(f"Data retrieval for endpoint '{endpoint}' failed or was interrupted.")
		return 1



if __name__ == "__main__":
	# Add the parent directory to the system path for proper module imports
	# This is necessary if the script is run directly from the src directory
	if str(Path(__file__).resolve().parent) not in sys.path:
		sys.path.append(str(Path(__file__).resolve().parent))

	# Create argument parser
	parser = create_arg_parser()
	
	# Parse arguments
	args = parser.parse_args()
		
	# Check if we're running in test mode
	if args.test:
		sys.exit(run_tests())
	# Else run normal operation
	else:
		# Check if an endpoint is provided
		if not args.endpoint:
			parser.error('Parameter required: endpoint')
			sys.exit(1)
			
		# Convert params list to dictionary
		params_dict = parse_params_to_dict(args.params)
		
		# Call main function with parsed arguments
		try:
			exit_code = main(args.endpoint, loglevel=args.loglevel, **params_dict)
			sys.exit(exit_code)
		except Exception as e:
			print(f"Unhandled exception: {e}")
			traceback.print_exc()
			sys.exit(1)
