#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scheduler.py
# This module provides scheduling capabilities for data retrieval operations.

import asyncio
from datetime import datetime, timedelta
import time
import threading
from typing import Dict, List, Optional, Any, Callable, Set, Tuple, Union, cast
from concurrent.futures import Future
from pathlib import Path
import signal
import json

from interfaces import IConfigProvider
from connectors import WebConnector, DatabaseConnector
from reporter import DataQualityCheckerFactory
from processors import DataProcessorFactory


class DataScheduler:
	"""Scheduler for orchestrating data retrieval operations."""

	def __init__(self, config: IConfigProvider, webconnector: WebConnector, databaseconnector: DatabaseConnector) -> None:
		"""
		Initialize the scheduler with configuration and connector.
		
		Args:
			config: Configuration provider
			webconnector: Web connector for data retrieval
			databaseconnector: Database connector for data loading
		"""
		self._config = config
		self._logger = config.get_logger()
		self._webconnector = webconnector
		self._db_connector = databaseconnector

		# Register for cycle updates
		self._webconnector.register_cycle_callback(self._handle_cycle_update)

		# Track scheduled tasks
		self._scheduled_tasks: Dict[str, asyncio.Future] = {}
		self._scheduled_endpoints: Set[str] = set()
		self._cycle_data_tasks: Dict[str, Dict[int, Dict[str, asyncio.Future]]] = {} # endpoint/cycle/date_str/task
		self._running = False
		self._event_loop: Optional[asyncio.AbstractEventLoop] = None
		self._thread: Optional[threading.Thread] = None
		self._endpoint_params: Optional[Dict[str, Any]] = None
		
		# Get scheduler configuration
		self._workers = config.get_config('workers', 4)

		self._logger.debug(f"DataScheduler initialized with {self._workers} workers")

	def _handle_cycle_update(self, cycle: int, date: Optional[datetime] = None) -> None:
		"""
		Handle cycle update notifications from the connector.
		
		Args:
			cycle: The cycle number
			date: Optional specific date (defaults to current date)
		"""
		if date is None:
			date = datetime.now()
			
		date_str = date.strftime("%Y%m%d")
		
		# Create tasks for each registered endpoint with this cycle
		for endpoint in self._scheduled_endpoints:
			task_key = f"{endpoint}_{date_str}_cycle{cycle}"
			
			# Skip if we're already processing this cycle for this endpoint and date
			if (endpoint in self._cycle_data_tasks and 
				cycle in self._cycle_data_tasks[endpoint] and
				date_str in self._cycle_data_tasks[endpoint][cycle]):
				self._logger.debug(f"{task_key} already scheduled, skipping")
				continue
				
			# Skip if already in cache
			if self._webconnector.is_in_cache(endpoint, cycle, date):
				cached_path = self._webconnector.get_cached_file_path(endpoint, cycle, date)
				self._logger.info(f"Data for {task_key} already in cache: {cached_path}, skipping download")
				continue
				
			# Schedule immediate task for this cycle and date
			if self._event_loop is not None:
				self._logger.debug(f"Scheduling data retrieval for {endpoint} date {date_str} cycle {cycle}")
				try:
					task = asyncio.run_coroutine_threadsafe(
						self._fetch_endpoint_data(endpoint, cycle, date),
						self._event_loop
					)
					
					# Track the task with date information
					if endpoint not in self._cycle_data_tasks:
						self._cycle_data_tasks[endpoint] = {}
					if cycle not in self._cycle_data_tasks[endpoint]:
						self._cycle_data_tasks[endpoint][cycle] = {}
					
					# Store the task in the cycle data tasks
					self._cycle_data_tasks[endpoint][cycle][date_str] = cast(asyncio.Future, task)
				except RuntimeError as e:
					self._logger.error(f"Failed to schedule task: {e}")
			else:
				self._logger.warning(f"Cannot schedule {task_key}: event loop not available")

	async def _fetch_endpoint_data(self, endpoint: str, cycle: Optional[int] = None, 
								date: Optional[datetime] = None) -> Tuple[bool, List[Path]]:
		"""
		Fetch data for a specific endpoint, cycle, and date.
		
		Args:
			endpoint: Endpoint to fetch data for
			cycle: Optional cycle number
			date: Optional specific date
			
		Returns:
			Tuple of (success_flag, list_of_downloaded_files)
		"""
		try:
			date_str = date.strftime("%Y%m%d") if date else 'today'
			self._logger.info(f"Fetching data for endpoint {endpoint} date {date_str}" + 
						(f" cycle {cycle}" if cycle is not None else ""))
			
			# Get API parameters for this request
			api_params = {}
			if hasattr(self, '_endpoint_params') and self._endpoint_params:
				# Flatten params if it's a list of dictionaries
				if isinstance(self._endpoint_params, list) and len(self._endpoint_params) > 0:
					# Cast to correct type for type checking
					list_params = cast(List[Dict[str, Any]], self._endpoint_params)
					api_params = list_params[0]
				else:
					api_params = self._endpoint_params
			
			# Track if files came from cache for quality check decision
			from_cache = False

			# Pass the date and params to the connector
			success, files = await self._webconnector.fetch_data_with_retry(
				endpoint, 
				cycle, 
				date=date, 
				**api_params
			)
			
			# Check if these are cached files by looking at the connector's response
			if hasattr(self._webconnector, 'last_response_from_cache'):
				from_cache = self._webconnector.last_response_from_cache

			if success:
				if len(files) > 0:
					self._logger.info(f"Successfully retrieved {len(files)} file(s) for {endpoint} date {date_str}")
					
					# Only apply processors to newly downloaded files
					if not from_cache:
						asset = self._config.get_config('asset_id')
						# Create a data processing pipeline based on the endpoint
						pipeline = DataProcessorFactory.create_pipeline(
							data_type='capacity' if 'capacity' in asset else 'default',
							config=self._config
						)
						
						# Process all files through the pipeline
						for file_path in files:
							try:
								processed_file = pipeline.process(file_path)
								if not processed_file:
									self._logger.warning(f"Failed to process {file_path} through pipeline")
							except Exception as e:
								self._logger.warning(f"Error processing {file_path}: {str(e)}")
						
						# Queue files for database loading if enabled
						if self._db_connector and self._config.get_config('enable_database_loading', True):
							table_name = self._get_table_name_for_endpoint(endpoint)
							success = self._db_connector.post_data(table_name, files, cycle, date)
							if success:
								self._logger.info(f"Queued {len(files)} files for loading to database table '{table_name}'")
							else:
								self._logger.error(f"Failed to queue files for database loading to table '{table_name}'")
						
						# Run data quality check after all processing is complete
						if self._config.get_config('data_quality_report'):
							await self._process_data_quality(endpoint, files, cycle, date)
							
					elif from_cache and len(files) > 0:
						self._logger.debug(f"Skipping processing for cached files {[file.name for file in files]} from {endpoint}")
				else:
					self._logger.debug(f"No files retrieved for {endpoint} date {date_str}" +
							(f" cycle {cycle}" if cycle is not None else ""))
				
			else:
				self._logger.debug(f"No data found for {endpoint} date {date_str}" +
							(f" cycle {cycle}" if cycle is not None else ""))
			
			# Clean up task tracking
			if cycle is not None and date is not None:
				date_str = date.strftime("%Y%m%d")
				if (endpoint in self._cycle_data_tasks and 
					cycle in self._cycle_data_tasks[endpoint] and
					date_str in self._cycle_data_tasks[endpoint][cycle]):
					del self._cycle_data_tasks[endpoint][cycle][date_str]
				
			return success, files
			
		except Exception as e:
			self._logger.error(f"Error fetching data for {endpoint}: {str(e)}")
			return False, []

	def _get_table_name_for_endpoint(self, endpoint: str) -> str:
		"""
		Converts endpoint to a database table name.
		
		Args:
			endpoint: Endpoint name
			
		Returns:
			Database table name
		"""
		# Remove slashes and normalize endpoint name
		normalized_endpoint = endpoint.replace('/', '_').replace('-', '_')
		
		# First try to get from the mapping, removing any slashes
		endpoint_key = endpoint.replace('/', '_')
		
		# Check if there's a mapping in config
		mappings = self._config.get_config('endpoint_table_mappings', {})
		
		# Try exact match first
		if endpoint in mappings:
			return mappings[endpoint]
			
		# Try with underscores instead of hyphens
		if endpoint_key in mappings:
			return mappings[endpoint_key]
			
		# Try with hyphens instead of underscores
		hyphenated = endpoint.replace('_', '-')
		if hyphenated in mappings:
			return mappings[hyphenated]
		
		# Default to normalized endpoint if no mapping found
		self._logger.warning(f"No table mapping found for endpoint '{endpoint}', using normalized name")
		return normalized_endpoint

	async def _process_data_quality(self, endpoint: str, files: List[Path], cycle: Optional[int] = None,
								date: Optional[datetime] = None) -> None:
		"""
		Process data quality for downloaded files.
		
		Args:
			endpoint: Endpoint
			files: List of downloaded files
			cycle: Optional cycle number
			date: Optional specific date
		"""		
		try:
			self._logger.info(f"Running data quality check for {len(files)} files from {endpoint}")
			
			# Determine data type from endpoint for quality checker
			data_type = 'capacity' if 'capacity' in endpoint else 'default'
			
			# Create data quality checker
			quality_checker = DataQualityCheckerFactory.create_checker(data_type, self._config)
			
			# Process each file once
			for file_path in files:
				try:
					# Skip extremely small files (likely empty)
					if file_path.stat().st_size < 200:
						self._logger.debug(f"Skipping quality check for file that's too small: {file_path}")
						continue
					
					self._logger.debug(f"Checking data quality for file: {file_path}")
					
					# Run quality check on processed data
					quality_report = quality_checker.check_data_quality(file_path)
					
					# Save quality report with descriptive name
					report_name = f"quality_{file_path.stem}"
					report_path = quality_checker.save_report(report_name)
					
					# Log quality check results
					if quality_report.passed:
						self._logger.info(f"{file_path.name} passed data quality check with score {quality_report.quality_score:.2f}")
					else:
						self._logger.warning(f"{file_path.name} failed data quality check with score {quality_report.quality_score:.2f}")
						
				except Exception as e:
					self._logger.error(f"Error checking data quality for {file_path}: {str(e)}")
					
		except Exception as e:
			self._logger.error(f"Error in data quality processing: {str(e)}")

	async def _scheduled_task(self, endpoint: str, interval_hours: float) -> None:
		"""
		Run a scheduled task at regular intervals.
		
		Args:
			endpoint: Endpoint to fetch data for
			interval_hours: Hours between scheduled runs
		"""
		try:
			while self._running:
				# Wait for the next interval
				await asyncio.sleep(interval_hours * 3600)  # Convert hours to seconds
		except asyncio.CancelledError:
			self._logger.info(f"Scheduled task for {endpoint} was cancelled")
		except Exception as e:
			self._logger.error(f"Error in scheduled task for {endpoint}: {str(e)}")

	def schedule_endpoint(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> bool:
		"""
		Schedule a recurring, time-based trigger for retrieving data from the endpoint.
		Allows balancing between getting timely data updates and minimizing system load.

		Args:
			endpoint: Endpoint to schedule
			interval_hours: Hours between scheduled runs
			params: Optional parameters to include in the request
			
		Returns:
			True if scheduled successfully
		"""
		if not self._running:
			self._logger.error('Cannot schedule endpoint: scheduler is not running')
			return False
			
		if endpoint in self._scheduled_tasks:
			self._logger.warning(f"Endpoint {endpoint} is already scheduled")
			return False
		
		# Store parameters for this endpoint
		self._endpoint_params = params

		# Add to scheduled endpoints set
		self._scheduled_endpoints.add(endpoint)
		
		# Create and schedule the task
		interval_hours = self._config.get_config('schedule_interval_hours', 1.0) 
		if not isinstance(interval_hours, float) or interval_hours <= 0:
			interval_hours = abs(float(interval_hours))
		self._logger.debug(f"Scheduling endpoint {endpoint} with interval {interval_hours} hours")
		if self._event_loop is not None:
			task = asyncio.run_coroutine_threadsafe(
				self._scheduled_task(endpoint, interval_hours),
				self._event_loop
			)
			
			# Store the task for management
			self._scheduled_tasks[endpoint] = cast(asyncio.Future, task)
			return True
		else:
			self._logger.error('Cannot schedule endpoint: event loop not available')
			return False
	
	def unschedule_endpoint(self, endpoint: str) -> bool:
		"""
		Remove scheduled endpoint.
		
		Args:
			endpoint: API endpoint to unschedule
			
		Returns:
			True if unscheduled successfully
		"""
		if endpoint not in self._scheduled_tasks:
			self._logger.warning(f"Endpoint {endpoint} is not scheduled")
			return False
			
		self._logger.info(f"Unscheduling endpoint {endpoint}")
		
		# Cancel the task
		task = self._scheduled_tasks[endpoint]
		task.cancel()
		
		# Remove from tracking
		del self._scheduled_tasks[endpoint]
		self._scheduled_endpoints.remove(endpoint)
		
		return True

	async def load_to_database(self, table_name: str, data_files: List[Path], 
							cycle: Optional[int] = None, date: Optional[datetime] = None) -> bool:
		"""
		Load processed data files into the database.
		
		Args:
			table_name: Database table name
			data_files: List of processed CSV files to load
			cycle: Optional cycle number
			date: Optional specific date
			
		Returns:
			Success flag
		"""
		self._logger.info(f"Loading {len(data_files)} files to database table '{table_name}'")
		
		# Queue the files for processing
		return self._db_connector.post_data(table_name, data_files, cycle, date)

	def _run_event_loop(self) -> None:
		"""Run the event loop in a separate thread."""
		loop = None
		try:
			# Create a new event loop for this thread
			loop = asyncio.new_event_loop()
			asyncio.set_event_loop(loop)
			self._event_loop = loop
			
			# Run the event loop
			loop.run_forever()
		except Exception as e:
			self._logger.error(f"Error in event loop: {str(e)}")
		finally:
			if loop is not None:
				loop.close()
			self._event_loop = None
	
	def _handle_signal(self, sig: signal.Signals) -> None:
		"""Handle termination signals."""
		self._logger.info(f"Received signal {sig.name}, shutting down...")
		self.stop()
	
	def start(self) -> bool:
		"""
		Start the scheduler.
		
		Returns:
			True if started successfully
		"""
		if self._running:
			self._logger.warning("Scheduler is already running")
			return False
			
		self._logger.info("Starting scheduler")
		self._running = True
		
		# Start event loop in a separate thread
		self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
		self._thread.start()
		
		# Wait for event loop to be ready with a timeout
		start_time = time.time()
		timeout = 10  # 10 second timeout
		while self._event_loop is None and self._thread is not None and self._thread.is_alive():
			self._logger.debug("Waiting for event loop to be ready...")
			threading.Event().wait(0.1)
			if time.time() - start_time > timeout:
				self._logger.error("Timeout waiting for event loop")
				self._running = False
				return False
			
		return self._thread is not None and self._thread.is_alive()
	
	def stop(self) -> None:
		"""Stop the scheduler and cancel all scheduled tasks."""
		if not self._running:
			self._logger.warning("Scheduler is not running")
			return
			
		self._logger.info("Stopping scheduler")
		self._running = False
		
		# Cancel all scheduled tasks
		for endpoint, task in list(self._scheduled_tasks.items()):
			self._logger.info(f"Cancelling scheduled task for {endpoint}")
			task.cancel()
			
		# Cancel all cycle tasks safely
		for endpoint, cycles in list(self._cycle_data_tasks.items()):
			if isinstance(cycles, dict):
				for cycle, dates in list(cycles.items()):
					for date_str, task in list(dates.items()):
						self._logger.info(f"Cancelling cycle task for {endpoint} cycle {cycle}")
						try:
							task.cancel()
						except Exception as e:
							self._logger.error(f"Error cancelling task for {endpoint} cycle {cycle}: {e}")
		
		# Close database connector if it exists
		if self._db_connector:
			try:
				# Wait for remaining queue items to be processed
				if hasattr(self._db_connector, '_queue') and hasattr(self._db_connector._queue, 'join'):
					try:
						self._db_connector._queue.join()
					except Exception as e:
						self._logger.warning(f"Error waiting for database queue to complete: {e}")
						
				# Close the connector
				if hasattr(self._db_connector, 'close'):
					self._db_connector.close()
					self._logger.debug('Database connector closed')
			except Exception as e:
				self._logger.error(f"Error closing database connector: {e}")

		# Stop the event loop
		if self._event_loop is not None:
			try:
				asyncio.run_coroutine_threadsafe(
					self._stop_event_loop(),
					self._event_loop
				)
			except RuntimeError as e:
				self._logger.warning(f"Could not stop event loop: {e}")
			
		# Wait for thread to end
		if self._thread is not None and self._thread.is_alive():
			self._thread.join(timeout=5.0)
	
	async def _stop_event_loop(self) -> None:
		"""Stop the event loop safely."""
		try:
			# Schedule event loop to stop
			loop = asyncio.get_event_loop()
			loop.stop()
		except RuntimeError as e:
			self._logger.warning(f"Error stopping event loop: {e}")
	

class CycleMonitor:
	"""Monitor for detecting new data cycles and ensuring all available data is downloaded."""
	
	def __init__(self, config: IConfigProvider, connector: WebConnector, 
				check_interval_minutes: float = 10.0, endpoint: Optional[str] = None,
				params: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None) -> None:
		"""Initialize the cycle monitor."""
		self._config = config
		self._logger = config.get_logger()
		self._connector = connector
		self._check_interval = check_interval_minutes * 60  # To seconds
		self._params = params
		self._endpoint = endpoint or 'capacity/operationally-available'

		# Get history setting from config
		self._history = config.get_config('history', 3)  # Default to 3 days
		
		# Track downloaded data to avoid duplicates
		self._downloaded_data = set()  # Set of (date_str, cycle) tuples
		
		# Track running state 
		self._running = False

		# Thread for monitoring
		self._thread: Optional[threading.Thread] = None

		self._logger.debug(f"CycleMonitor initialized for endpoint '{self._endpoint}' with check interval {check_interval_minutes} minutes")

	#FIXME: Reduce code duplication: this logic is similar to _check_for_new_cycles()
	def _check_historical_data(self) -> None:
		"""Check for historical data and ensure all cycles are downloaded."""
		dates = []
		# Generate past dates based on history setting
		for i in range(1, self._history + 1):
			past_date = datetime.now() - timedelta(days=i)
			dates.append(past_date)

		#FIXME: Enforce separation of concerns: _get_api_params() should be a separate method
		# Get API parameters if any
		api_params = {}
		if self._params:
			# Use first set of params if it's a list
			if isinstance(self._params, list) and len(self._params) > 0:
				# Cast to correct type for type checking
				list_params = cast(List[Dict[str, Any]], self._params)
				api_params = list_params[0]
			else:
				api_params = self._params
		
		# Convert parameters to string keys
		string_api_params = {str(k): v for k, v in api_params.items()} if isinstance(api_params, dict) else {}
		
		# For each past date, try all possible cycles (1-8)
		for date in dates:
			date_str = date.strftime("%Y%m%d")
			
			# Try all possible cycle numbers instead of assuming fixed mappings
			for cycle in range(1, 9):  # Try cycles 1 through 8
				# Skip if already downloaded in this session
				download_key = (date_str, cycle)
				if download_key in self._downloaded_data:
					self._logger.debug(f"Data for {date_str} cycle {cycle} already processed in this session")
					continue
				
				# Check persistent cache first with params
				if self._connector.is_in_cache(self._endpoint, cycle, date, **string_api_params):
					# Check if this is actually an intraday cycle we want by examining metadata
					cached_path = self._connector.get_cached_file_path(self._endpoint, cycle, date)
					if cached_path:
						metadata_dir = Path(self._config.get_config('output_dir')) / 'metadata'
						metadata_path = metadata_dir / f"{cached_path.stem}.meta.json"
						if metadata_path.exists():
							try:
								with open(metadata_path, 'r') as f:
									metadata = json.load(f)
									cycle_name = metadata.get('cycle_name')
									if cycle_name and 'intraday' in cycle_name.lower():
										self._logger.info(f"Data for {self._endpoint} date: {date_str} {cycle_name} is already downloaded")
										# Add to downloaded data to avoid reprocessing
										self._downloaded_data.add(download_key)
							except Exception as e:
								self._logger.warning(f"Error reading metadata: {e}")
					continue
				
				# Notify the connector to try this cycle
				self._logger.debug(f"Checking for intraday data with cycle={cycle} for {self._endpoint} {date_str}")
				self._connector.notify_cycle_update(cycle, date=date)
				self._downloaded_data.add(download_key)
	
	def _check_for_new_cycles(self) -> None:
		"""Check for newly available data for today based on current time."""
		current_hour = datetime.now().hour
		today = datetime.now()
		today_str = today.strftime("%Y%m%d")
		
		# Get API parameters if any
		api_params = {}
		if self._params:
			# Use first set of params if it's a list
			if isinstance(self._params, list) and len(self._params) > 0:
				list_params = cast(List[Dict[str, Any]], self._params)
				api_params = list_params[0]
			else:
				api_params = self._params
		
		# Convert parameters to string keys
		string_api_params = {str(k): v for k, v in api_params.items()} if isinstance(api_params, dict) else {}
		
		# Determine which intraday cycles might be available based on time
		# Rather than mapping to specific cycle numbers, we'll try all possible cycles
		expected_intraday = []
		if current_hour >= 15 and current_hour < 20:
			expected_intraday.append('Intraday 1')
		elif current_hour >= 20 and current_hour < 23:
			expected_intraday.append('Intraday 1')
			expected_intraday.append('Intraday 2')
		elif current_hour >= 23 or current_hour < 3:
			expected_intraday.append('Intraday 1')
			expected_intraday.append('Intraday 2')
			expected_intraday.append('Intraday 3')

		# Log which intraday cycles we expect to be available
		if expected_intraday:
			self._logger.info(f"Based on current time ({current_hour}:00), expecting data for: {', '.join(expected_intraday)}")
		
		# Try all possible cycle numbers to find intraday data
		for cycle in range(1, 9):  # Try cycles 1 through 8
			# Skip if already downloaded in this session
			download_key = (today_str, cycle)
			if download_key in self._downloaded_data:
				self._logger.debug(f"Data for {self._endpoint} cycle {cycle} already processed in this session")
				continue
			
			# Check persistent cache before scheduling
			if self._connector.is_in_cache(self._endpoint, cycle, today, **string_api_params):
				cached_path = self._connector.get_cached_file_path(self._endpoint, cycle, today)
				# Check if this is an intraday cycle we want
				if cached_path:
					metadata_dir = Path(self._config.get_config('output_dir')) / "metadata"
					metadata_path = metadata_dir / f"{cached_path.stem}.meta.json"
					if metadata_path.exists():
						try:
							with open(metadata_path, 'r') as f:
								metadata = json.load(f)
								cycle_name = metadata.get('cycle_name')
								if cycle_name and "intraday" in cycle_name.lower():
									self._logger.info(f"Data for {self._endpoint} date: {today_str} {cycle_name} is already downloaded")
									# Add to downloaded data to avoid reprocessing
									self._downloaded_data.add(download_key)
						except Exception as e:
							self._logger.warning(f"Error reading metadata: {e}")
				continue
				
			# Notify the connector to try this cycle - it will filter non-intraday data
			self._logger.info(f"Checking for intraday data with cycle={cycle} for {self._endpoint}")
			self._connector.notify_cycle_update(cycle, date=today)
			self._downloaded_data.add(download_key)
	
	def _monitor_loop(self) -> None:
		"""Main monitoring loop."""
		try:
			# First, check for all historical data
			self._check_historical_data()
			
			# Then start periodic checks for today's data
			while self._running:
				self._check_for_new_cycles()
				
				# Sleep until next check
				for _ in range(int(self._check_interval / 5)):
					if self._running:
						threading.Event().wait(5)
					else:
						break
		except Exception as e:
			self._logger.error(f"Error in cycle monitor: {str(e)}")
	
	def start(self) -> bool:
		"""Start the cycle monitor."""
		if self._running:
			self._logger.warning('Cycle monitor is already running')
			return False

		self._logger.info('Starting cycle monitor')
		self._running = True
		
		# Start monitoring in a separate thread
		self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
		self._thread.start()
		
		return self._thread.is_alive()
	
	def stop(self) -> None:
		"""Stop the cycle monitor."""
		if not self._running:
			self._logger.warning('Cycle monitor is not running')
			return

		self._logger.info('Stopping cycle monitor')
		self._running = False
		
		# Wait for thread to end
		if self._thread is not None and self._thread.is_alive():
			self._thread.join(timeout=5.0)
