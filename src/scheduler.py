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

from interfaces import IConfigProvider
from connectors import WebConnector


class DataScheduler:
	"""Scheduler for orchestrating data retrieval operations."""
	
	def __init__(self, config: IConfigProvider, connector: WebConnector) -> None:
		"""
		Initialize the scheduler with configuration and connector.
		
		Args:
			config: Configuration provider
			connector: Web connector for data retrieval
		"""
		self._config = config
		self._logger = config.get_logger()
		self._connector = connector
		
		# Register for cycle updates
		self._connector.register_cycle_callback(self._handle_cycle_update)
		
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
				
			# Check persistent cache first - skip if already in cache
			if self._connector.is_in_cache(endpoint, cycle, date):
				cached_path = self._connector.get_cached_file_path(endpoint, cycle, date)
				self._logger.info(f"Data for {task_key} already in cache: {cached_path}, skipping download")
				continue
				
			# Schedule immediate task for this cycle and date
			if self._event_loop is not None:
				self._logger.info(f"Scheduling data retrieval for {endpoint} date {date_str} cycle {cycle}")
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
			endpoint: API endpoint to fetch data for
			cycle: Optional cycle number
			date: Optional specific date
			
		Returns:
			Tuple of (success_flag, list_of_downloaded_files)
		"""
		try:
			date_str = date.strftime("%Y%m%d") if date else "today"
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
					
			# Pass the date and params to the connector
			success, files = await self._connector.fetch_data_with_retry(
				endpoint, 
				cycle, 
				date=date, 
				**api_params
			)
			
			if success:
				self._logger.info(f"Successfully retrieved {len(files)} files for {endpoint} date {date_str}" +
								(f" cycle {cycle}" if cycle is not None else ""))
			else:
				self._logger.error(f"Failed to retrieve data for {endpoint} date {date_str}" +
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
	
	async def _scheduled_task(self, endpoint: str, interval_hours: float) -> None:
		"""
		Run a scheduled task at regular intervals.
		
		Args:
			endpoint: endpoint to fetch data for
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

	def schedule_endpoint(self, endpoint: str, interval_hours: float = 24.0, params: Optional[Dict[str, Any]] = None) -> bool:
		"""Schedule regular data retrieval for an endpoint.
		
		Args:
			endpoint: Endpoint to schedule
			interval_hours: Hours between scheduled runs
			params: Optional parameters to include in the request
			
		Returns:
			True if scheduled successfully
		"""
		if not self._running:
			self._logger.error("Cannot schedule endpoint: scheduler is not running")
			return False
			
		if endpoint in self._scheduled_tasks:
			self._logger.warning(f"Endpoint {endpoint} is already scheduled")
			return False
		
		# Store parameters for this endpoint
		self._endpoint_params = params

		# Add to scheduled endpoints set
		self._scheduled_endpoints.add(endpoint)
		
		# Create and schedule the task
		if self._event_loop is not None:
			task = asyncio.run_coroutine_threadsafe(
				self._scheduled_task(endpoint, interval_hours),
				self._event_loop
			)
			
			# Store the task for management
			self._scheduled_tasks[endpoint] = cast(asyncio.Future, task)
			return True
		else:
			self._logger.error("Cannot schedule endpoint: event loop not available")
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
		
		self._running = False
		self._thread: Optional[threading.Thread] = None
	
	def _check_historical_data(self) -> None:
		"""Check for historical data and ensure all cycles are downloaded."""
		# Get dates based on history setting (excluding today)
		today = datetime.now().date()
		dates = []
		for i in range(1, self._history + 1):
			past_date = datetime.now() - timedelta(days=i)
			dates.append(past_date)
		
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
		
		# For each past date, check all cycles
		for date in dates:
			date_str = date.strftime("%Y%m%d")
			
			for cycle in [1, 2, 3]:
				# Skip if already downloaded in this session
				download_key = (date_str, cycle)
				if download_key in self._downloaded_data:
					self._logger.debug(f"Data for {date_str} cycle {cycle} already processed in this session")
					continue
				
				# Check persistent cache first with params
				string_api_params = {str(k): v for k, v in api_params.items()} if isinstance(api_params, dict) else {}
				if self._connector.is_in_cache(self._endpoint, cycle, date, **string_api_params):
					self._logger.info(f"Data for {self._endpoint} {date_str} cycle {cycle} already downloaded")
					# Add to downloaded data to avoid reprocessing
					self._downloaded_data.add(download_key)
					continue
				
				# Notify the connector with parameters
				self._logger.debug(f"Data for {self._endpoint} {date_str} cycle {cycle} not in cache, scheduling download")
				if cycle is not None and date is not None:
					self._connector.notify_cycle_update(cycle, date=date)
				self._downloaded_data.add(download_key)
	
	def _check_for_new_cycles(self) -> None:
		"""Check for newly available data for today based on current time."""
		current_hour = datetime.now().hour
		today = datetime.now()
		today_str = today.strftime("%Y%m%d")
		
		# Determine which cycles should be available now
		available_cycles = []
		if current_hour >= 15 and current_hour < 20:
			available_cycles.append(1)
		elif current_hour >= 20 and current_hour < 23:
			available_cycles.append(2)
		elif current_hour >= 23 or current_hour < 3:
			# Cycle 3 is available from 23:00 to 03:00 next day,
			# which is completely arbitrary like the other cycles definitions...
			available_cycles.append(3)
		
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
		
		# Check each potentially available cycle
		for cycle in available_cycles:
			# Skip if already downloaded in this session
			download_key = (today_str, cycle)
			if download_key in self._downloaded_data:
				self._logger.debug(f"Data for {self._endpoint} cycle {cycle} already processed in this session")
				continue
			
			# Check persistent cache before scheduling - PASS THE PARAMETERS HERE
			if self._connector.is_in_cache(self._endpoint, cycle, today, **string_api_params):
				cached_path = self._connector.get_cached_file_path(self._endpoint, cycle, today)
				self._logger.info(f"Data for {self._endpoint} {today_str} cycle {cycle} already in cache: {cached_path}")
				# Add to downloaded data to avoid reprocessing
				self._downloaded_data.add(download_key)
				continue
				
			# Notify the connector
			self._logger.info(f"New data available for {self._endpoint} cycle {cycle}, not in cache, scheduling download")
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
			self._logger.warning("Cycle monitor is already running")
			return False
			
		self._logger.info("Starting cycle monitor")
		self._running = True
		
		# Start monitoring in a separate thread
		self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
		self._thread.start()
		
		return self._thread.is_alive()
	
	def stop(self) -> None:
		"""Stop the cycle monitor."""
		if not self._running:
			self._logger.warning("Cycle monitor is not running")
			return
			
		self._logger.info("Stopping cycle monitor")
		self._running = False
		
		# Wait for thread to end
		if self._thread is not None and self._thread.is_alive():
			self._thread.join(timeout=5.0)
