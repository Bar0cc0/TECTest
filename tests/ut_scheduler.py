#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# test_scheduler.py
# Unit tests for the scheduler module

import pytest
import asyncio
from datetime import datetime, timedelta
import threading
import json
from pathlib import Path
import tempfile
import shutil
import time
from unittest.mock import Mock, MagicMock, patch, AsyncMock, call, ANY

from src.scheduler import DataScheduler, CycleMonitor
from src.interfaces import IConfigProvider
from src.connectors import WebConnector, DatabaseConnector
from tests.mockconfigprovider import MockConfigProvider


@pytest.fixture
def mock_web_connector():
	"""Create a mock web connector for testing."""
	connector = Mock(spec=WebConnector)
	
	# Setup mock methods with appropriate return values
	connector.fetch_data_with_retry = AsyncMock(return_value=(True, [Path("test_file.csv")]))
	connector.is_in_cache = Mock(return_value=False)
	connector.get_cached_file_path = Mock(return_value=Path("cached_file.csv"))
	connector.register_cycle_callback = Mock()
	connector.notify_cycle_update = Mock()
	connector.last_response_from_cache = False
	
	return connector

@pytest.fixture
def mock_db_connector():
	"""Create a mock database connector for testing."""
	connector = Mock(spec=DatabaseConnector)
	
	# Setup mock methods with appropriate return values
	connector.post_data = Mock(return_value=True)
	connector.close = Mock()
	connector._queue = MagicMock()
	connector._queue.join = Mock()
	
	return connector

@pytest.fixture
def mock_config():
	"""Create a mock config provider for testing."""
	config = MockConfigProvider()
	
	# Set default configuration values
	config.set_config('workers', 2)
	config.set_config('schedule_interval_hours', 1.0)
	config.set_config('enable_database_loading', True)
	config.set_config('data_quality_report', True)
	config.set_config('asset_id', 'TW')

	api_endpoints_config = {
		"test/endpoint": { 
			"endpoint_specific_part": "test/endpoint", 
			"table_name": "test_table",               
			"params": {}                       
		},
		"capacity/operationally-available": { 
			"endpoint_specific_part": "operationally-available",
			"table_name": "capacity_operational",
			"params": {"searchType": "CapacitySearch"} 
		}
	}
	config.set_config('api_endpoints', api_endpoints_config)

	config.set_config('endpoint_table_mappings', {
		'test/endpoint': 'test_table', 
		'operationally-available': 'capacity_operational'
	})

	config.set_config('output_dir', str(Path(tempfile.mkdtemp()) / "output"))
	
	# Create the output directory
	Path(config.get_config('output_dir')).mkdir(parents=True, exist_ok=True)
	
	# Add metadata directory
	metadata_dir = Path(config.get_config('output_dir')) / "metadata"
	metadata_dir.mkdir(exist_ok=True)
	
	yield config
	
	# Clean up the temporary directory
	try:
		shutil.rmtree(Path(config.get_config('output_dir')).parent)
	except:
		pass

@pytest.fixture
def mock_data_processor_factory():
	"""Create a mock data processor factory."""
	with patch('src.scheduler.DataProcessorFactory') as factory:
		pipeline_mock = MagicMock()
		pipeline_mock.process = MagicMock(return_value=Path("processed_file.csv"))
		factory.create_pipeline.return_value = pipeline_mock
		yield factory

@pytest.fixture
def mock_data_quality_checker_factory():
	"""Create a mock data quality checker factory."""
	with patch('src.scheduler.DataQualityCheckerFactory') as factory:
		checker_mock = MagicMock()
		report_mock = MagicMock()
		report_mock.passed = True
		report_mock.quality_score = 0.95
		checker_mock.check_data_quality.return_value = report_mock
		checker_mock.save_report.return_value = Path("quality_report.json")
		factory.create_checker.return_value = checker_mock
		yield factory

@pytest.fixture
def data_scheduler(mock_config, mock_web_connector, mock_db_connector):
	"""Create a DataScheduler instance for testing."""
	scheduler = DataScheduler(mock_config, mock_web_connector, mock_db_connector)
	
	# Patch the event loop methods to avoid actually running an event loop
	scheduler._event_loop = MagicMock()
	scheduler._thread = MagicMock()
	scheduler._running = True

	yield scheduler
	
	# Clean up
	scheduler.stop()

@pytest.fixture
def cycle_monitor(mock_config, mock_web_connector):
	"""Create a CycleMonitor instance for testing."""
	monitor = CycleMonitor(
		mock_config, 
		mock_web_connector, 
		endpoint="test/endpoint"
	)
	
	yield monitor
	
	# Clean up
	monitor.stop()


class TestDataScheduler:
	"""Tests for the DataScheduler class."""
	
	def test_initialization(self, mock_config, mock_web_connector, mock_db_connector):
		"""Test scheduler initialization."""
		scheduler = DataScheduler(mock_config, mock_web_connector, mock_db_connector)
		
		# Verify the connector's callback was registered
		mock_web_connector.register_cycle_callback.assert_called_once()
		assert scheduler._scheduled_tasks == {}
		assert scheduler._scheduled_endpoints == set()
		assert scheduler._cycle_data_tasks == {}
		assert scheduler._running is False
		assert scheduler._workers == 2
		
		# Clean up
		scheduler.stop()
	
	def test_handle_cycle_update_cached(self, data_scheduler, mock_web_connector):
		"""Test _handle_cycle_update when data is already in cache."""
		date = datetime(2025, 6, 8)
		endpoint = "test/endpoint"
		cycle = 1
		
		# Setup cache hit
		mock_web_connector.is_in_cache.return_value = True
		
		# Register the endpoint
		data_scheduler._scheduled_endpoints.add(endpoint)
		
		# Call the handler
		data_scheduler._handle_cycle_update(cycle, date)
		
		# Verify behavior for cached data
		mock_web_connector.is_in_cache.assert_called_with(endpoint, cycle, date)
		mock_web_connector.get_cached_file_path.assert_called_with(endpoint, cycle, date)
		
		# No task should be created for cached data
		assert endpoint not in data_scheduler._cycle_data_tasks
	
	def test_handle_cycle_update_new_task(self, data_scheduler, mock_web_connector):
		"""Test _handle_cycle_update schedules a new task."""
		date = datetime(2025, 6, 8)
		endpoint = "test/endpoint"
		cycle = 1
		
		# Setup cache miss
		mock_web_connector.is_in_cache.return_value = False
		
		# Register the endpoint
		data_scheduler._scheduled_endpoints.add(endpoint)
		
		# Patch asyncio.run_coroutine_threadsafe
		with patch('asyncio.run_coroutine_threadsafe') as mock_run:
			# Call the handler
			data_scheduler._handle_cycle_update(cycle, date)
			
			# Verify a new task was scheduled
			assert mock_run.called
			# First arg should be a coroutine (_fetch_endpoint_data)
			coro_obj = mock_run.call_args[0][0]
			assert asyncio.iscoroutine(coro_obj)
			# Second arg should be the event loop
			assert mock_run.call_args[0][1] == data_scheduler._event_loop
	
	@pytest.mark.asyncio
	async def test_fetch_endpoint_data(self, data_scheduler, mock_web_connector, 
									mock_db_connector, mock_data_processor_factory):
		"""Test _fetch_endpoint_data successfully retrieves and processes data,
		correctly using endpoint configuration."""
		endpoint_alias = "test/endpoint" # The alias passed to DataScheduler
		cycle = 1
		date = datetime(2025, 6, 8)
		
		# Create a test file
		test_file = Path("test_file.csv")
		mock_web_connector.fetch_data_with_retry.return_value = (True, [test_file])
		
		# Call the method with the endpoint alias
		success, files = await data_scheduler._fetch_endpoint_data(endpoint_alias, cycle, date)
		
		# Verify results
		assert success is True, "DataScheduler._fetch_endpoint_data should return True on success"
		assert len(files) == 1
		assert files[0] == test_file
		
		# Define what DataScheduler should resolve from its config for the given alias
		# Based on the mock_config fixture:
		# config.set_config('API.endpoints', {
		#     "test/endpoint": {
		#         "endpoint_specific_part": "test/endpoint",
		#         "table_name": "test_table",
		#         "params": {}
		#     }, ...
		# })
		expected_webconnector_endpoint_part = "test/endpoint" # Resolved from API.endpoints.test/endpoint.endpoint_specific_part
		expected_webconnector_params = {}                   # Resolved from API.endpoints.test/endpoint.params

		# Verify the web connector was called with the resolved specific part and params
		mock_web_connector.fetch_data_with_retry.assert_called_once_with(
			expected_webconnector_endpoint_part, 
			cycle, 
			date=date,
			**expected_webconnector_params # Spreading an empty dict is fine
		)
		
		# Verify processor factory was called
		mock_data_processor_factory.create_pipeline.assert_called_once()
		
		# Verify DB connector was called
		mock_db_connector.post_data.assert_called_once()
	
	@pytest.mark.asyncio
	async def test_fetch_endpoint_data_with_cache(self, data_scheduler, mock_web_connector,
												mock_db_connector, mock_data_processor_factory):
		"""Test _fetch_endpoint_data with cached files."""
		endpoint = "test/endpoint"
		cycle = 1
		date = datetime(2025, 6, 8)
		
		# Create a test file
		test_file = Path("test_file.csv")
		mock_web_connector.fetch_data_with_retry.return_value = (True, [test_file])
		
		# Set response from cache
		mock_web_connector.last_response_from_cache = True
		
		# Call the method
		success, files = await data_scheduler._fetch_endpoint_data(endpoint, cycle, date)
		
		# Verify results
		assert success is True
		assert len(files) == 1
		
		# Verify the processor was NOT called for cached files
		mock_data_processor_factory.create_pipeline.assert_not_called()
		
		# Verify DB connector was NOT called for cached files
		mock_db_connector.post_data.assert_not_called()
	
	@pytest.mark.asyncio
	async def test_fetch_endpoint_data_failure(self, data_scheduler, mock_web_connector):
		"""Test _fetch_endpoint_data when data retrieval fails."""
		endpoint = "test/endpoint"
		cycle = 1
		date = datetime(2025, 6, 8)
		
		# Setup failure response
		mock_web_connector.fetch_data_with_retry.return_value = (False, [])
		
		# Call the method
		success, files = await data_scheduler._fetch_endpoint_data(endpoint, cycle, date)
		
		# Verify results
		assert success is False
		assert len(files) == 0
	
	@pytest.mark.asyncio
	async def test_process_data_quality(self, data_scheduler, mock_data_quality_checker_factory):
		"""Test _process_data_quality method."""
		endpoint = "test/endpoint"
		files = [Path("test_file.csv")]
		
		# Setup mock file with sufficient size
		with patch('pathlib.Path.stat') as mock_stat:
			mock_stat.return_value.st_size = 1000  # Large enough to pass check
			
			# Call the method
			await data_scheduler._process_data_quality(endpoint, files)
			
			# Verify quality checker was created and used
			mock_data_quality_checker_factory.create_checker.assert_called_once()
			checker = mock_data_quality_checker_factory.create_checker.return_value
			checker.check_data_quality.assert_called_once_with(files[0])
			checker.save_report.assert_called_once()
	
	def test_schedule_endpoint(self, data_scheduler):
		"""Test schedule_endpoint method."""
		endpoint = "test/endpoint"
		params = {"param1": "value1"}
		
		# Patch asyncio.run_coroutine_threadsafe
		with patch('asyncio.run_coroutine_threadsafe') as mock_run:
			# Call the method
			result = data_scheduler.schedule_endpoint(endpoint, params)
			
			# Verify results
			assert result is True
			assert endpoint in data_scheduler._scheduled_endpoints
			assert data_scheduler._endpoint_params == params
			assert mock_run.called
	
	def test_unschedule_endpoint(self, data_scheduler):
		"""Test unschedule_endpoint method."""
		endpoint = "test/endpoint"
		
		# Setup a mock task
		mock_task = MagicMock()
		data_scheduler._scheduled_tasks[endpoint] = mock_task
		data_scheduler._scheduled_endpoints.add(endpoint)
		
		# Call the method
		result = data_scheduler.unschedule_endpoint(endpoint)
		
		# Verify results
		assert result is True
		assert endpoint not in data_scheduler._scheduled_tasks
		assert endpoint not in data_scheduler._scheduled_endpoints
		mock_task.cancel.assert_called_once()

	def test_get_table_name_for_endpoint(self, data_scheduler):
		"""Test _get_table_name_for_endpoint method."""
		# Test with mapping
		table_name = data_scheduler._get_table_name_for_endpoint("test/endpoint")
		assert table_name == "test_table"
		
		# Test with normalized endpoint (no mapping)
		table_name = data_scheduler._get_table_name_for_endpoint("unmapped/endpoint")
		assert table_name == "unmapped_endpoint"
	
	@pytest.mark.asyncio
	async def test_load_to_database(self, data_scheduler, mock_db_connector):
		"""Test load_to_database method."""
		table_name = "test_table"
		files = [Path("file1.csv"), Path("file2.csv")]
		cycle = 1
		date = datetime(2025, 6, 8)
		
		# Call the method
		result = await data_scheduler.load_to_database(table_name, files, cycle, date)
		
		# Verify results
		assert result is True
		mock_db_connector.post_data.assert_called_with(table_name, files, cycle, date)
	
	def test_start(self, mock_config, mock_web_connector, mock_db_connector):
		"""Test start method."""
		scheduler = DataScheduler(mock_config, mock_web_connector, mock_db_connector)
		
		# Patch the thread creation
		with patch('threading.Thread') as mock_thread:
			mock_thread_instance = MagicMock()
			mock_thread.return_value = mock_thread_instance
			
			# Set the return value for is_alive
			mock_thread_instance.is_alive.return_value = True
			
			# Patch event loop setup
			with patch.object(scheduler, '_event_loop', None):
				# Simulate event loop being created quickly
				def thread_start_side_effect():
					scheduler._event_loop = MagicMock()
				
				mock_thread_instance.start.side_effect = thread_start_side_effect
				
				# Call the method
				result = scheduler.start()
				
				# The start method is returning the is_alive() result, not True directly
				assert result is mock_thread_instance.is_alive()
				
				# More important assertions that verify the method worked correctly
				assert scheduler._running is True
				mock_thread.assert_called_once()
				mock_thread_instance.start.assert_called_once()
		
		# Clean up
		scheduler.stop()
	
	def test_stop(self, data_scheduler, mock_db_connector):
		"""Test stop method."""
		# Setup mock tasks
		endpoint = "test/endpoint"
		mock_task = MagicMock()
		data_scheduler._scheduled_tasks[endpoint] = mock_task
		
		# Setup cycle tasks
		cycle_task = MagicMock()
		data_scheduler._cycle_data_tasks = {
			endpoint: {
				1: {
					"20250608": cycle_task
				}
			}
		}
		
		# Call the method
		data_scheduler.stop()
		
		# Verify results
		assert data_scheduler._running is False
		mock_task.cancel.assert_called_once()
		cycle_task.cancel.assert_called_once()
		mock_db_connector._queue.join.assert_called_once()
		mock_db_connector.close.assert_called_once()
	
	def test_scheduled_task_cancellation(self, data_scheduler):
		"""Test that scheduled task handles cancellation correctly."""
		endpoint = "test/endpoint"
		interval_hours = 1.0
		
		# Mock logger to capture log messages
		mock_logger = MagicMock()
		data_scheduler._logger = mock_logger
		
		# Setup tracking variable to verify the coroutine ran
		finished_running = False
		
		async def run_task_with_cancel():
			nonlocal finished_running
			# Start the task
			task = asyncio.create_task(data_scheduler._scheduled_task(endpoint, interval_hours))
			# Give it a moment to start
			await asyncio.sleep(0.01)
			# Cancel the task
			task.cancel()
			try:
				# Wait for it to finish
				await task
				# If we get here, the task handled cancellation gracefully
				finished_running = True
			except asyncio.CancelledError:
				# The task propagated the cancellation
				pass
		
		# Run our test coroutine
		asyncio.run(run_task_with_cancel())
		
		# Verify that either:
		# 1. The task completed (handled cancellation internally)
		# 2. The logger recorded the cancellation
		assert finished_running or mock_logger.debug.called or mock_logger.info.called

	@pytest.mark.asyncio
	async def test_process_data_quality_failure(self, data_scheduler, mock_data_quality_checker_factory):
		"""Test _process_data_quality when quality check fails."""
		endpoint = "test/endpoint"
		files = [Path("test_file.csv")]
		
		# Setup mock file
		with patch('pathlib.Path.stat') as mock_stat:
			mock_stat.return_value.st_size = 1000
			
			# Setup quality check failure
			checker_mock = mock_data_quality_checker_factory.create_checker.return_value
			report_mock = MagicMock()
			report_mock.passed = False
			report_mock.quality_score = 0.3
			checker_mock.check_data_quality.return_value = report_mock
			
			# Call the method
			await data_scheduler._process_data_quality(endpoint, files)
			
			# Verify quality checker was used
			mock_data_quality_checker_factory.create_checker.assert_called_once()
			checker_mock.check_data_quality.assert_called_once()
			
			# Even with failure, report should be saved
			checker_mock.save_report.assert_called_once()

class TestCycleMonitor:
	"""Tests for the CycleMonitor class."""
	
	def test_initialization(self, mock_config, mock_web_connector):
		"""Test cycle monitor initialization."""
		monitor = CycleMonitor(
			mock_config, 
			mock_web_connector, 
			endpoint="test/endpoint"
		)
		
		assert monitor._check_interval == 600  # 10 minutes in seconds
		assert monitor._endpoint == "test/endpoint"
		assert monitor._running is False
		assert monitor._downloaded_data == set()
		
		# Clean up
		monitor.stop()
	
	def test_check_historical_data(self, cycle_monitor, mock_web_connector):
		"""Test _check_historical_data method."""
		# Set history to a small value for testing
		cycle_monitor._history = 2
		
		# Setup cache miss to ensure connector is called
		mock_web_connector.is_in_cache.return_value = False
		
		# Call the method
		cycle_monitor._check_historical_data()
		
		# Verify notifications were sent for each day and cycle
		assert mock_web_connector.notify_cycle_update.call_count > 0
		
		# Verify downloaded data was tracked
		assert len(cycle_monitor._downloaded_data) > 0
	
	def test_check_historical_data_with_cache(self, cycle_monitor, mock_web_connector, mock_config):
		"""Test _check_historical_data when data is in cache."""
		# Set history to a small value for testing
		cycle_monitor._history = 1
		
		# Setup cache hit with metadata
		mock_web_connector.is_in_cache.return_value = True
		mock_web_connector.get_cached_file_path.return_value = Path("cached_file.csv")
		
		# Create metadata file
		metadata_dir = Path(mock_config.get_config('output_dir')) / "metadata"
		metadata_file = metadata_dir / "cached_file.meta.json"
		metadata = {
			"cycle_name": "Intraday 1",
			"date": "20250608",
			"endpoint": "test/endpoint"
		}
		
		with open(metadata_file, 'w') as f:
			json.dump(metadata, f)
		
		# Call the method
		cycle_monitor._check_historical_data()
		
		# Verify cache was checked
		assert mock_web_connector.is_in_cache.called
		
		# Verify connector was not notified for cached data
		assert mock_web_connector.notify_cycle_update.call_count == 0
		
		# Cleanup
		metadata_file.unlink()
	
	def test_check_for_new_cycles(self, cycle_monitor, mock_web_connector, mock_config):
		"""Test _check_for_new_cycles method."""
		# Setup cache miss to ensure connector is called
		mock_web_connector.is_in_cache.return_value = False
		
		# Call the method
		cycle_monitor._check_for_new_cycles()
		
		# Verify notifications were sent for current cycles
		assert mock_web_connector.notify_cycle_update.call_count > 0
		
		# Verify downloaded data was tracked
		assert len(cycle_monitor._downloaded_data) > 0
	
	def test_check_for_new_cycles_with_cache(self, cycle_monitor, mock_web_connector, mock_config):
		"""Test _check_for_new_cycles when data is in cache."""
		# Setup cache hit with metadata
		mock_web_connector.is_in_cache.return_value = True
		mock_web_connector.get_cached_file_path.return_value = Path("cached_file.csv")
		
		# Create metadata file
		metadata_dir = Path(mock_config.get_config('output_dir')) / "metadata"
		metadata_file = metadata_dir / "cached_file.meta.json"
		metadata = {
			"cycle_name": "Intraday 1",
			"date": datetime.now().strftime("%Y%m%d"),
			"endpoint": "test/endpoint"
		}
		
		with open(metadata_file, 'w') as f:
			json.dump(metadata, f)
		
		# Call the method
		cycle_monitor._check_for_new_cycles()
		
		# Verify cache was checked
		assert mock_web_connector.is_in_cache.called
		
		# Verify connector was not notified for cached data
		assert mock_web_connector.notify_cycle_update.call_count == 0
		
		# Cleanup
		metadata_file.unlink()
	
	def test_start(self, mock_config, mock_web_connector, mock_db_connector):
		"""Test start method."""
		scheduler = DataScheduler(mock_config, mock_web_connector, mock_db_connector)
		
		# Patch the thread creation
		with patch('threading.Thread') as mock_thread:
			mock_thread_instance = MagicMock()
			mock_thread.return_value = mock_thread_instance
			
			# Set the return value for is_alive
			mock_thread_instance.is_alive.return_value = True
			
			# Patch event loop setup
			with patch.object(scheduler, '_event_loop', None):
				# Simulate event loop being created quickly
				def thread_start_side_effect():
					scheduler._event_loop = MagicMock()
				
				mock_thread_instance.start.side_effect = thread_start_side_effect
				
				# Call the method
				result = scheduler.start()
				
				# The start method is returning the is_alive() result, not True directly
				assert result is mock_thread_instance.is_alive()
				
				# More important assertions that verify the method worked correctly
				assert scheduler._running is True
				mock_thread.assert_called_once()
				mock_thread_instance.start.assert_called_once()
		
		# Clean up
		scheduler.stop()
		
	def test_stop(self, cycle_monitor):
		"""Test stop method."""
		# Setup running state and mock thread
		cycle_monitor._running = True
		cycle_monitor._thread = MagicMock()
		
		# Call the method
		cycle_monitor.stop()
		
		# Verify results
		assert cycle_monitor._running is False
		cycle_monitor._thread.join.assert_called_once()
	
	def test_monitor_loop(self, cycle_monitor):
		"""Test the _monitor_loop method."""
		# Patch the check methods
		with patch.object(cycle_monitor, '_check_historical_data') as mock_historical:
			with patch.object(cycle_monitor, '_check_for_new_cycles') as mock_new_cycles:
				# Patch threading.Event to make it run quickly
				with patch('threading.Event') as mock_event:
					mock_event_instance = MagicMock()
					mock_event.return_value = mock_event_instance
					
					# Set up monitor to run for a short time
					cycle_monitor._running = True
					
					def wait_side_effect(seconds):
						# Stop after first iteration
						cycle_monitor._running = False
					
					mock_event_instance.wait.side_effect = wait_side_effect
					
					# Run the monitor loop
					cycle_monitor._monitor_loop()
					
					# Verify both check methods were called
					mock_historical.assert_called_once()
					mock_new_cycles.assert_called_once()
