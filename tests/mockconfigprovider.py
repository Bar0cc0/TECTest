#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# mockconfigprovider.py
# Shared test fixtures and utilities for all test modules

import pytest
import tempfile
import shutil
from unittest.mock import Mock
from pathlib import Path
import logging
from typing import Optional, Generator, Dict, Any

from src.interfaces import IConfigProvider


class MockConfigProvider(IConfigProvider):
	"""Mock implementation of IConfigProvider for testing."""

	def __init__(self, config_values: Optional[Dict[str, Any]] = None):
		self.config = config_values or {}
		self.logger = Mock(spec=logging.Logger)
		self.logger.debug = Mock()
		self.logger.info = Mock()
		self.logger.warning = Mock()
		self.logger.error = Mock()
	
	def get_logger(self) -> logging.Logger:
		"""Return a mock logger."""
		return self.logger

	def get_config(self, key: str, default: Optional[Any] = None) -> Any:
		"""Get a configuration value by key."""
		# First try direct key access
		if key in self.config:
			return self.config[key]
		
		# Try nested access
		for section_key, section in self.config.items():
			if isinstance(section, dict) and key in section:
				return section[key]
		
		return default

	def set_config(self, key: str, value: Optional[Any] = None) -> None:
		"""Set a configuration value by key."""
		self.config[key] = value


@pytest.fixture
def mock_config(temp_cache_dir: Path, temp_output_dir: Path) -> MockConfigProvider:
	"""Create a mock configuration for testing."""
	return MockConfigProvider({
		'API': {
			'base_url': 'https://test-api.example.com/',
			'routing_path': '/mockdata/',
			'asset_id': 'TEST',
			'format': 'csv',
			'search_type': 'Mock',
			'retry_attempts': 2,
			'timeout': 10
		},
		'Output': {
			'output_dir': str(temp_output_dir),
			'enable_cache': True,
			'cache_dir': str(temp_cache_dir),
			'cache_ttl': 3600
		},
		'Database': {
			'db_type': 'postgresql',
			'db_host': 'localhost',
			'db_port': 5432,
			'db_name': 'test_db',
			'db_user': 'test_user',
			'db_password': 'test_password'
		},
		'Scheduler': {
			'schedule_interval': 3600,
			'max_cycles': 3,
			'check_interval': 300
		}
	})


@pytest.fixture
def temp_cache_dir() -> Generator[Path, None, None]:
	"""Create a temporary directory for cache testing."""
	temp_dir = tempfile.mkdtemp()
	yield Path(temp_dir)
	# Cleanup after tests with error handling
	try:
		shutil.rmtree(temp_dir)
	except (FileNotFoundError, PermissionError):
		pass  # Directory may already be deleted


@pytest.fixture
def temp_output_dir() -> Generator[Path, None, None]:
	"""Create a temporary directory for output testing."""
	temp_dir = tempfile.mkdtemp()
	yield Path(temp_dir)
	# Cleanup after tests with error handling
	try:
		shutil.rmtree(temp_dir)
	except (FileNotFoundError, PermissionError):
		pass  # Directory may already be deleted
