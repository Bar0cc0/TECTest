#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# configuration.py
# This module provides a configuration manager for the application.

from __future__ import annotations
import logging
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

from interfaces import IConfigProvider


class Configuration(IConfigProvider):
	"""Configuration provider for the application."""

	_instance = None

	def __new__(cls, *args, **kwargs) -> Configuration:
		"""Ensure only one instance of Configuration exists."""
		if cls._instance is None:
			cls._instance = super(Configuration, cls).__new__(cls)
		return cls._instance
	
	def __init__(self, 
			  	config_path: Optional[str] = None, 
				root_dir: Optional[str] = None, 
				loglevel: Optional[str] = None
			) -> None:

		# Determine project root directory
		if root_dir:
			self._root = Path(root_dir).resolve()
		else:
			# Default: 1 level up from this script location
			script_dir = Path(__file__).resolve().parent
			self._root = script_dir.parents[0]
		
		# Store root in config for other components to access
		self._config = {'root_dir': str(self._root)}
		
		# Set config path relative to script dir if not absolute
		if config_path:
			config_path_obj = Path(config_path)
			if not config_path_obj.is_absolute():
				# If relative path provided, make it relative to script dir
				config_path_obj = Path(__file__).parent / config_path_obj
			self._config_path = config_path_obj
		else:
			# Default config is in the root directory
			self._config_path = self._root / 'config.yaml'
		
		# Update config with defaults using the established root
		self._config.update(self._load_default_config())

		# Load from file if available
		self.isLoadedFromFile = self.load_from_file()
		
		# Set up required directories
		self._initialize_directories()
		
		# Set up logging
		self._initialize_logging(loglevel)
		self._logger.info('Configuration initialized')

	def _initialize_directories(self) -> List[Path]:
		"""Create required directories."""
		paths_created = []
		
		for key, value in self._config.items():
			# Check if this is a directory path configuration
			if isinstance(value, Path) and key.endswith('_dir'):
				# Clean output directory between runs
				if key == 'output_dir' and value.exists():
					for item in value.iterdir():
						if item.is_file():
							try:
								item.unlink()
							except Exception as e:
								self._logger.error(f"Error deleting file {item}: {e}")
								continue
				
				# Create directory if it doesn't exist
				if not value.exists():
					try:
						value.mkdir(parents=True, exist_ok=True)
						paths_created.append(value)
					except Exception as e:
						self._logger.error(f"Error creating directory {value}: {e}")

		return paths_created
	
	def _initialize_logging(self, loglevel: Optional[str] = None) -> None:
		"""Initialize logging system."""
		log_dir = self._config.get('log_dir', 'Logs')
		
		# Convert to Path if it's a string
		if isinstance(log_dir, str):
			# If it's a relative path, make it relative to root dir
			if not log_dir.startswith('/'):
				log_dir = self._root / log_dir
		log_dir = Path(log_dir)

		# Create log directory if needed
		if not log_dir.exists():
			log_dir.mkdir(parents=True, exist_ok=True)
			
		log_file = log_dir / f"{datetime.now().strftime('%Y-%m-%d')}_events.log"
		
		# Configure the logger with the new format including dataset
		logging.basicConfig(
			level=logging.INFO,
			format='%(asctime)s - %(levelname)s - %(message)s',
			handlers=[
				logging.FileHandler(log_file),
				logging.StreamHandler()
			]
		)
		
		self._logger = logging.getLogger(__name__)
		
		# Set logger to debug level for detailed output
		if loglevel and loglevel.upper() == 'DEBUG':
			self._logger.setLevel(logging.DEBUG)
		else:
			self._logger.setLevel(logging.INFO)

		if self.isLoadedFromFile:
			self._logger.debug(f"Configuration loaded from {self._config_path}")
		else:
			self._logger.debug('Using default configuration')

	def _load_default_config(self) -> Dict[str, Any]:
		"""Load the default configuration."""
		return {
			# API settings
			'url': None,
			'token': None,
			'account': None,
			'timeout': 30,  # seconds
			'retry_attempts': 3,

			# Directories
			'output_dir': self._root / 'Data',
			'log_dir': self._root / 'Logs',
			'test_dir': self._root / 'tests',
			'cache_dir': self._root / 'Cache',

			# Optimization settings
			'enable_caching': False,
			'cache_ttl': 10,  # seconds
			'workers': 4,
			'batch_size': 1000,

			# Reporting
			'data_quality_report': True,
			'add_data_lineage': True
		}
	
	def get_logger(self) -> logging.Logger:
		"""Get logger instance."""
		return self._logger
	
	def get_config(self, key: str, default=None) -> Any:
		"""Get a configuration value with optional default."""
		value = self._config.get(key, default)
		if value is None:
			self._logger.warning(f"Configuration key '{key}' not found.")
		return value
	
	def set_config(self, key: str, value: Any) -> None:
		"""Set a configuration value."""
		self._config[key] = value
		
	def get_all_config(self) -> Dict[str, Any]:
		"""Get all configuration values."""
		return self._config.copy()  # Return a copy to prevent direct modification
	
	def load_from_file(self, config_path: Optional[str] = None) -> bool:
		"""Load configuration from a YAML file."""
		path = Path(config_path) if config_path else self._config_path
		if not path.exists():
			if self._logger:
				self._logger.warning(f"Config file not found: {path}. Using defaults.")
			return False
			
		try:
			with open(path, 'r') as f:
				# Load all documents in the YAML file
				all_docs = list(yaml.safe_load_all(f))
				
			# Merge all documents into a single configuration
			file_config = {}
			for doc in all_docs:
				if doc:  # Skip empty documents
					file_config.update(doc)

			# Process paths in Directories
			if 'Directories' in file_config and isinstance(file_config['Directories'], dict):
				dir_config = file_config['Directories']
				# Process each directory path in the dictionary
				for key, value in dir_config.items():
					if isinstance(value, str):
						# Convert to Path object by joining with root dir
						dir_config[key] = self._root / value
				
				# Flatten Directories to top level
				file_config.update(dir_config)
				# Remove the nested Directories dictionary
				file_config.pop('Directories')
					
			# Flatten API section
			if 'API' in file_config and isinstance(file_config['API'], dict):
				top_level_api = file_config.pop('API')
				file_config.update(top_level_api)
				
			# Flatten Reporting section
			if 'Reporting' in file_config and isinstance(file_config['Reporting'], dict):
				top_level_reporting = file_config.pop('Reporting')
				file_config.update(top_level_reporting)
			
			# Flatten Optimization section
			if 'Optimization' in file_config and isinstance(file_config['Optimization'], dict):
				top_level_opt = file_config.pop('Optimization')
				file_config.update(top_level_opt)

			# Update config
			self._config.update(file_config)
			
			return True
			
		except Exception as e:
			if self._logger:
				self._logger.error(f"Error loading config: {e}")
			return False
	