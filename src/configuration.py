#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# configuration.py
# This module provides a configuration manager for the application.

from __future__ import annotations
import logging
import yaml
import os
import tempfile
import uuid
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

from interfaces import IConfigProvider


class Configuration(IConfigProvider):
	"""
	Configuration provider for the application.
	Handles loading from defaults, YAML file, and environment variable overrides.
	Manages logging setup.
	"""

	_instance = None
	_initialized_fully = False

	def __new__(cls, *args, **kwargs) -> Configuration:
		"""Ensure only one instance of Configuration exists."""
		if cls._instance is None:
			cls._instance = super(Configuration, cls).__new__(cls)
			cls._initialized_fully = False
		return cls._instance
	
	def __init__(self, 
				config_path_override: Optional[str] = None, 
				root_dir_override: Optional[str] = None, 
				loglevel: Optional[str] = None
			) -> None:

		if self._initialized_fully:
			if loglevel and hasattr(self, '_logger') and self._logger:
				self._update_loglevel(loglevel)
			return

		# Early logger setup for use during initialization
		self._logger = logging.getLogger(__name__)

		# if logger is used before full configuration
		self._null_handler = logging.NullHandler()
		self._logger.addHandler(self._null_handler)
		self._logger.setLevel(logging.DEBUG) # Capture all early messages if needed later

		# Determine project root directory
		if root_dir_override:
			self._root = Path(root_dir_override).resolve()
		else:
			script_dir = Path(__file__).resolve().parent
			self._root = script_dir.parents[0] # Assumes src is one level below project root
		
		self._config: Dict[str, Any] = {'root_dir': str(self._root)}
		
		# Determine config file path
		if config_path_override:
			config_path_obj = Path(config_path_override)
			if not config_path_obj.is_absolute():
				config_path_obj = self._root / config_path_obj # Relative to project root
			self._config_path = config_path_obj.resolve()
		else:
			self._config_path = (self._root / 'config.yaml').resolve()

		# Load configurations in order: defaults -> file -> env overrides
		self._config.update(self._load_default_config())
		self.isLoadedFromFile = self.load_from_file(self._config_path)
		self._apply_env_overrides()

		# Initialize directories based on final config
		self._initialize_directories()

		# Finalize logging setup
		effective_loglevel = loglevel
		if not effective_loglevel and 'loglevel' in self._config:
			effective_loglevel = str(self._config['loglevel'])
		self._initialize_logging(effective_loglevel) 

		self._logger.info(f"Configuration initialized") 
		self._logger.debug(f"""Configuration parameters: 
					Root: {self._root}, 
					Config: {self._config_path if self.isLoadedFromFile else 'Defaults only'}""")
		self._initialized_fully = True

	def _load_default_config(self) -> Dict[str, Any]:
		"""Load the default configuration values."""
		return {		
			# API connection settings
			'url': '', 'routing_path':'', 'format':'',
			'asset_id': '', 'endpoint': '', 'search_type': '',
			'api_key': '', 'token': '', 'account': '',
			'headers': {}, 'params': {}, 'verify_ssl': True,
			'timeout': 30, 'retry_attempts': 3, 
			'http_session_pool_size': 5, 'max_concurrent_requests': 10,

			# Database settings
			'db_type': '', 'db_host': 'localhost', 'db_port': 5432,
			'db_name': '', 'db_user': '', 'db_password': '',

			# Directories (as Path objects)
			'output_dir': self._root / 'data',
			'log_dir': self._root / 'logs',
			'test_dir': self._root / 'tests',
			'cache_dir': self._root / 'cache',

			# Optimization settings
			'enable_caching': False, 'cache_ttl': 10,
			'workers': 4, 'batch_size': 1000, 'trigger_delay': 1,

			# Reporting
			'data_quality_report': True, 'add_data_lineage': True,

			# Logging
			'loglevel': 'INFO'
		}

	def load_from_file(self, config_file_path: Path) -> bool:
		"""Load configuration from a YAML file, updating self._config."""
		if not config_file_path.exists():
			self._logger.warning(f"Config file not found: {config_file_path}. Using defaults and environment variables.")
			return False
			
		try:
			with open(config_file_path, 'r') as f:
				all_docs = list(yaml.safe_load_all(f))
				
			file_config: Dict[str, Any] = {}
			for doc in all_docs:
				if isinstance(doc, dict):
					file_config.update(doc)

			# Helper to flatten nested sections
			def flatten_section(parent_config: Dict[str, Any], section_name: str):
				if section_name in parent_config and isinstance(parent_config[section_name], dict):
					section_data = parent_config.pop(section_name)
					parent_config.update(section_data)
			
			# Flatten known sections (API, Database, Directories, Optimization, Reporting)
			for section in ['API', 'Database', 'Directories', 'Optimization', 'Reporting']:
				flatten_section(file_config, section)

			# Process directory paths from file_config to be Path objects relative to root
			for key, value in file_config.items():
				if key.endswith('_dir') and isinstance(value, str):
					path_obj = Path(value)
					if not path_obj.is_absolute():
						file_config[key] = (self._root / path_obj).resolve()
					else:
						file_config[key] = path_obj.resolve()
			
			self._config.update(file_config)
			self._logger.debug(f"Configuration successfully loaded from {config_file_path}")
			return True
			
		except Exception as e:
			self._logger.error(f"Error loading config file {config_file_path}: {e}", exc_info=True)
			return False

	def _apply_env_overrides(self) -> None:
		"""Apply environment variable overrides to self._config."""
		self._logger.debug('Applying environment variable overrides')

		# Database settings
		db_env_map = {
			'DB_HOST': 'db_host', 'DB_PORT': 'db_port', 'DB_NAME': 'db_name',
			'DB_USER': 'db_user', 'DB_PASSWORD': 'db_password', 'DB_TYPE': 'db_type'
		}
		for env_var, config_key in db_env_map.items():
			value = os.getenv(env_var)
			if value is not None:
				if config_key == 'db_port':
					try:
						self._config[config_key] = int(value)
						self._logger.debug(f"Overridden '{config_key}' with env var '{env_var}': {self._config[config_key]}")
					except ValueError:
						self._logger.warning(f"Invalid environment variable for {env_var} (expected int): '{value}'. Using existing value for '{config_key}'.")
				else:
					self._config[config_key] = value
					self._logger.debug(f"Overridden '{config_key}' with env var '{env_var}': {value}")
		
		# Logging level
		env_loglevel = os.getenv('LOG_LEVEL')
		if env_loglevel is not None:
			self._config['loglevel'] = env_loglevel.upper()
			self._logger.debug(f"Overridden 'loglevel' with env var 'LOG_LEVEL': {self._config['loglevel']}")

	def _initialize_directories(self) -> None:
		"""Create required directories defined in self._config if they are Path objects and end with _dir."""
		self._logger.debug('Initializing directories')
		
		for key, value in self._config.items():
			if isinstance(value, Path) and key.endswith('_dir'):
				self._logger.debug(f"Checking directory for '{key}': {value}")

				if key == 'output_dir' and value.exists():
					self._logger.info(f"Cleaning output directory: {value}")
					for item in value.iterdir():
						if item.is_file():
							try: item.unlink()
							except Exception as e: self._logger.error(f"Error deleting file {item}: {e}")
				
				if not value.exists():
					try:
						value.mkdir(parents=True, exist_ok=True)
						self._logger.info(f"Created directory: {value}")
					except Exception as e:
						self._logger.error(f"Error creating directory {value}: {e}", exc_info=True)

	def _initialize_logging(self, loglevel_param: Optional[str] = None) -> None:
		"""
		Initialize logging system.
		Log level precedence: constructor param -> LOG_LEVEL env -> config file -> default.
		Log directory precedence: config file -> default.
		"""
		# Remove the initial NullHandler used during early initialization
		if hasattr(self, '_null_handler') and self._null_handler:
			self._logger.removeHandler(self._null_handler)
			self._null_handler = None

		# Determine Log Directory
		final_log_dir: Optional[Path] = None
		log_dir_source = 'unknown'

		# Get from config
		config_log_dir = self._config.get('log_dir')
		if isinstance(config_log_dir, Path):
			final_log_dir = config_log_dir.resolve()
			log_dir_source = "config file ('log_dir')"
		elif isinstance(config_log_dir, str): # Should not happen if config loading is correct
			self._logger.warning(f"'log_dir' in config is a string, should be Path. Resolving '{config_log_dir}' relative to root.")
			path_obj = Path(config_log_dir)
			final_log_dir = (self._root / path_obj).resolve() if not path_obj.is_absolute() else path_obj.resolve()
			log_dir_source = "config file ('log_dir' as string)"
		else: # Fallback to default if not in config or wrong type
			final_log_dir = (self._root / 'logs').resolve()
			log_dir_source = "hardcoded default (root/logs)"

		# Update self._config
		if final_log_dir:
			self._config['log_dir'] = str(final_log_dir)

		# Create Log Directory
		log_file: Optional[Path] = None
		if final_log_dir:
			try:
				final_log_dir.mkdir(parents=True, exist_ok=True)
				log_file = final_log_dir / f"{datetime.now().strftime('%Y-%m-%d')}_events.log"
			except Exception as e:
				self._logger.error(f"Could not create/access primary log directory {final_log_dir} (from {log_dir_source}): {e}. Attempting fallback.", exc_info=True)
				try:
					fallback_base = Path(tempfile.gettempdir())
					try:
						unique_id = str(os.getuid())
					except AttributeError:
						unique_id = uuid.uuid4().hex[:8]
					final_log_dir = fallback_base / f"tec_app_logs_{unique_id}"
					final_log_dir.mkdir(parents=True, exist_ok=True)
					self._config['log_dir'] = str(final_log_dir) # Update config with fallback path
					log_file = final_log_dir / f"{datetime.now().strftime('%Y-%m-%d')}_events.log"
					log_dir_source = f"fallback directory ({final_log_dir})"
					self._logger.info(f"Using fallback log directory: {final_log_dir}")
				except Exception as fallback_e:
					self._logger.critical(f"Could not create fallback log directory {final_log_dir}: {fallback_e}. File logging disabled.", exc_info=True)
					final_log_dir = None # Disable file logging
					self._config['log_dir'] = '' # Indicate no file log dir

		# Determine Effective Log Level
		level_to_set_str = 'INFO' # Default
		if 'loglevel' in self._config and isinstance(self._config['loglevel'], str):
			level_to_set_str = self._config['loglevel'].upper() # From config (possibly env override)
		if loglevel_param:
			level_to_set_str = loglevel_param.upper()

		effective_loglevel_val = getattr(logging, level_to_set_str, logging.INFO)
		if not isinstance(effective_loglevel_val, int): # Safety check if getattr failed
			self._logger.warning(f"Invalid log level string '{level_to_set_str}'. Defaulting to INFO.")
			effective_loglevel_val = logging.INFO


		# Configure Handlers
		handlers_list: List[logging.Handler] = []
		file_handler_active = False
		if log_file:
			try:
				file_h = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7, encoding='utf-8')
				file_h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'))
				handlers_list.append(file_h)
				file_handler_active = True
			except Exception as e:
				self._logger.error(f"Failed to initialize file handler for {log_file}: {e}", exc_info=True)
		
		stream_h = logging.StreamHandler()
		stream_h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
		handlers_list.append(stream_h)

		# Configure Root Logger
		root_logger = logging.getLogger() 

		# Remove all existing handlers from the root logger to avoid duplication
		for h in root_logger.handlers[:]:
			root_logger.removeHandler(h)
			h.close()
		
		root_logger.setLevel(effective_loglevel_val)
		for h in handlers_list:
			root_logger.addHandler(h)

		# Set self._logger to the configured logger for this class
		# self._logger was already logging.getLogger(__name__), now ensure its level is also set.
		self._logger.setLevel(effective_loglevel_val)

		log_file_path_msg = str(log_file) if file_handler_active else 'Disabled'
		self._logger.info(f"Logging initialized")
		self._logger.debug(f"""Logging parameters:
				Source: {log_dir_source},
				Level: {logging.getLevelName(effective_loglevel_val)}, 
				Log file: {log_file_path_msg}""")

	def _update_loglevel(self, new_loglevel_str: str) -> None:
		"""Dynamically updates the log level for the application's logger and root logger."""
		if not hasattr(self, '_logger') or not self._logger:
			return # Logger not initialized

		new_level_val = getattr(logging, new_loglevel_str.upper(), None)
		if new_level_val is None or not isinstance(new_level_val, int):
			self._logger.warning(f"Attempted to update to invalid log level: {new_loglevel_str}")
			return

		current_root_level = logging.getLogger().level
		current_app_logger_level = self._logger.level

		if current_root_level != new_level_val:
			logging.getLogger().setLevel(new_level_val)
			self._logger.info(f"Root logger level updated to: {new_loglevel_str.upper()}")

		if current_app_logger_level != new_level_val:
			self._logger.setLevel(new_level_val)
			self._logger.info(f"Application logger ({self._logger.name}) level updated to: {new_loglevel_str.upper()}")
		
		self._config['loglevel'] = new_loglevel_str.upper() # Update internal config state

	def get_logger(self) -> logging.Logger:
		"""Get the configured logger instance for the application."""
		if not hasattr(self, '_logger'):
			self._initialize_logging()
		return self._logger
	
	def get_config(self, key: str, default: Any = None) -> Any:
		"""Get a configuration value."""
		value = self._config.get(key, default)
		if value is None and default is None:
			if hasattr(self, '_logger') and self._logger:
				self._logger.warning(f"Configuration key '{key}' not found and no default provided.")
			else: 
				# Fallback if logger is not yet available
				print(f"Warning: Configuration key '{key}' not found during early access.")
		return value
	
	def set_config(self, key: str, value: Any) -> None:
		"""Set a configuration value. Use with caution after initialization."""
		if hasattr(self, '_logger') and self._logger:
			self._logger.info(f"Configuration value set: {key} = {value}")
		self._config[key] = value
		if key == 'loglevel' and isinstance(value, str):
			self._update_loglevel(value)
		
	def get_all_config(self) -> Dict[str, Any]:
		"""Get a copy of all configuration values."""
		return self._config.copy()
