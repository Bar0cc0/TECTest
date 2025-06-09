#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# connectors.py
# This module provides connectivity to external data sources.

import asyncio
import aiohttp
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
import urllib.parse
import threading
import queue
import pandas as pd
import re
import psycopg2

from interfaces import IConfigProvider,	IConnector


class WebConnector(IConnector):
	"""Connector for web-based data sources."""
	
	def __init__(self, config: IConfigProvider) -> None:
		"""
		Initialize the WebConnector with configuration.
		
		Args:
			config: Configuration provider containing API settings
		"""
		
		self._config = config
		self._logger = config.get_logger()
		
		# Get API configuration
		self._base_url = config.get_config('url')
		self._routing_path = config.get_config('routing_path')
		self._format = config.get_config('format')
		self._asset_id = config.get_config('asset_id')
		self._history = config.get_config('history')
		self._search_type = config.get_config('search_type')
		self._timeout = config.get_config('timeout')
		self._retry_attempts = config.get_config('retry_attempts')
		self._http_session_pool_size = config.get_config('http_session_pool_size')
		self._max_concurrent_requests = config.get_config('max_concurrent_requests')

		self._logger.debug(f"""WebConnector parameters: 
				base URL: {self._base_url}, 
				routing path: {self._routing_path}, 
				format: {self._format},
				asset ID: {self._asset_id}, history: {self._history},
				search type: {self._search_type}, 
				timeout: {self._timeout},
				retry attempts: {self._retry_attempts},
				HTTP session pool size: {self._http_session_pool_size},
				Max concurrent requests: {self._max_concurrent_requests}""")

		# Callback for cycle updates
		self._cycle_callback: Optional[Callable[[int, Optional[datetime]], None]] = None
		
		# Directory to save downloaded files
		self._output_dir = config.get_config('output_dir')
		if isinstance(self._output_dir, str):
			self._output_dir = Path(self._output_dir)
		if not self._output_dir.exists():
			self._output_dir.mkdir(parents=True, exist_ok=True)

		# Get cache configuration
		self._cache_enabled = config.get_config('enable_caching')
		self._cache_ttl = config.get_config('cache_ttl')
		self._cache_dir = config.get_config('cache_dir')
		if isinstance(self._cache_dir, str):
			self._cache_dir = Path(self._cache_dir)
		
		self._logger.debug(f"Cache enabled: {self._cache_enabled}")
			
		# Create cache directory if it doesn't exist
		if not self._cache_dir.exists():
			self._cache_dir.mkdir(parents=True, exist_ok=True)
		
		# Cache file location
		self._cache_file = self._cache_dir / 'download_cache.json'
		
		# Initialize the cache
		self._download_cache = self._load_cache()
		
		# Clean up expired files based on TTL
		self._cleanup_expired_files()

		# Request cache to prevent duplicate downloads
		self._request_cache = set()

		# Add tracking property for cache status
		self.last_response_from_cache = False

		self._logger.info('WebConnector initialized')

	def _load_cache(self) -> Dict[str, float]:
		"""
		Load the download cache from file.
		
		Returns:
			Dictionary with cache keys and timestamps
		"""
		if self._cache_file.exists():
			try:
				with open(self._cache_file, 'r') as f:
					return json.load(f)
			except (json.JSONDecodeError, IOError) as e:
				self._logger.error(f"Error loading cache file: {e}")
				return {}
		return {}
	
	def _save_cache(self) -> None:
		"""Save the download cache to file."""
		try:
			with open(self._cache_file, 'w') as f:
				json.dump(self._download_cache, f)
		except IOError as e:
			self._logger.error(f"Error saving cache file: {e}")
	
	def _get_cache_key(self, endpoint: str, date: datetime, cycle: Optional[int] = None, 
					cycle_name: Optional[str] = None, **params) -> str:
		"""
		Generate a unique cache key for a download request.
		
		Args:
			endpoint: Endpoint
			date: Date for the request
			cycle: Optional cycle number (used only if cycle_name not provided)
			cycle_name: Optional cycle name (takes precedence over cycle number)
			params: Additional parameters
			
		Returns:
			Cache key string
		"""
		# Format date as YYYYMMDD
		date_str = date.strftime("%Y%m%d")
		
		# If we have the actual cycle name from the response, use it
		# Otherwise fall back to the numeric cycle
		if cycle_name:
			# Normalize the cycle name for consistent keys (lowercase, no spaces)
			norm_cycle_name = cycle_name.lower().replace(' ', '_')
			cycle_str = f"_{norm_cycle_name}"
		else:
			# Use numeric cycle as temporary identifier
			cycle_str = f"_cycle{cycle}" if cycle is not None else ''
		
		# Only include non-empty parameters
		param_items = [(k, v) for k, v in sorted(params.items()) if v]
		param_str = '_'.join(f"{k}={v}" for k, v in param_items) if param_items else ''
		
		# Create a unique key with date, cycle and params
		return f"{endpoint}_{date_str}{cycle_str}_{param_str}"
	
	def _cleanup_expired_files(self) -> None:
		"""Remove expired files based on cache TTL."""
		if not self._cache_enabled:
			return
			
		current_time = time.time()
		expired_keys = []
		
		for cache_key, timestamp in self._download_cache.items():
			# Check if file has expired
			if current_time - timestamp > self._cache_ttl:
				# Extract file information from cache key
				parts = cache_key.split('_')
				if len(parts) >= 3:
					endpoint = parts[0].replace('/', '_')
					date_str = parts[1]
					cycle_part = parts[2]
					
					# Determine cycle suffix
					cycle_str = ""
					if cycle_part.startswith('cycle'):
						cycle_str = f"_{cycle_part}"
					
					# Construct the file path
					file_path = self._output_dir / endpoint / f"{date_str}{cycle_str}.{self._format}"
					
					# Delete the file if it exists
					if file_path.exists():
						try:
							file_path.unlink()
							self._logger.debug(f"Deleted expired file: {file_path}")
						except IOError as e:
							self._logger.error(f"Error deleting expired file {file_path}: {e}")
					
				# Add to list of keys to remove from cache
				expired_keys.append(cache_key)
		
		# Remove expired entries from cache
		for key in expired_keys:
			del self._download_cache[key]
		
		# Save updated cache
		if expired_keys:
			self._save_cache()
			self._logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
    
	def is_in_cache(self, endpoint: str, cycle: Optional[int] = None, 
					date: Optional[datetime] = None, **params) -> bool:
		"""
		Check if data for the specified parameters is already in cache.
		
		Args:
			endpoint: API endpoint
			cycle: Optional cycle number
			date: Optional specific date (defaults to today)
			**params: Additional parameters
			
		Returns:
			True if data is in cache and file exists, False otherwise
		"""
		if not self._cache_enabled:
			return False
			
		if date is None:
			date = datetime.now()
			
		# Generate cache key
		cache_key = self._get_cache_key(endpoint, date, cycle, **params)
		
		# Check if in cache
		if cache_key in self._download_cache:
			# Verify the file exists
			date_str = date.strftime("%Y%m%d")
			cycle_str = f"_cycle{cycle}" if cycle is not None else ""
			endpoint_dir = endpoint.replace('/', '_')
			output_dir = self._output_dir / endpoint_dir
			output_path = output_dir / f"{date_str}{cycle_str}.{self._format}"
			
			# Check if file exists and cache hasn't expired
			if output_path.exists():
				current_time = time.time()
				timestamp = self._download_cache[cache_key]
				
				# Check if cache entry is still valid (not expired)
				if current_time - timestamp <= self._cache_ttl:
					return True
					
				# Cache expired, handle expiration
				self._logger.info(f"Cache entry expired for {cache_key}")
				return False
				
			# File doesn't exist, clean up cache entry
			del self._download_cache[cache_key]
			self._save_cache()
			
		return False
		
	def get_cached_file_path(self, endpoint: str, cycle: Optional[int] = None, 
						date: Optional[datetime] = None) -> Optional[Path]:
		"""
		Get the path to a cached file if it exists.
		
		Args:
			endpoint: Endpoint
			cycle: Optional cycle number
			date: Optional specific date
			
		Returns:
			Path to cached file or None if not in cache
		"""
		if date is None:
			date = datetime.now()
			
		date_str = date.strftime("%Y%m%d")
		cycle_str = f"_cycle{cycle}" if cycle is not None else ""
		endpoint_dir = endpoint.replace('/', '_')
		output_dir = self._output_dir / endpoint_dir
		output_path = output_dir / f"{date_str}{cycle_str}.{self._format}"
		
		# Only return if file exists
		if output_path.exists():
			return output_path
			
		return None

	def register_cycle_callback(self, callback: Callable[[int, Optional[datetime]], None]) -> None:
		"""
		Register a callback function to be called when cycle updates are available.
		
		Args:
			callback: Function that takes a cycle number and optional date parameter
		"""
		self._cycle_callback = callback

	def notify_cycle_update(self, cycle: int, date: Optional[datetime] = None) -> None:
		"""
		Notify that a new cycle of data is available.
		
		Args:
			cycle: The cycle number
			date: Optional specific date (defaults to current date)
		"""
		if self._cycle_callback:
			self._cycle_callback(cycle, date)
	
	def _format_gas_day(self, date: datetime) -> str:
		"""
		Format a date for the gasDay parameter (MM%2FDD%2FYYYY).
		
		Args:
			date: The date to format
			
		Returns:
			Formatted date string
		"""
		return date.strftime("%m%%2F%d%%2F%Y")
	
	def _get_date_range(self, days: int) -> List[datetime]:
		"""
		Get a list of dates going back 'days' from today.
		
		Args:
			days: Number of days to go back
			
		Returns:
			List of datetime objects
		"""
		today = datetime.now()
		return [today - timedelta(days=i) for i in range(days)]
	
	def _build_url(self, endpoint: str, date: datetime, cycle: Optional[int] = None, **params) -> str:
		"""
		Build the complete URL for the HTTP request.
		
		Args:
			endpoint: Endpoint
			date: Date for gasDay parameter
			cycle: Optional cycle number
			**params: Additional parameters
			
		Returns:
			Complete URL string
		"""
		# Start with base URL and routing path

		url = f"{self._base_url}{self._routing_path}{self._asset_id}{endpoint}"
		
		# Add required query parameters with defaults
		query_params = {
			'f': self._format,
			'extension': self._format,
			'asset': self._asset_id,
			'gasDay': self._format_gas_day(date),
			'searchType': self._search_type,
			'searchString': '',
			'locType': 'ALL',
			'locZone': 'ALL'
		}
		
		# Add cycle if provided
		if cycle is not None:
			query_params['cycle'] = str(cycle)
			
		# Process any searchType parameter specially
		if 'searchType' in params:
			search_type = params['searchType']
			# If it's a list with a single item (from the [NOM] format), use the first item
			if isinstance(search_type, list) and len(search_type) > 0:
				query_params['searchType'] = search_type[0]
			else:
				query_params['searchType'] = search_type
		
		# Update with any additional params, overriding defaults
		for key, value in params.items():
			if key != 'searchType':  # Already handled
				if isinstance(value, list) and len(value) > 0:
					# If it's a list from parsed parameters, use the first item
					query_params[key] = value[0]
				else:
					query_params[key] = value
		
		# Encode and append query string
		query_string = urllib.parse.urlencode(query_params, safe='%')
		return f"{url}?{query_string}"
	
	async def _download_file(self, session: aiohttp.ClientSession, url: str, output_path: Path) -> bool:
		"""
		Download a file from a URL and save it to the specified path.
		
		Args:
			session: aiohttp ClientSession
			url: URL to download from
			output_path: Path to save the file
			
		Returns:
			True if successful, False otherwise
		"""
		try:
			async with session.get(url, timeout=self._timeout) as response:
				if response.status == 200:
					content = await response.read()
					# Create parent directories if they don't exist
					output_path.parent.mkdir(parents=True, exist_ok=True)
					# Write content to file
					with open(output_path, 'wb') as f:
						f.write(content)
					self._logger.debug(f"Successfully downloaded {url} to {output_path}")
					return True
				else:
					self._logger.error(f"Failed to download {url}: HTTP {response.status}")
					return False
		except Exception as e:
			self._logger.error(f"Error downloading {url}: {str(e)}")
			return False

	def _sanitize_filename(self, filename: str) -> str:
		"""
		Sanitize a filename to ensure it's valid for the filesystem.
		Preserves spaces but replaces invalid characters.
		
		Args:
			filename: Original filename
			
		Returns:
			Sanitized filename
		"""
		if not filename:
			return 'unnamed_file.csv'
			
		# Handle both Unix and Windows paths
		if '\\' in filename:
			# Handle Windows-style paths even on Unix
			clean_name = filename.split('\\')[-1]
		else:
			# Regular path handling
			clean_name = Path(filename).name
		
		# Handle remaining problematic characters
		invalid_chars = r'[<>:"/|?*]'
		clean_name = re.sub(invalid_chars, '_', clean_name)
		
		# Ensure we have a non-empty filename
		if not clean_name or clean_name.startswith('.'):
			clean_name = f"unnamed_{int(time.time())}.csv"
			
		return clean_name

	async def fetch_data(self, endpoint: str, cycle: Optional[int] = None, 
						date: Optional[datetime] = None, **params) -> Tuple[bool, List[Path]]:
		"""
		Fetch data for the specified endpoint, cycle, and date with caching.
		
		Args:
			endpoint: Endpoint
			cycle: Optional cycle number
			date: Optional specific date (defaults to today)
			**params: Additional parameters
			
		Returns:
			Tuple of (success_flag, list_of_downloaded_files)
		"""
		if date is None:
			date = datetime.now()
		
		downloaded_files = []
		
		# Generate initial cache key using numeric cycle
		initial_cache_key = self._get_cache_key(endpoint, date, cycle, **params)
		
		# Check in-memory cache to avoid duplicates in same session
		if initial_cache_key in self._request_cache:
			self._logger.debug(f"Skipping already processed request in this session: {initial_cache_key}")
			return True, []
		
		#FIXME: Enforce separation of concerns: implement _get_cached_filenames()
		# Check persistent cache if enabled, using metadata to find files by actual cycle name
		if self._cache_enabled:
			date_str = date.strftime("%Y%m%d")
			endpoint_dir = endpoint.replace('/', '_')
			output_dir = self._output_dir / endpoint_dir
			metadata_dir = self._output_dir / 'metadata'
			matching_files = []
			
			if metadata_dir.exists() and output_dir.exists():
				# Try to find matching files based on metadata
				for meta_file in metadata_dir.glob('*.meta.json'):
					try:
						with open(meta_file, 'r') as f:
							metadata = json.load(f)
							# Check if this metadata matches our request parameters
							if (metadata.get('endpoint') == endpoint and 
								metadata.get('date') == date_str):
								
								# Only include files with intraday cycle name
								cycle_name = metadata.get('cycle_name')
								if not cycle_name or not cycle_name.lower().startswith('intraday'):
									continue
								
								# If specific cycle requested, verify it matches
								# We check either the numeric cycle OR the cycle name if available
								if cycle is not None:
									if metadata.get('requested_cycle') != cycle:
										continue
									
								# Create cache key with the actual cycle name
								actual_cache_key = self._get_cache_key(endpoint, date, None, cycle_name, **params)
								
								# Check if this cache entry is still valid
								if actual_cache_key in self._download_cache:
									current_time = time.time()
									timestamp = self._download_cache[actual_cache_key]
									if current_time - timestamp > self._cache_ttl:
										# Expired cache entry
										continue
										
									# Get the saved filename from metadata
									saved_filename = metadata.get('saved_filename')
									if saved_filename:
										file_path = output_dir / saved_filename
										if file_path.exists():
											# Skip tiny files (likely empty)
											if file_path.stat().st_size < 200:
												self._logger.debug(f"Skipping cached file that's too small: {file_path}")
												continue
											
											matching_files.append(file_path)
											self._logger.info(f"Using cached file: {file_path} (cycle: {cycle_name})")
											
											# Add to in-memory cache
											self._request_cache.add(initial_cache_key)
					except (json.JSONDecodeError, IOError) as e:
						self._logger.warning(f"Error reading metadata file {meta_file}: {e}")
						continue
			
			if matching_files:
				# Set flag that this response came from cache
				self.last_response_from_cache = True
				return True, matching_files
		
		# Add to in-memory cache before downloading
		self._request_cache.add(initial_cache_key)
		
		# Create async HTTP session
		async with aiohttp.ClientSession() as session:
			# Build URL with cycle parameter
			url = self._build_url(endpoint, date, cycle, **params)
			self._logger.debug(f"Fetching data from URL: {url}")
			
			try:
				# Make the HTTP request to extract the original filename
				async with session.get(url, timeout=self._timeout) as response:
					if response.status == 200:
						# Check content length first - skip tiny files (likely empty)
						content_length = response.headers.get('Content-Length')
						if content_length and int(content_length) < 200:
							self._logger.debug(f"Skipping likely empty file ({content_length} bytes)")
							return False, []
						
						#FIXME: Handle Content-Type to determine if we got a valid file
						# Extract the original filename from Content-Disposition header
						original_filename = None
						content_disposition = response.headers.get('Content-Disposition')
						if content_disposition:
							match = re.search(r'filename="?([^"]+)"?', content_disposition)
							if match:
								original_filename = match.group(1)
								self._logger.debug(f"Original filename from response: {original_filename}")
						
						# If no original filename found, generate one based on endpoint and date
						if not original_filename:
							date_str = date.strftime("%Y%m%d")
							original_filename = f"{endpoint.replace('/', '_')}_{date_str}.{self._format}"
							self._logger.warning(f"No original filename found: {content_disposition}")
						
						# Determine the actual cycle based on the original filename
						cycle_name = None
						if original_filename:
							cycle_name = CycleIdentifier.get_cycle_from_filename(original_filename)
							if cycle_name:
								self._logger.debug(f"Identified cycle: {cycle_name} from filename: {original_filename}")
								if cycle is not None:
									self._logger.debug(f"Requested cycle {cycle} but file indicates {cycle_name}")
						
						# Only allow files with Intraday cycle names
						if not cycle_name or not cycle_name.lower().startswith('intraday'):
							self._logger.debug(f"Skipping non-intraday file: {original_filename}")
							return False, []
						
						# Read response content to check file size and content
						content = await response.read()
						
						# Skip if file is too small (likely empty or just headers)
						if len(content) < 200:
							self._logger.debug(f"Skipping file with content size {len(content)} bytes (likely empty)")
							return False, []
						
						# Create directory structure
						endpoint_dir = endpoint.replace('/', '_')
						output_dir = self._output_dir / endpoint_dir
						output_dir.mkdir(parents=True, exist_ok=True)
						
						# Use original filename for output, but sanitize it for filesystem safety
						safe_original_filename = self._sanitize_filename(original_filename)
						output_path = output_dir / safe_original_filename
						
						# Save the response content
						with open(output_path, 'wb') as f:
							f.write(content)
						
						# Create metadata directory
						metadata_dir = self._output_dir / 'metadata'
						metadata_dir.mkdir(parents=True, exist_ok=True)
						
						# Store metadata about this file
						download_timestamp = datetime.now()
						metadata = {
							'endpoint': endpoint,
							'requested_cycle': cycle,
							'cycle_name': cycle_name,
							'date': date.strftime("%Y%m%d"),
							'original_filename': original_filename,
							'saved_filename': safe_original_filename,
							'download_time': download_timestamp.isoformat(),
							'download_timestamp': time.time()
						}
						
						metadata_filename = f"{output_path.stem}.meta.json"
						metadata_path = metadata_dir / metadata_filename
						
						# Write metadata to file
						with open(metadata_path, 'w') as f:
							json.dump(metadata, f, indent=2)
						
						downloaded_files.append(output_path)
						
						# Update cache with current timestamp using the actual cycle name
						if self._cache_enabled and cycle_name:
							# Create a new cache key with the actual cycle name from the response
							actual_cache_key = self._get_cache_key(endpoint, date, None, cycle_name, **params)
							self._download_cache[actual_cache_key] = time.time()
							self._save_cache()
						
						self._logger.info(f"Downloaded {len(content)} bytes to {output_path} (cycle: {cycle_name})")
					else:
						self._logger.error(f"Error fetching data: {response.status} {response.reason}")
						return False, [] # Return False if status is not 200

			except aiohttp.ClientError as e:
				self._logger.error(f"AIOHTTP ClientError in fetch_data for {url}: {e}")
				return False, []
			except Exception as e: # Catch other potential errors during the download/processing phase
				self._logger.error(f"Unexpected error during fetch_data's download section for {url}: {e}")
				return False, []

		return len(downloaded_files) > 0, downloaded_files

	def _save_metadata(self, file_path: Path, metadata: Dict[str, Any]) -> None:
		"""Save metadata for a downloaded file."""
		try:
			# Create metadata file path
			metadata_path = file_path.with_suffix('.meta.json')
			
			# Write metadata to file
			with open(metadata_path, 'w') as f:
				json.dump(metadata, f, indent=2)
		except Exception as e:
			self._logger.warning(f"Error saving metadata: {e}")

	async def fetch_data_with_retry(self, endpoint: str, cycle: Optional[int] = None, 
								 date: Optional[datetime] = None, **params) -> Tuple[bool, List[Path]]:
		"""
		Fetch data with retry logic.
		
		Args:
			endpoint: Endpoint
			cycle: Optional cycle number
			date: Optional date
			**params: Additional parameters

		Returns:
			Tuple of (success_flag, list_of_downloaded_files)
		"""
		attempt = 0
		while attempt < self._retry_attempts:
			try:
				success, files = await self.fetch_data(endpoint, cycle, date=date, **params)
				if success:
					return True, files
				
				# If not successful, wait and retry
				attempt += 1
				if attempt < self._retry_attempts:
					wait_time = 2 ** attempt  # Exponential backoff
					self._logger.debug(f"Retry {attempt} for {endpoint} cycle {cycle} in {wait_time} seconds")
					await asyncio.sleep(wait_time)
			except Exception as e:
				self._logger.error(f"Error during attempt {attempt} for {endpoint} cycle {cycle}: {str(e)}")
				attempt += 1
				if attempt < self._retry_attempts:
					await asyncio.sleep(2 ** attempt)

		self._logger.error(f"Failed to fetch data for {endpoint} cycle {cycle} after {self._retry_attempts} attempts")
		return False, []

	def post_data(self, endpoint: str, data_files: List[Path], 
				cycle: Optional[int] = None, date: Optional[datetime] = None, 
				**params) -> bool:
		"""
		Not supported for WebConnector.
		
		Args:
			endpoint: The data endpoint/table/path
			data_files: List of files to post
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Always raises NotImplementedError
		"""
		raise NotImplementedError('WebConnector does not support post operations')

	def post_data_with_retry(self, endpoint: str, data_files: List[Path],
						cycle: Optional[int] = None, date: Optional[datetime] = None,
						**params) -> bool:
		"""
		Not supported for WebConnector.
		
		Args:
			endpoint: The data endpoint/table/path
			data_files: List of files to post
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Always raises NotImplementedError
		"""
		raise NotImplementedError("WebConnector does not support post operations with retry")

class CycleIdentifier:
	"""Extract cycle information from original HTTP response filenames."""
	
	# Constants for cycle types we care about
	INTRADAY_1 = 'Intraday 1'
	INTRADAY_2 = 'Intraday 2'
	INTRADAY_3 = 'Intraday 3'
	TIMELY = 'Timely'
	EVENING = 'Evening'
	FINAL = 'Final'

	@staticmethod
	def get_cycle_from_filename(filename: str) -> Optional[str]:
		"""
		Extract cycle descriptive name directly from the filename.
		
		Args:
			filename: Original filename from HTTP response
			
		Returns:
			Standardized descriptive cycle name or None if couldn't be determined
		"""
		# Remove path information if present
		filename = Path(filename).name.lower()

		#FIXME: Lookup patterns/tags hardcoding
		# Look for common patterns in the filename
		if 'intraday' in filename or 'intra' in filename:
			# Try to extract the intraday number
			match = re.search(r'intraday\s*(\d)', filename, re.IGNORECASE)
			if match:
				intraday_num = int(match.group(1))
				if intraday_num == 1:
					return CycleIdentifier.INTRADAY_1
				elif intraday_num == 2:
					return CycleIdentifier.INTRADAY_2
				elif intraday_num == 3:
					return CycleIdentifier.INTRADAY_3
		
		# Check for other cycle types
		if 'timely' in filename:
			return CycleIdentifier.TIMELY
		elif 'evening' in filename:
			return CycleIdentifier.EVENING
		elif 'final' in filename:
			return CycleIdentifier.FINAL
				
		return None
	
	@staticmethod
	def is_intraday_cycle(cycle_name: str) -> bool:
		"""Check if the cycle name is an intraday cycle."""
		return cycle_name in [
			CycleIdentifier.INTRADAY_1, 
			CycleIdentifier.INTRADAY_2, 
			CycleIdentifier.INTRADAY_3
		]


class DatabaseConnector(IConnector):
	"""
	Connector for connecting to the database and processing files through a queue.
	"""
	_instance = None
	_lock = threading.Lock()
	
	def __new__(cls, config: IConfigProvider):
		"""Implement singleton pattern with double-checked locking."""
		if cls._instance is None:
			with cls._lock:
				if cls._instance is None:
					cls._instance = super(DatabaseConnector, cls).__new__(cls)
					cls._instance._initialized = False
		return cls._instance
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the DatabaseConnector with configuration."""
		# Only initialize once
		if hasattr(self, '_initialized') and self._initialized:
			return
			
		self._config = config
		self._logger = config.get_logger()
		
		# Get database configuration
		db_config = config.get_config('Database', {})
		self._db_type = config.get_config('db_type') or db_config.get('db_type', 'postgresql')
		self._db_host = config.get_config('db_host') or db_config.get('db_host', 'localhost')
		self._db_port = config.get_config('db_port') or db_config.get('db_port', 5432)
		self._db_name = config.get_config('db_name') or db_config.get('db_name', 'tec_data')
		self._db_user = config.get_config('db_user') or db_config.get('db_user', 'postgres')
		self._db_password = config.get_config('db_password') or db_config.get('db_password', 'postgres')
		self._retry_attempts = config.get_config('retry_attempts') or config.get_config('API', {}).get('retry_attempts', 3)

		self._logger.debug(f"""DatabaseConnector parameters:
				db_type: {self._db_type}, 
				db_host: {self._db_host}:{self._db_port}, 
				db_name: {self._db_name},
				db_user: {self._db_user},
				db_password: {'***' if self._db_password else 'None'},
				retry_attempts: {self._retry_attempts}""")

		# Initialize connection
		self._connection = None
		self._connect()
		
		# Setup processing queue
		self._queue = queue.Queue()
		self._processing = False
		self._worker_thread = None
		self._running = False
		
		# Callback for cycle updates
		self._cycle_callback = None
		
		# Start the worker thread
		self._start_worker()
		
		self._initialized = True
		self._logger.info("DatabaseConnector initialized")
	
	def _connect(self) -> None:
		"""Establish connection to the database. For now, only PostgreSQL is supported."""
		#TODO: Implement factory pattern to add support for other databases
		try:
			if self._db_type == 'postgresql':
				self._connection = psycopg2.connect(
					host=self._db_host,
					port=self._db_port,
					dbname=self._db_name,
					user=self._db_user,
					password=self._db_password
				)
				# Don't enable autocommit to enforce explicit transaction control
				self._connection.autocommit = False
				self._logger.info(f'Successfully connected to database {self._db_name} at {self._db_host}:{self._db_port}')
			else:
				self._logger.error(f"Unsupported database type: {self._db_type}")
		except Exception as e:
			self._logger.error(f"Error connecting to database: {str(e)}")
			self._connection = None
	
	def _start_worker(self) -> None:
		"""Start the worker thread that processes the queue."""
		if self._worker_thread is not None and self._worker_thread.is_alive():
			return  # Worker is already running
			
		self._running = True
		self._worker_thread = threading.Thread(target=self._process_queue, daemon=True)
		self._worker_thread.start()
		self._logger.debug(f"Database worker thread started: {self._worker_thread.name}")

	def _stop_worker(self) -> None:
		"""Stop the worker thread."""
		self._running = False
		if self._worker_thread and self._worker_thread.is_alive():
			self._worker_thread.join(timeout=5.0)
			self._logger.debug(f"Database worker thread stopped: {self._worker_thread.name}")

	def _process_queue(self) -> None:
		"""Worker thread function to process items in the queue."""
		while self._running:
			try:
				# Get an item from the queue with timeout to allow checking _running flag
				try:
					item = self._queue.get(timeout=1.0)
				except queue.Empty:
					continue
					
				# Process the item
				self._processing = True
				try:
					self._logger.debug(f"Processing queue item: {item['endpoint']}, {len(item['files'])} files")
					success = self._post_data_internal(
						item['endpoint'],
						item['files'],
						item['cycle'],
						item['date'],
						**item.get('params', {})
					)
					if success:
						self._logger.info(f"Successfully processed {len(item['files'])} files for {item['endpoint']}")
					else:
						self._logger.error(f"Failed to process files for {item['endpoint']}")
				except Exception as e:
					self._logger.error(f"Error processing queue item: {str(e)}")
				finally:
					self._processing = False
					self._queue.task_done()
			except Exception as e:
				self._logger.error(f"Error in database worker thread: {str(e)}")

	def register_cycle_callback(self, callback: Callable[[int, Optional[datetime]], None]) -> None:
		"""Register a callback for cycle updates.
		Args:
			callback: Function that takes a cycle number and optional date parameter
		"""
		self._cycle_callback = callback

	def notify_cycle_update(self, cycle: int, date: Optional[datetime] = None) -> None:
		"""Notify that a new cycle is available.
		Args:
			cycle: The cycle number
			date: Optional date of the cycle
		"""
		if self._cycle_callback:
			self._cycle_callback(cycle, date)

	async def fetch_data(self, endpoint: str, cycle: Optional[int] = None, 
					date: Optional[datetime] = None, **params) -> Tuple[bool, List[Path]]:
		"""Not supported for DatabaseConnector."""
		raise NotImplementedError('fetch_data not implemented for DatabaseConnector')

	async def fetch_data_with_retry(self, endpoint: str, cycle: Optional[int] = None, 
							date: Optional[datetime] = None, **params) -> Tuple[bool, List[Path]]:
		"""Not supported for DatabaseConnector."""
		raise NotImplementedError('fetch_data_with_retry not implemented for DatabaseConnector')
	
	def post_data(self, endpoint: str, data_files: List[Path], 
				cycle: Optional[int] = None, date: Optional[datetime] = None, 
				**params) -> bool:
		"""
		Queue data files for posting to the database.
		
		Args:
			endpoint: The table name to post to
			data_files: List of CSV files to post
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			True if files were queued successfully
		"""
		if not data_files:
			self._logger.warning('No files provided to post_data')
			return False
		
		# Check if there are any valid files to queue
		valid_files = []
		for file_path in data_files:
			if not file_path.exists():
				self._logger.warning(f"File does not exist: {file_path}")
				continue
				
			if file_path.stat().st_size < 200:
				self._logger.warning(f"File too small, likely empty: {file_path}")
				continue
				
			valid_files.append(file_path)
		
		if not valid_files:
			self._logger.warning('No valid files to queue')
			return False
			
		# Add to the processing queue
		self._logger.info(f"Queuing {len(valid_files)} files for {endpoint}")
		self._queue.put({
			'endpoint': endpoint,
			'files': valid_files,
			'cycle': cycle,
			'date': date,
			'params': params
		})
		
		# Ensure worker is running
		if not self._worker_thread or not self._worker_thread.is_alive():
			self._start_worker()
			
		return True # Files were queued successfully

	def post_data_with_retry(self, endpoint: str, data_files: List[Path],
						cycle: Optional[int] = None, date: Optional[datetime] = None,
						**params) -> bool:
		"""
		Post data with retry logic.
		
		Args:
			endpoint: The database table
			data_files: List of files to post
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Success flag
		"""
		attempt = 0
		while attempt < self._retry_attempts:
			try:
				# Call the internal post method
				success = self._post_data_internal(endpoint, data_files, cycle, date, **params)
				if success:
					return True
					
				# If not successful, retry
				attempt += 1
				if attempt < self._retry_attempts:
					wait_time = 2 ** attempt  # Exponential backoff
					self._logger.debug(f"Retry {attempt} for posting to {endpoint} in {wait_time} seconds")
					time.sleep(wait_time)
			except Exception as e:
				self._logger.error(f"Error during post attempt {attempt} to {endpoint}: {str(e)}")
				attempt += 1
				if attempt < self._retry_attempts:
					time.sleep(2 ** attempt)
		
		# If we get here, all attempts failed
		self._logger.error(f"Failed to post data to {endpoint} after {self._retry_attempts} attempts")
		return False

	def _post_data_internal(self, endpoint: str, data_files: List[Path],
						cycle: Optional[int] = None, date: Optional[datetime] = None,
						**params) -> bool:
		"""
		Internal method to post data to the database.
		
		Args:
			endpoint: The table name
			data_files: List of CSV files to load
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Success flag
		"""
		if not self._connection:
			self._logger.error('No database connection available')
			self._connect()  # Try to reconnect
			if not self._connection:
				return False
		
		attempt = 0
		while attempt < self._retry_attempts:
			try:
				# Create a cursor
				cursor = self._connection.cursor()
				
				# Process each file
				total_rows = 0
				for file_path in data_files:
					self._logger.debug(f"Processing file: {file_path.name}")
					
					# Read CSV file
					try:
						df = pd.read_csv(file_path)
					except Exception as e:
						self._logger.error(f"Error reading file {file_path.name}: {str(e)}")
						continue
						
					if df.empty:
						self._logger.warning(f"File {file_path.name} is empty, skipping")
						continue
					
					# Ensure the table exists
					try:
						self._ensure_table_exists(cursor, endpoint, df)
					except Exception as e:
						self._logger.error(f"Error ensuring table exists: {str(e)}")
						self._connection.rollback()
						raise e
					
					# Insert the data
					try:
						rows = self._insert_dataframe(cursor, endpoint, df)
						total_rows += rows
					except Exception as e:
						self._logger.error(f"Error inserting data: {str(e)}")
						self._connection.rollback()
						raise e
				
				# Commit the transaction
				self._connection.commit()
				self._logger.info(f"Successfully inserted {total_rows} rows into {endpoint}")
				return True
				
			except Exception as e:
				attempt += 1
				self._logger.error(f"Error in _post_data_internal function (attempt {attempt}/{self._retry_attempts}): {str(e)}")
				
				# Try to reconnect if connection issue
				if 'connection' in str(e).lower() or 'broken' in str(e).lower():
					self._connect()
				
				if attempt < self._retry_attempts:
					#TODO: Add jitter to backoff strategy
					wait_time = 2 ** attempt  # Exponential backoff
					self._logger.debug(f"Retrying in {wait_time} seconds...")
					time.sleep(wait_time)
				else:
					return False	
		return True # Return success after all operations complete

	def _ensure_table_exists(self, cursor, table_name: str, df: pd.DataFrame) -> bool:
		"""
		Ensure the specified table exists with the correct schema.
		If it doesn't exist, create it based on the DataFrame schema.
		
		Args:
			cursor: Database cursor
			table_name: Name of the table
			df: DataFrame containing the data to be inserted
		"""
		# Check if connection is valid
		if not self._connection:
			self._logger.error(f"Cannot ensure table {table_name} exists: No database connection")
			return False
			
		try:
			# Check if table exists
			#FIXME: Schema hardcoding
			cursor.execute("""
				SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE table_schema = 'staging' 
					AND table_name = %s
				);
			""", (table_name,))
			
			table_exists = cursor.fetchone()[0]
			
			if not table_exists:
				# Get schema file path
				schema_file_path = Path(__file__).parent / 'staging_schema.sql'
				
				self._logger.info(f"Table {table_name} does not exist. Executing schema file: {schema_file_path}")
				
				if schema_file_path.exists():
					try:
						# Read the entire schema file
						with open(schema_file_path, 'r') as f:
							schema_sql = f.read()
						
						# Execute the entire schema file to create all tables and indexes
						cursor.execute(schema_sql)
						
						# Add null check before commit
						if self._connection:
							self._connection.commit()
						else:
							self._logger.error('Cannot commit schema changes: Connection lost')
							return False
							
						# Verify the table was created
						#FIXME: Schema hardcoding
						cursor.execute("""
							SELECT EXISTS (
								SELECT FROM information_schema.tables 
								WHERE table_schema = 'staging' 
								AND table_name = %s
							);
						""", (table_name,))
						
						if not cursor.fetchone()[0]:
							self._logger.error(f"Schema file did not create the required table: {table_name}")
							return False
							
					except Exception as e:
						self._logger.error(f"Error executing schema file: {str(e)}")
						if self._connection:
							self._connection.rollback()
						return False
				else:
					self._logger.error(f"Schema file not found: {schema_file_path}")
					return False
					
			return True
		except Exception as e:
			self._logger.error(f"Error checking table existence: {str(e)}")
			if self._connection:
				self._connection.rollback()
			return False

	def _insert_dataframe(self, cursor, table_name: str, df: pd.DataFrame) -> int:
		"""
		Insert DataFrame into database table.
		
		Args:
			cursor: Database cursor
			table_name: Name of the table
			df: DataFrame containing the data to be inserted
			
		Returns:
			Number of rows inserted
		"""
		try:
			# Create a copy to avoid modifying the original
			df_copy = df.copy()
			
			# Log column types for debugging
			self._logger.debug(f"DataFrame column dtypes before conversion: {df_copy.dtypes}")
			
			# Convert all NumPy integer types to Python int
			for col in df_copy.select_dtypes(include=['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64']).columns:
				self._logger.debug(f"Converting column {col} from {df_copy[col].dtype} to Python int")
				df_copy[col] = df_copy[col].astype(object).where(pd.notnull(df_copy[col]), None)
				# Apply conversion only to non-null values
				mask = df_copy[col].notnull()
				df_copy.loc[mask, col] = df_copy.loc[mask, col].apply(lambda x: int(x) if pd.notnull(x) else None)
			
			# Convert all NumPy float types to Python float
			for col in df_copy.select_dtypes(include=['float16', 'float32', 'float64']).columns:
				self._logger.debug(f"Converting column {col} from {df_copy[col].dtype} to Python float")
				df_copy[col] = df_copy[col].astype(object).where(pd.notnull(df_copy[col]), None)
				# Apply conversion only to non-null values
				mask = df_copy[col].notnull()
				df_copy.loc[mask, col] = df_copy.loc[mask, col].apply(lambda x: float(x) if pd.notnull(x) else None)
			
			# Convert boolean types to Python bool
			for col in df_copy.select_dtypes(include=['bool']).columns:
				self._logger.debug(f"Converting column {col} from bool to Python bool")
				df_copy[col] = df_copy[col].astype(object).where(pd.notnull(df_copy[col]), None)
				# Apply conversion only to non-null values
				mask = df_copy[col].notnull()
				df_copy.loc[mask, col] = df_copy.loc[mask, col].apply(lambda x: bool(x) if pd.notnull(x) else None)
			
			# Handle datetime types
			for col in df_copy.select_dtypes(include=['datetime64']).columns:
				self._logger.debug(f"Converting column {col} from datetime64 to Python datetime")
				df_copy[col] = df_copy[col].astype(object).where(pd.notnull(df_copy[col]), None)
			
			# Replace any remaining NaN values with None for SQL compatibility
			df_copy = df_copy.replace({pd.NA: None, pd.NaT: None})
			df_copy = df_copy.where(pd.notnull(df_copy), None)
			
			# Get column names with proper quoting for SQL, skipping the ID column
			columns = [col for col in df_copy.columns if col.lower() != 'id']
			column_str = ", ".join(f'"{col}"' for col in columns)
			
			# Create parameterized query, excluding the id column which is SERIAL
			placeholders = ", ".join(["%s"] * len(columns))
			insert_query = f'INSERT INTO staging.{table_name} ({column_str}) VALUES ({placeholders})' #FIXME: Schema hardcoding
			
			# Log the query for debugging
			self._logger.debug(f"Insert query: {insert_query}")
			
			# Convert DataFrame to list of tuples for insertion
			# Extract only the columns we're inserting (excluding id)
			df_for_insert = df_copy[columns]
			data = [tuple(x) for x in df_for_insert.to_numpy()]
			
			# Execute in batches to avoid memory issues with large datasets
			batch_size = self._config.get_config('Optimization', {}).get('batch_size', 1000)
			row_count = 0
			
			for i in range(0, len(data), batch_size):
				batch = data[i:i+batch_size]
				cursor.executemany(insert_query, batch)
				row_count += len(batch)
				self._logger.debug(f"Inserted batch of {len(batch)} rows into staging.{table_name}") #FIXME: Schema hardcoding

			return row_count
			
		except Exception as e:
			self._logger.error(f"Error in _insert_dataframe: {str(e)}")
			# Re-raise to be handled by caller
			raise
	
	def close(self) -> None:
		"""Close the database connection and stop the worker thread."""
		self._stop_worker()
		
		if self._connection:
			try:
				self._connection.close()
				self._logger.info('Database connection closed')
			except Exception as e:
				self._logger.error(f"Error closing database connection: {str(e)}")
			finally:
				self._connection = None

	def __del__(self) -> None:
		"""Ensure resources are cleaned up when object is garbage collected."""
		self.close()
