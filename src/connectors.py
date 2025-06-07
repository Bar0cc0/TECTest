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

		# Callback for cycle updates
		self._cycle_callback: Optional[Callable[[int, Optional[datetime]], None]] = None
		
		# Directory to save downloaded files
		self._output_dir = config.get_config('output_dir')
		if isinstance(self._output_dir, str):
			self._output_dir = Path(self._output_dir)
		if not self._output_dir.exists():
			self._output_dir.mkdir(parents=True, exist_ok=True)

		# Get cache configuration
		self._cache_enabled = config.get_config('enable_caching', True)
		self._cache_ttl = config.get_config('cache_ttl', 86400)  # Default 1 day in seconds
		self._cache_dir = config.get_config('cache_dir')
		if isinstance(self._cache_dir, str):
			self._cache_dir = Path(self._cache_dir)
			
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
		date_str = date.strftime("%Y%m%d")
		
		# If we have the actual cycle name from the response, use it
		# Otherwise fall back to the numeric cycle
		if cycle_name:
			# Normalize the cycle name for consistent keys (lowercase, no spaces)
			norm_cycle_name = cycle_name.lower().replace(' ', '_')
			cycle_str = f"_{norm_cycle_name}"
		else:
			# Use numeric cycle as temporary identifier
			cycle_str = f"_cycle{cycle}" if cycle is not None else ""
		
		# Only include non-empty parameters
		param_items = [(k, v) for k, v in sorted(params.items()) if v]
		param_str = "_".join(f"{k}={v}" for k, v in param_items) if param_items else ""
		
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
					if cycle_part.startswith("cycle"):
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
			endpoint: API endpoint
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
		Build the complete URL for the API request.
		
		Args:
			endpoint: API endpoint
			date: Date for gasDay parameter
			cycle: Optional cycle number
			**params: Additional parameters
			
		Returns:
			Complete URL string
		"""
		# Start with base URL and routing path
		url = f"{self._base_url}{self._routing_path}{endpoint}"
		
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
			return "unnamed_file.csv"
			
		# Handle path separators and other problematic characters
		import re
		# Remove any directory path components
		clean_name = Path(filename).name
		
		# Replace any characters that are invalid in filenames
		# but preserve spaces - just replace truly problematic chars
		invalid_chars = r'[<>:"/\\|?*]'
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
			endpoint: API endpoint
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
		
		# Check persistent cache if enabled, using metadata to find files by actual cycle name
		if self._cache_enabled:
			date_str = date.strftime("%Y%m%d")
			endpoint_dir = endpoint.replace('/', '_')
			output_dir = self._output_dir / endpoint_dir
			metadata_dir = self._output_dir / "metadata"
			matching_files = []
			
			if metadata_dir.exists() and output_dir.exists():
				# Try to find matching files based on metadata
				for meta_file in metadata_dir.glob("*.meta.json"):
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
			
			# Make the HTTP request to extract the original filename
			async with session.get(url, timeout=self._timeout) as response:
				if response.status == 200:
					# Check content length first - skip tiny files (likely empty)
					content_length = response.headers.get('Content-Length')
					if content_length and int(content_length) < 200:
						self._logger.debug(f"Skipping likely empty file ({content_length} bytes)")
						return False, []
					
					# Extract the original filename from Content-Disposition header
					original_filename = None
					content_disposition = response.headers.get('Content-Disposition')
					if content_disposition:
						import re
						match = re.search(r'filename="?([^"]+)"?', content_disposition)
						if match:
							original_filename = match.group(1)
							self._logger.debug(f"Original filename from response: {original_filename}")
					
					# If no original filename found, generate one based on endpoint and date
					if not original_filename:
						date_str = date.strftime("%Y%m%d")
						original_filename = f"{endpoint.replace('/', '_')}_{date_str}.{self._format}"
						self._logger.warning(f"No original filename found, using generated name: {original_filename}")
					
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
					metadata_dir = self._output_dir / "metadata"
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
					
					# Update cache with current timestamp - using the actual cycle name
					if self._cache_enabled and cycle_name:
						# Create a new cache key with the actual cycle name from the response
						actual_cache_key = self._get_cache_key(endpoint, date, None, cycle_name, **params)
						self._download_cache[actual_cache_key] = time.time()
						self._save_cache()
					
					self._logger.info(f"Downloaded {len(content)} bytes to {output_path} (cycle: {cycle_name})")
				else:
					self._logger.error(f"Error fetching data: {response.status} {response.reason}")
			
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

	async def fetch_data_with_retry(self, endpoint: str, cycle: Optional[int] = None, **params) -> Tuple[bool, List[Path]]:
		"""
		Fetch data with retry logic.
		
		Args:
			endpoint: API endpoint
			cycle: Optional cycle number
			**params: Additional parameters
			
		Returns:
			Tuple of (success_flag, list_of_downloaded_files)
		"""
		attempt = 0
		while attempt < self._retry_attempts:
			try:
				success, files = await self.fetch_data(endpoint, cycle, **params)
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



class CycleIdentifier:
	"""Extract cycle information from original HTTP response filenames."""
	
	# Constants for cycle types we care about
	INTRADAY_1 = "Intraday 1"
	INTRADAY_2 = "Intraday 2" 
	INTRADAY_3 = "Intraday 3"
	TIMELY = "Timely"
	EVENING = "Evening"
	FINAL = "Final"
	
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
		
		# Look for common patterns in the filename
		if "intraday" in filename or "intra" in filename:
			# Try to extract the intraday number
			import re
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
		if "timely" in filename:
			return CycleIdentifier.TIMELY
		elif "evening" in filename:
			return CycleIdentifier.EVENING
		elif "final" in filename:
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
