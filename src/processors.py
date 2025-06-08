#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# processors.py
# This module provides data processing capabilities.

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set, Type, Tuple
from abc import ABC, abstractmethod
from datetime import datetime
import time
import json
import re
import shutil
import os

from interfaces import IConfigProvider, IDataProcessor


# Pre-compile regular expressions
INTRADAY_PATTERN = re.compile(r'intraday\s*(\d+)', re.IGNORECASE)
DATE_PATTERN = re.compile(r'(\d{2})-(\d{2})-(\d{4})')
SANITIZE_PATTERN = re.compile(r'[^\w]', re.UNICODE)
UNAVAILABLE_PATTERN = re.compile(r'unavailable', re.IGNORECASE)

# Common date formats for parsing
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]


class MetadataExtractor:
	"""
	Dedicated class for metadata extraction with caching for better performance.
	"""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the metadata extractor with caching."""
		self.config = config
		self.logger = config.get_logger()
		self.cache = {}  # Cache metadata results
		
	def extract_metadata(self, data_file: Path) -> Dict[str, Any]:
		"""
		Extract metadata with caching for performance.
		
		Args:
			data_file: Path to the file to process
			
		Returns:
			Dictionary with metadata values and fallbacks
		"""
		# Return cached result if available
		if data_file in self.cache:
			return self.cache[data_file]
			
		# Initialize with fallback values
		metadata_result = {
			'cycle_name': 'Unknown_Cycle',
			'download_timestamp': time.time(),
			'measure_date': datetime.now().strftime("%Y-%m-%d")
		}
		
		# Find associated metadata file
		metadata_dir = Path(self.config.get_config('output_dir')) / 'metadata'
		metadata_path = metadata_dir / f"{data_file.stem}.meta.json"
		
		# Try to get values from metadata if available
		if metadata_path.exists():
			try:
				with open(metadata_path, 'r') as f:
					metadata = json.load(f)
				
				# Extract cycle name
				if 'cycle_name' in metadata and metadata['cycle_name']:
					metadata_result['cycle_name'] = metadata['cycle_name']
				else:
					# Try to extract cycle from filename
					filename = data_file.name
					if 'intraday' in filename.lower():
						match = INTRADAY_PATTERN.search(filename.lower())
						if match:
							metadata_result['cycle_name'] = f"Intraday {match.group(1)}"
				
				# Extract timestamp
				if 'download_timestamp' in metadata and metadata['download_timestamp']:
					metadata_result['download_timestamp'] = metadata['download_timestamp']
				else:
					# Use file modification time as fallback
					metadata_result['download_timestamp'] = data_file.stat().st_mtime
				
				# Extract date
				if 'date' in metadata and metadata['date']:
					date_value = metadata['date']
					
					# Parse date string
					if isinstance(date_value, str):
						# Try multiple date formats
						for fmt in DATE_FORMATS:
							try:
								parsed_date = datetime.strptime(date_value, fmt)
								metadata_result['measure_date'] = parsed_date.strftime("%Y-%m-%d")
								break
							except ValueError:
								continue
						# If all formats failed, use timestamp
						if metadata_result['measure_date'] == datetime.now().strftime("%Y-%m-%d"):
							metadata_result['measure_date'] = datetime.fromtimestamp(
								metadata_result['download_timestamp']).strftime("%Y-%m-%d")
					else:
						# If it's a timestamp, convert to date string
						metadata_result['measure_date'] = datetime.fromtimestamp(
							float(date_value)).strftime("%Y-%m-%d")
				else:
					# Use download timestamp as fallback for date
					metadata_result['measure_date'] = datetime.fromtimestamp(
						metadata_result['download_timestamp']).strftime("%Y-%m-%d")
			except Exception as e:
				self.logger.warning(f"Error reading metadata, using fallback values: {e}")
		else:
			self.logger.debug(f"No metadata found for {data_file}, using fallback values")
			# Try to extract cycle from filename
			filename = data_file.name
			if 'intraday' in filename.lower():
				match = INTRADAY_PATTERN.search(filename.lower())
				if match:
					metadata_result['cycle_name'] = f"Intraday {match.group(1)}"
			
			# Try to extract date from filename
			date_match = DATE_PATTERN.search(filename)
			if date_match:
				# Format: MM-DD-YYYY in filename
				month, day, year = date_match.groups()
				metadata_result['measure_date'] = f"{year}-{month}-{day}"
		
		# Format cycle_id by replacing spaces with underscores
		metadata_result['cycle_id'] = metadata_result['cycle_name'].replace(' ', '_')
		
		# Cache the result
		self.cache[data_file] = metadata_result
		return metadata_result


class DataProcessor(IDataProcessor):
	"""Base class for data processors."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the base data processor."""
		self.config = config
		self.logger = config.get_logger()
		self.output_dir = Path(config.get_config('output_dir'))
		# Add metadata extractor for all processors
		self.metadata_extractor = MetadataExtractor(config)

	@abstractmethod
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Process data file and fix quality issues.
		
		Args:
			data_file: Path to the file to process
			
		Returns:
			Path to the processed file (same as input)
		"""
		pass
	
	def process_dataframe(self, df: pd.DataFrame, data_file: Path) -> pd.DataFrame:
		"""
		Process a DataFrame directly without file I/O.
		
		Args:
			df: DataFrame to process
			data_file: Original file path (for metadata/logging)
			
		Returns:
			Processed DataFrame
		"""
		# Default implementation that subclasses should override
		return df
	
	def _ensure_output_dir(self) -> None:
		"""Ensure output directory exists for backups."""
		self.output_dir.mkdir(parents=True, exist_ok=True)
		
	def _create_backup(self, original_file: Path) -> Optional[Path]:
		"""Create a backup of the original file before modifying it."""
		try:
			self._ensure_output_dir()
			timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
			backup_file = self.output_dir / f"backup_{timestamp}_{original_file.name}"
			
			# Copy original to backup
			shutil.copy2(original_file, backup_file)
			
			self.logger.debug(f"Created backup at {backup_file}")
			return backup_file
		except Exception as e:
			self.logger.warning(f"Failed to create backup: {e}")
			return None
	
	def _read_csv_chunked(self, data_file: Path, chunksize: int = 100000) -> pd.DataFrame:
		"""
		Read a large CSV file in chunks and concatenate.
		
		Args:
			data_file: Path to the CSV file
			chunksize: Number of rows per chunk
			
		Returns:
			Complete DataFrame
		"""
		try:
			# Get file size to determine if chunking is needed (>50MB)
			file_size = os.path.getsize(data_file) / (1024 * 1024)  # Size in MB
			
			if file_size > 50:
				self.logger.debug(f"File size is {file_size:.2f}MB, using chunked reading")
				chunks = []
				for chunk in pd.read_csv(data_file, chunksize=chunksize):
					chunks.append(chunk)
				return pd.concat(chunks, ignore_index=True)
			else:
				return pd.read_csv(data_file)
		except Exception as e:
			self.logger.error(f"Error reading CSV file: {e}")
			# Return empty DataFrame as fallback
			return pd.DataFrame()


class CapacityDataProcessor(DataProcessor):
	"""Processor for capacity/operationally-available data."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the capacity data processor."""
		super().__init__(config)
		
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Process capacity data file to fix quality issues.
		
		Args:
			data_file: Path to the file to process
			
		Returns:
			Path to the processed file (same as input) or None if processing failed
		"""
		try:
			self.logger.info(f"Processing capacity data file: {data_file}")
			
			# Read the CSV file with chunking support for large files
			df = self._read_csv_chunked(data_file)
			if df.empty:
				return data_file
				
			# Process the DataFrame
			df = self.process_dataframe(df, data_file)
			
			# Save processed data back to the original file
			df.to_csv(data_file, index=False)
			self.logger.debug(f"Processed data saved back to original file: {data_file}")
			return data_file
			
		except Exception as e:
			self.logger.error(f"Error processing capacity data: {e}")
			return None
	
	def process_dataframe(self, df: pd.DataFrame, data_file: Path) -> pd.DataFrame:
		"""
		Process capacity data DataFrame to fix quality issues.
		
		Args:
			df: DataFrame to process
			data_file: Original file path (for metadata/logging)
			
		Returns:
			Processed DataFrame
		"""
		# Apply data fixes without creating copies unless necessary
		df = self._fix_duplicates(df)
		df = self._fix_inconsistent_data(df)
		return df
	
	def _fix_inconsistent_data(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Converts literals containing 'unavailable' in qty_reason to 'Unavailable'."""
		# Only create a copy if we're going to modify the DataFrame
		if 'qty_reason' not in df.columns:
			return df
			
		# Find rows where qty_reason contains 'unavailable'
		unavailable_mask = df['qty_reason'].str.contains('unavailable', case=False, na=False)
		
		if unavailable_mask.sum() > 0:
			# Create a copy only when we need to modify
			df_fixed = df.copy()
			unavailable_count = unavailable_mask.sum()
			
			# Replace qty_reason containing 'unavailable' with 'Unavailable'
			df_fixed.loc[unavailable_mask, 'qty_reason'] = 'Unavailable'
			
			self.logger.debug(f"Converted {unavailable_count} 'unavailable' values in qty_reason column to 'Unavailable'")
			return df_fixed
		
		# Return original DataFrame if no changes needed
		return df
	
	def _fix_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix duplicate records in the dataframe."""
		# Create a composite key for duplicate detection
		key_columns = [col for col in ['date', 'cycle', 'location'] if col in df.columns]
			
		if not key_columns:
			return df
			
		# Check if there are duplicates (faster than duplicated().sum())
		if len(df) == df[key_columns].drop_duplicates().shape[0]:
			return df
			
		# Create a copy only when we need to modify
		df_fixed = df.copy()
		duplicate_count = len(df) - df[key_columns].drop_duplicates().shape[0]
		
		if duplicate_count > 0:
			self.logger.info(f"Fixing {duplicate_count} duplicate records")
			
			# Define aggregation functions
			agg_columns = {}
			
			# Numeric columns get 'max' aggregation
			for col in ['capacity', 'available']:
				if col in df_fixed.columns:
					agg_columns[col] = 'max'
			
			# All other columns get 'first'
			for col in df_fixed.columns:
				if col not in key_columns and col not in agg_columns:
					agg_columns[col] = 'first'
					
			# Partition by composite key and aggregate duplicates
			df_fixed = df_fixed.groupby(key_columns, as_index=False).agg(agg_columns)
			
		return df_fixed


class LineageProcessor(DataProcessor):
	"""Processor that adds data lineage information to downloaded files."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the lineage processor."""
		super().__init__(config)
	
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Add lineage information (cycle_id, download_timestamp and measure_date) to CSV files.
		
		Args:
			data_file: Path to the file to process
			
		Returns:
			Path to the processed file or None if processing failed
		"""
		try:
			# Skip non-CSV files
			if data_file.suffix.lower() != '.csv':
				self.logger.debug(f"Skipping non-CSV file for lineage: {data_file}")
				return data_file
				
			self.logger.debug(f"Adding lineage information to: {data_file}")
			
			# Read CSV file with chunking support for large files
			df = self._read_csv_chunked(data_file)
			if df.empty:
				return data_file
				
			# Process the DataFrame
			df = self.process_dataframe(df, data_file)
			
			# Save back to the original file
			df.to_csv(data_file, index=False)
			return data_file
			
		except Exception as e:
			self.logger.error(f"Error adding lineage information: {e}")
			return data_file  # Return original file even if processing fails
	
	def process_dataframe(self, df: pd.DataFrame, data_file: Path) -> pd.DataFrame:
		"""
		Add lineage information to DataFrame.
		
		Args:
			df: DataFrame to process
			data_file: Original file path (for metadata)
			
		Returns:
			Processed DataFrame with lineage columns
		"""
		# Check if lineage columns already exist
		if all(col in df.columns for col in ['cycle_id', 'download_timestamp', 'measure_date']):
			self.logger.debug(f"Lineage information already exists, skipping")
			return df
			
		# Extract metadata from separate utility class
		metadata = self.metadata_extractor.extract_metadata(data_file)
		
		# Add lineage columns (no need to copy the DataFrame)
		df['measure_date'] = metadata['measure_date']
		df['cycle_id'] = metadata['cycle_id']
		df['download_timestamp'] = metadata['download_timestamp']
		
		self.logger.info(f"Added lineage information: cycle_id={metadata['cycle_id']}, " + 
						f"download_timestamp={metadata['download_timestamp']}, " + 
						f"measure_date={metadata['measure_date']}")
		return df


class HeaderSanitizerProcessor(DataProcessor):
	"""Processor that sanitizes CSV headers for database compatibility."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the header sanitizer processor."""
		super().__init__(config)
	
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Sanitize CSV file headers for database compatibility.
		
		Args:
			data_file: Path to the file to process
			
		Returns:
			Path to the processed file or None if processing failed
		"""
		try:
			# Skip non-CSV files
			if data_file.suffix.lower() != '.csv':
				self.logger.debug(f"Skipping non-CSV file for header sanitization: {data_file}")
				return data_file
				
			self.logger.debug(f"Sanitizing headers in: {data_file}")
			
			# Read CSV file with chunking support for large files
			df = self._read_csv_chunked(data_file)
			if df.empty:
				return data_file
				
			# Process the DataFrame
			df = self.process_dataframe(df, data_file)
			
			# Save back to the original file
			df.to_csv(data_file, index=False)
			return data_file
			
		except Exception as e:
			self.logger.error(f"Error sanitizing headers: {e}")
			return data_file
	
	def process_dataframe(self, df: pd.DataFrame, data_file: Path) -> pd.DataFrame:
		"""
		Sanitize DataFrame headers for database compatibility.
		
		Args:
			df: DataFrame to process
			data_file: Original file path (for logging)
			
		Returns:
			Processed DataFrame with sanitized headers
		"""
		# Store original headers for logging
		original_headers = list(df.columns)
		
		# Sanitize headers
		sanitized_headers = {}
		for col in df.columns:
			# Convert to lowercase
			new_col = col.lower()
			
			# Replace spaces with underscores
			new_col = new_col.replace(' ', '_')
			
			# Remove special characters that might cause DB issues
			# Using precompiled regex pattern
			new_col = SANITIZE_PATTERN.sub('_', new_col)
			
			# Ensure it doesn't start with a digit
			if new_col and new_col[0].isdigit():
				new_col = f"col_{new_col}"
			
			# Avoid duplicate column names
			count = 1
			base_col = new_col
			while new_col in sanitized_headers.values():
				new_col = f"{base_col}_{count}"
				count += 1
			
			sanitized_headers[col] = new_col
		
		# Check if any changes were made before renaming
		changed_cols = [old for old, new in sanitized_headers.items() if old != new]
		if not changed_cols:
			self.logger.debug(f"No header changes needed")
			return df
		
		# Rename columns - this creates a new DataFrame
		df_renamed = df.rename(columns=sanitized_headers)
		
		# Log the changes
		for old, new in sanitized_headers.items():
			if old != new:
				self.logger.debug(f"Renamed column: '{old}' -> '{new}'")
		
		self.logger.debug(f"Sanitized {len(changed_cols)} headers")
		return df_renamed


class DataPipeline:
	"""Pipeline pattern for processing data files through multiple processors."""
	
	def __init__(self, processors: List[DataProcessor], config: IConfigProvider):
		"""Initialize the data pipeline with processors."""
		self.processors = processors
		self.config = config
		self.logger = config.get_logger()
	
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Process a file through all processors with minimal I/O.
		
		Args:
			data_file: Path to the file to process
			
		Returns:
			Path to the processed file
		"""
		try:
			# Skip non-CSV files
			if data_file.suffix.lower() != '.csv':
				self.logger.debug(f"Skipping non-CSV file: {data_file}")
				return data_file
				
			self.logger.info(f"Processing file through pipeline: {data_file}")
			
			# Read the file once
			df = self.processors[0]._read_csv_chunked(data_file)
			if df.empty:
				return data_file
			
			# Apply all processors to the DataFrame
			for processor in self.processors:
				df = processor.process_dataframe(df, data_file)
				
			# Write the file once
			df.to_csv(data_file, index=False)
			
			self.logger.info(f"Completed pipeline processing for: {data_file}")
			return data_file
			
		except Exception as e:
			self.logger.error(f"Error in data pipeline: {e}")
			return data_file


class DataProcessorFactory:
	"""Factory for creating appropriate data processors with extensible registry."""
	
	# Dictionary to store processor factories
	_processor_registry: Dict[str, Type[DataProcessor]] = {
		'header_sanitizer': HeaderSanitizerProcessor,
		'lineage': LineageProcessor,
		'capacity': CapacityDataProcessor
	}
	
	# Set default processor type
	_default_processor_type = 'lineage'
	
	@classmethod
	def register_processor(cls, data_type: str, processor_class: Type[DataProcessor]) -> None:
		"""
		Register a new processor type with the factory.
		
		Args:
			data_type: String identifier for the processor type
			processor_class: The processor class to instantiate
		"""
		cls._processor_registry[data_type.lower()] = processor_class
	
	@classmethod
	def create_processor(cls, data_type: str, config: IConfigProvider) -> DataProcessor:
		"""
		Create a data processor for the specified data type.
		
		Args:
			data_type: Type of data to process
			config: Configuration provider
			
		Returns:
			Appropriate data processor for the data type, or default if not found
		"""
		data_type = data_type.lower()
		
		# Try to get the processor class
		processor_class = cls._processor_registry.get(data_type)
		
		if processor_class:
			return processor_class(config)
		else:
			# Log warning when falling back
			logger = config.get_logger()
			logger.warning(f"No processor found for data type '{data_type}', "
						f"falling back to '{cls._default_processor_type}'")
			
			# Use default processor
			default_processor = cls._processor_registry.get(cls._default_processor_type)
			if default_processor:
				return default_processor(config)
			else:
				# Ultimate fallback if something is wrong with registry
				logger.error(f"Default processor '{cls._default_processor_type}' not found in registry.")
				return CapacityDataProcessor(config)
	
	@classmethod
	def create_pipeline(cls, data_type: str, config: IConfigProvider) -> DataPipeline:
		"""
		Create a pipeline with all processors needed for the data type.
		
		Args:
			data_type: Type of data to process
			config: Configuration provider
			
		Returns:
			DataPipeline with appropriate processors
		"""
		# Default processing pipeline: header sanitizer -> lineage -> type-specific
		processors = [
			HeaderSanitizerProcessor(config),
			LineageProcessor(config)
		]
		
		# Add type-specific processor if not already in the pipeline
		specific_processor = cls.create_processor(data_type, config)
		if not any(isinstance(proc, type(specific_processor)) for proc in processors):
			processors.append(specific_processor)
			
		return DataPipeline(processors, config)
