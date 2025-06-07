#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# processors.py
# This module provides data processing capabilities.

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set, Type
from abc import ABC, abstractmethod
from datetime import datetime
import time
import json
import re

from interfaces import IConfigProvider, IDataProcessor


class DataProcessor(IDataProcessor):
	"""Base class for data processors."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the base data processor."""
		self.config = config
		self.logger = config.get_logger()
		self.output_dir = Path(config.get_config('output_dir'))

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
			import shutil
			shutil.copy2(original_file, backup_file)
			
			self.logger.debug(f"Created backup at {backup_file}")
			return backup_file
		except Exception as e:
			self.logger.warning(f"Failed to create backup: {e}")
			return None


class CapacityDataProcessor(DataProcessor):
	"""Processor for capacity/operationally-available data."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the capacity data processor."""
		super().__init__(config)
		
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Process capacity data file to fix quality issues.
		
		Modifies the original file in-place after creating a backup.
		
		Args:
			data_file: Path to the file to process
			
		Returns:
			Path to the processed file (same as input) or None if processing failed
		"""
		try:
			self.logger.info(f"Processing capacity data file: {data_file}")
			
			# Read the CSV file
			df = pd.read_csv(data_file)
			
			# Apply data fixes
			df = self._fix_duplicates(df)
			df = self._fix_inconsistent_data(df)
			
			# Save processed data back to the original file
			df.to_csv(data_file, index=False)

			self.logger.debug(f"Processed data saved back to original file: {data_file}")

			return data_file
			
		except Exception as e:
			self.logger.error(f"Error processing capacity data: {e}")
			return None
	
	def _fix_inconsistent_data(self, df: pd.DataFrame) -> pd.DataFrame:
		"""
		Converts 'unavailable' markers in the qty_reason column to 'N'.
		- Values containing 'unavailable' in qty_reason will be replaced with 'N'
		- Empty values in other columns are left unchanged
		"""
		df_fixed = df.copy()
		unavailable_count = 0
		
		# Handle 'unavailable' values only in the qty_reason column
		if 'qty_reason' in df_fixed.columns:
			# Find rows where qty_reason contains 'unavailable'
			unavailable_mask = df_fixed['qty_reason'].str.contains('unavailable', case=False, na=False)
			
			if unavailable_mask.sum() > 0:
				unavailable_count = unavailable_mask.sum()
				
				# Replace qty_reason containing 'unavailable' with 'N'
				df_fixed.loc[unavailable_mask, 'qty_reason'] = 'N'
				
				self.logger.debug(f"Converted {unavailable_count} 'unavailable' values in qty_reason column to 'N'")
		
		return df_fixed
	
	def _fix_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix duplicate records in the dataframe."""
		df_fixed = df.copy()
		
		# For capacity data, identify duplicate keys
		key_columns = []
		if 'date' in df_fixed.columns:
			key_columns.append('date')
		if 'cycle' in df_fixed.columns:
			key_columns.append('cycle')
		if 'location' in df_fixed.columns:
			key_columns.append('location')
			
		if key_columns:
			# Check if there are duplicates
			duplicate_count = df_fixed.duplicated(subset=key_columns, keep=False).sum()
			
			if duplicate_count > 0:
				self.logger.info(f"Fixing {duplicate_count} duplicate records")
				
				# Get columns to aggregate
				agg_columns = {}
				if 'capacity' in df_fixed.columns:
					agg_columns['capacity'] = 'max'
				if 'available' in df_fixed.columns:
					agg_columns['available'] = 'max'
				
				# Include any other columns as 'first'
				for col in df_fixed.columns:
					if col not in key_columns and col not in agg_columns:
						agg_columns[col] = 'first'
				
				# Aggregate duplicates
				df_fixed = df_fixed.groupby(key_columns).agg(agg_columns).reset_index()
			
		return df_fixed


class LineageProcessor(DataProcessor):
	"""Processor that adds data lineage information to downloaded files."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the lineage processor."""
		super().__init__(config)
	
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Add lineage information (cycle_id and timestamp) to CSV files.
		
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
			
			# Find associated metadata file
			metadata_dir = Path(self.config.get_config('output_dir')) / "metadata"
			metadata_path = metadata_dir / f"{data_file.stem}.meta.json"
			
			# Initialize with fallback values
			cycle_name = "Unknown_Cycle"
			download_timestamp = time.time()  # Current timestamp as fallback
			
			# Try to get values from metadata if available
			if metadata_path.exists():
				try:
					with open(metadata_path, 'r') as f:
						metadata = json.load(f)
					
					# Extract metadata fields with fallbacks
					if 'cycle_name' in metadata and metadata['cycle_name']:
						cycle_name = metadata['cycle_name']
					else:
						# Try to extract cycle from filename
						filename = data_file.name
						if 'intraday' in filename.lower():
							import re
							match = re.search(r'intraday\s*(\d+)', filename.lower())
							if match:
								cycle_name = f"Intraday {match.group(1)}"
								self.logger.debug(f"Extracted cycle name from filename: {cycle_name}")
					
					if 'download_timestamp' in metadata and metadata['download_timestamp']:
						download_timestamp = metadata['download_timestamp']
					else:
						# Use file modification time as fallback
						download_timestamp = data_file.stat().st_mtime
						self.logger.debug(f"Using file modification time as timestamp fallback for {data_file}: {download_timestamp}")
				except Exception as e:
					self.logger.warning(f"Error reading metadata, using fallback values: {e}")
			else:
				self.logger.warning(f"No metadata found for {data_file}, using fallback values")
				# Try to extract cycle from filename as fallback
				filename = data_file.name
				if 'intraday' in filename.lower():
					import re
					match = re.search(r'intraday\s*(\d+)', filename.lower())
					if match:
						cycle_name = f"Intraday {match.group(1)}"
						self.logger.debug(f"Extracted cycle name from filename: {cycle_name}")
			
			# Format cycle_id by replacing spaces with underscores
			cycle_id = cycle_name.replace(' ', '_')
			
			# Read CSV file
			df = pd.read_csv(data_file)

			# Check if lineage columns already exist: if they do, don't modify the file
			if 'cycle_id' in df.columns and 'download_timestamp' in df.columns:
				self.logger.debug(f"Lineage information already exists in {data_file}, skipping")
				return data_file
			
			# Add lineage columns
			df['cycle_id'] = cycle_id
			df['download_timestamp'] = download_timestamp
			
			# Save back to the original file
			df.to_csv(data_file, index=False)

			self.logger.info(f"Added lineage information to {data_file}: cycle_id={cycle_id}, download_timestamp={download_timestamp}")
			return data_file
			
		except Exception as e:
			self.logger.error(f"Error adding lineage information: {e}")
			return data_file  # Return original file even if processing fails


class HeaderSanitizerProcessor(DataProcessor):
	"""Processor that sanitizes CSV headers for database compatibility."""
	
	def __init__(self, config: IConfigProvider):
		"""Initialize the header sanitizer processor."""
		super().__init__(config)
	
	def process(self, data_file: Path) -> Optional[Path]:
		"""
		Sanitize CSV file headers by:
		- Converting to lowercase
		- Replacing spaces with underscores
		- Removing problematic symbols
		
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
			
			# Read CSV file
			df = pd.read_csv(data_file)
			
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
				# Keep alphanumeric chars and underscores, replace others with underscore
				new_col = re.sub(r'[^\w]', '_', new_col)
				
				# Ensure it doesn't start with a digit (some databases don't allow this)
				if new_col and new_col[0].isdigit():
					new_col = f"col_{new_col}"
				
				# Avoid duplicate column names by adding suffix if needed
				count = 1
				base_col = new_col
				while new_col in sanitized_headers.values():
					new_col = f"{base_col}_{count}"
					count += 1
				
				sanitized_headers[col] = new_col
			
			# Rename columns
			df = df.rename(columns=sanitized_headers)
			
			# Check if any changes were made
			if original_headers == list(df.columns):
				self.logger.debug(f"No header changes needed for {data_file}")
				return data_file
			
			# Save back to the original file
			df.to_csv(data_file, index=False)
			
			# Log the changes
			for old, new in sanitized_headers.items():
				if old != new:
					self.logger.debug(f"Renamed column: '{old}' -> '{new}'")
			
			self.logger.debug(f"Sanitized {len([old for old, new in sanitized_headers.items() if old != new])} headers in {data_file}")
			return data_file
			
		except Exception as e:
			self.logger.error(f"Error sanitizing headers: {e}")
			return data_file  # Return original file even if processing fails


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
				logger.error(f"Default processor '{cls._default_processor_type}' not found in registry! "
						f"This indicates a configuration error.")
				return CapacityDataProcessor(config)
