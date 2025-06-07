#!/usr/bin/env python
# -*- coding: utf-8 -*-

# interfaces.py
# This module defines interfaces for the application components.

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Union, Any, Optional, Callable
from datetime import datetime
from pathlib import Path


class IConfigProvider(ABC):
	"""Interface for configuration providers."""
	
	@abstractmethod
	def get_logger(self) -> Any:
		"""Get the logger instance."""
		pass

	@abstractmethod
	def get_config(self, key: str, default: Any = None) -> Any:
		"""Get a configuration value."""
		pass

	@abstractmethod
	def set_config(self, key: str, value: Any) -> None:
		"""Set a configuration value."""
		pass


class IConnector(ABC):
	"""Interface for all connectors (web and database)."""
	
	@abstractmethod
	def register_cycle_callback(self, callback: Callable[[int, Optional[datetime]], None]) -> None:
		"""Register a callback for cycle updates."""
		pass
		
	@abstractmethod
	def notify_cycle_update(self, cycle: int, date: Optional[datetime] = None) -> None:
		"""Notify that a new cycle is available."""
		pass
	
	async def fetch_data(self, endpoint: str, cycle: Optional[int] = None, 
				date: Optional[datetime] = None, **params) -> Tuple[bool, List[Path]]:
		"""
		Fetch data from a source (async operation).
		
		Args:
			endpoint: The data endpoint/table/path
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Tuple of (success_flag, list_of_file_paths)
		"""
		raise NotImplementedError("This connector does not support fetch operations")
	
	async def fetch_data_with_retry(self, endpoint: str, cycle: Optional[int] = None, 
							date: Optional[datetime] = None, **params) -> Tuple[bool, List[Path]]:
		"""
		Fetch data with retry logic (async operation).
		
		Args:
			endpoint: The data endpoint/table/path
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Tuple of (success_flag, list_of_file_paths)
		"""
		raise NotImplementedError("This connector does not support fetch operations with retry")
	
	def post_data(self, endpoint: str, data_files: List[Path], 
				cycle: Optional[int] = None, date: Optional[datetime] = None, 
				**params) -> bool:
		"""
		Post data to a destination (sync operation).
		
		Args:
			endpoint: The data endpoint/table/path
			data_files: List of files to post
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Success flag
		"""
		raise NotImplementedError("This connector does not support post operations")
	
	def post_data_with_retry(self, endpoint: str, data_files: List[Path],
						cycle: Optional[int] = None, date: Optional[datetime] = None,
						**params) -> bool:
		"""
		Post data with retry logic (sync operation).
		
		Args:
			endpoint: The data endpoint/table/path
			data_files: List of files to post
			cycle: Optional cycle identifier
			date: Optional date
			**params: Additional parameters
			
		Returns:
			Success flag
		"""
		raise NotImplementedError("This connector does not support post operations with retry")

class IDataProcessor(ABC):
	"""Interface for data processors."""
	
	@abstractmethod
	def process(self, data_file: Path) -> Optional[Path]:
		"""Process data file and fix quality issues."""
		pass