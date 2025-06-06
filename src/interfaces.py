#!/usr/bin/env python
# -*- coding: utf-8 -*-

# interfaces.py
# This module defines interfaces for the application components.

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Union, Any, Optional, Callable
from datetime import datetime


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
	"""Interface for all connectors."""
	
	@abstractmethod
	def register_cycle_callback(self, callback: Callable[[int, Optional[datetime]], None]) -> None:
		"""Register a callback for cycle updates."""
		pass
		
	@abstractmethod
	def notify_cycle_update(self, cycle: int, date: Optional[datetime] = None) -> None:
		"""Notify that a new cycle is available."""
		pass