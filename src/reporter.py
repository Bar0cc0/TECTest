#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# reporter.py
# This module provides data quality checking capabilities.

import json
import pandas as pd
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set, cast

from interfaces import IConfigProvider


class DataQualityReport:
	"""Class representing a data quality report with issues and metrics."""
	
	def __init__(self):
		self.issues = []
		self.metrics = {}
		self.summary = {}
		
	def add_issue(self, severity: str, issue_type: str, description: str, affected_records: Optional[List[int]] = None):
		"""Add an issue to the report."""
		self.issues.append({
			'severity': severity,
			'type': issue_type,
			'description': description,
			'affected_records': affected_records or []
		})
		
	def add_metric(self, name: str, value: Any):
		"""Add a metric to the report."""
		self.metrics[name] = value
		
	def set_summary(self, summary: Dict[str, Any]):
		"""Set the summary of the report."""
		self.summary = summary
		
	@property
	def passed(self) -> bool:
		"""Return True if data quality checks passed."""
		if not self.summary:
			return False
		return self.summary.get('status', 'FAILED') == 'PASSED'
	
	@property
	def quality_score(self) -> float:
		"""Get quality score between 0.0 and 1.0."""
		if not self.summary:
			return 0.0
		return self.summary.get('quality_score', 0.0)
		
	def get_issue_count_by_severity(self) -> Dict[str, int]:
		"""Get count of issues grouped by severity."""
		counts = defaultdict(int)
		for issue in self.issues:
			counts[issue['severity']] += 1
		return dict(counts)
	
	def to_dict(self) -> Dict[str, Any]:
		"""Convert report to dictionary."""
		return {
			'summary': self.summary,
			'metrics': self.metrics,
			'issues': self.issues,
			'issue_counts': self.get_issue_count_by_severity()
		}
	
	def to_json(self, indent: int = 2) -> str:
		"""Convert report to JSON string."""
		return json.dumps(self.to_dict(), indent=indent)
	
	def save(self, filepath: Path) -> None:
		"""Save report to JSON file."""
		with open(filepath, 'w') as f:
			f.write(self.to_json())


class DataQualityChecker:
	"""Base class for data quality checkers."""
	
	def __init__(self, config: IConfigProvider):
		self.config = config
		self.logger = config.get_logger()
		if not hasattr(DataQualityChecker, '_cached_output_dir'):
			DataQualityChecker._cached_output_dir = Path(config.get_config('output_dir'), 'quality_reports')
		self.output_dir = DataQualityChecker._cached_output_dir
		self.quality_report = None
	
	def check_data_quality(self, data_file: Path) -> DataQualityReport:
		"""Check the quality of data and generate a report."""
		raise NotImplementedError("Subclasses must implement check_data_quality method")
	
	def save_report(self, filename_prefix: str = "quality_report") -> Optional[Path]:
		"""Save the quality report to a file."""
		if not self.quality_report:
			self.logger.warning("No quality report to save")
			return None
			
		# Create output directory if it doesn't exist
		self.output_dir.mkdir(parents=True, exist_ok=True)
		
		# Generate filename with timestamp
		timestamp = datetime.now().strftime("%H%M%S")
		filename = f"{filename_prefix}_{timestamp}.json"
		report_path = self.output_dir / filename
		
		try:
			self.quality_report.save(report_path)
			self.logger.info(f"Quality report saved to: {report_path.parent}")
			return report_path
		except Exception as e:
			self.logger.error(f"Error saving quality report: {e}")
			return None


class CapacityDataQualityChecker(DataQualityChecker):
	"""Checker for capacity/operationally-available data."""
	
	def __init__(self, config: IConfigProvider):
		super().__init__(config)
		self.expected_columns = ['date', 'location', 'capacity', 'available', 'cycle']
	
	def check_data_quality(self, data_file: Path) -> DataQualityReport:
		"""Check the quality of capacity data."""
		# Create report instance
		self.quality_report = DataQualityReport()
		
		# Check if file exists
		if not data_file.exists():
			self.logger.error(f"Data file does not exist: {data_file}")
			self.quality_report.add_issue(
				'ERROR',
				'FILE_NOT_FOUND',
				f"Data file not found: {data_file}"
			)
			self._set_failed_summary()
			return self.quality_report
			
		try:
			# Read the CSV file
			df = pd.read_csv(data_file)
			
			# Check if empty
			if df.empty:
				self.logger.warning(f"Empty data file: {data_file}")
				self.quality_report.add_issue(
					'ERROR',
					'EMPTY_FILE',
					f"Data file is empty: {data_file}"
				)
				self._set_failed_summary()
				return self.quality_report
				
			# Run quality checks
			self._check_completeness(df)
			self._check_consistency(df)
			self._check_duplicates(df)
			self._check_missing_values(df)
			self._check_outliers(df)
			
			# Add basic metrics
			self._add_metrics(df)
			
			# Create summary
			quality_score = self._calculate_quality_score(df)
			status = 'PASSED' if quality_score >= 0.8 else 'WARNING' if quality_score >= 0.5 else 'FAILED'
			
			self.quality_report.set_summary({
				'record_count': len(df),
				'column_count': len(df.columns),
				'issue_count': len(self.quality_report.issues),
				'quality_score': quality_score,
				'status': status,
				'file_path': str(data_file)
			})
			
			return self.quality_report
			
		except Exception as e:
			self.logger.error(f"Error checking data quality: {e}")
			self.quality_report.add_issue(
				'ERROR',
				'QUALITY_CHECK_ERROR',
				f"Error checking data quality: {str(e)}"
			)
			self._set_failed_summary()
			return self.quality_report

	def _set_failed_summary(self) -> None:
		"""Set a failed summary for the report."""
		self.quality_report.set_summary({
			'record_count': 0,
			'column_count': 0,
			'issue_count': len(self.quality_report.issues),
			'quality_score': 0.0,
			'status': 'FAILED'
		})
		
	def _calculate_quality_score(self, df: pd.DataFrame) -> float:
		"""Calculate quality score based on issues."""
		issue_counts = self.quality_report.get_issue_count_by_severity()
		record_count = max(len(df), 1)
		
		error_weight = issue_counts.get('ERROR', 0) * 1.0
		warning_weight = issue_counts.get('WARNING', 0) * 0.5
		info_weight = issue_counts.get('INFO', 0) * 0.1
		
		total_weighted_issues = error_weight + warning_weight + info_weight
		score = max(0, 1 - (total_weighted_issues / (record_count * 2)))
		return round(score, 2)
	
	def _check_completeness(self, df: pd.DataFrame) -> None:
		"""Check if all required columns are present."""
		missing_columns = set(self.expected_columns) - set(df.columns)
		if missing_columns:
			self.quality_report.add_issue(
				'ERROR',
				'MISSING_COLUMNS',
				f"Missing required columns: {', '.join(missing_columns)}"
			)
	
	def _check_consistency(self, df: pd.DataFrame) -> None:
		"""Check for data consistency."""
		if all(col in df.columns for col in ["capacity", "available"]):
			# Find rows where capacity < available
			inconsistent = df[df["capacity"] < df["available"]]
			if not inconsistent.empty:
				self.quality_report.add_issue(
					'ERROR',
					'INCONSISTENT_DATA',
					f"Inconsistent capacity data: capacity < available for {len(inconsistent)} records",
					inconsistent.index.tolist()
				)
				
	def _check_duplicates(self, df: pd.DataFrame) -> None:
		"""Check for duplicate records."""
		if all(col in df.columns for col in ['date', 'cycle', 'location']):
			duplicate_keys = df.duplicated(subset=['date', 'cycle', 'location'], keep=False)
			duplicates = df[duplicate_keys]
			
			if not duplicates.empty:
				self.quality_report.add_issue(
					'WARNING',
					'DUPLICATE_RECORDS',
					f"Duplicate location entries found: {len(duplicates)} records",
					duplicates.index.tolist()
				)
				
	def _check_missing_values(self, df: pd.DataFrame) -> None:
		"""Check for missing values in important fields."""
		for col in df.columns:
			missing = df[df[col].isna()]
			if not missing.empty:
				severity = 'ERROR' if col in ['date', 'cycle', 'location'] else 'WARNING'
				self.quality_report.add_issue(
					severity,
					'MISSING_VALUES',
					f"Missing values in {col}: {len(missing)} records affected",
					missing.index.tolist()
				)
				
	def _check_outliers(self, df: pd.DataFrame) -> None:
		"""Check for outlier values that might indicate errors."""
		numeric_columns = df.select_dtypes(include=['number']).columns
		
		for col in numeric_columns:
			if col not in ['date', 'cycle']:
				try:
					Q1 = df[col].quantile(0.25)
					Q3 = df[col].quantile(0.75)
					IQR = Q3 - Q1
					
					lower_bound = Q1 - 1.5 * IQR
					upper_bound = Q3 + 1.5 * IQR
					
					outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
					if not outliers.empty:
						self.quality_report.add_issue(
							'INFO',
							'POTENTIAL_OUTLIERS',
							f"Potential outliers in {col}: {len(outliers)} records affected",
							outliers.index.tolist()
						)
				except Exception as e:
					self.logger.warning(f"Error checking outliers for {col}: {e}")
					
	def _add_metrics(self, df: pd.DataFrame) -> None:
		"""Add basic data quality metrics to the report."""
		# Column-level metrics
		for col in df.select_dtypes(include=['number']).columns:
			self.quality_report.add_metric(f"{col}_min", float(df[col].min()))
			self.quality_report.add_metric(f"{col}_max", float(df[col].max()))
			self.quality_report.add_metric(f"{col}_mean", round(float(df[col].mean()), 2))
			
		# Completeness metrics
		total_cells = df.shape[0] * df.shape[1]
		missing_cells = df.isna().sum().sum()
		completeness = round(1 - (missing_cells / total_cells), 4) if total_cells > 0 else 0
		self.quality_report.add_metric('completeness', completeness)
		
		# Uniqueness metrics
		if 'location' in df.columns:
			self.quality_report.add_metric('unique_locations', int(df['location'].nunique()))


class DataQualityCheckerFactory:
	"""Factory for creating appropriate data quality checkers."""
	
	@staticmethod
	def create_checker(data_type: str, config: IConfigProvider) -> DataQualityChecker:
		"""Create a data quality checker for the specified data type."""
		if data_type == 'capacity':
			return CapacityDataQualityChecker(config)
		else:
			# Default to capacity checker for now
			return CapacityDataQualityChecker(config)
