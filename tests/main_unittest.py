#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# main_unittest.py
# Main test orchestrator for running all unit tests

import os
import sys
import pytest
import traceback
import glob
from pathlib import Path
import argparse
import time
from typing import List, Optional, Dict

def get_project_root() -> Path:
	"""
	Get the project root directory (parent directory of both src and tests).
	
	Returns:
		Path: The project root directory
	"""
	# If called from a test file
	current_file = Path(__file__).resolve()
	if 'tests' in current_file.parts:
		return current_file.parent.parent
	
	# If called from somewhere else (like src/main.py)
	current_dir = Path.cwd()
	if current_dir.name == 'src':
		return current_dir.parent
	elif current_dir.name == 'tests':
		return current_dir.parent
		
	# Last resort, try to find by going up from current file
	return current_file.parent.parent

def find_test_files(test_dir: Optional[Path] = None, pattern: str = 'ut_*.py') -> List[str]:
	"""
	Find all test files matching the pattern in the given directory.
	
	Args:
		test_dir: Directory to search for test files
		pattern: Glob pattern for test files
		
	Returns:
		List of test file paths
	"""
	# Default to tests directory if none specified
	if test_dir is None:
		project_root = get_project_root()
		test_dir = project_root / 'tests'
	
	# Get absolute path
	test_dir_path = Path(test_dir).resolve()
	
	# Find all test files and return absolute paths
	file_paths = glob.glob(str(test_dir_path / pattern))
	return [str(Path(f).resolve()) for f in file_paths]

def run_all_tests(test_dir: Optional[Path] = None, verbose: bool = False, 
				coverage: bool = False, default_endpoint: str = 'capacity/operationally-available') -> int:
	"""
	Run all test files found in the test directory.
	
	Args:
		test_dir: Directory to search for test files
		verbose: Whether to run with verbose output
		coverage: Whether to generate coverage report
		default_endpoint: Default endpoint to use in tests
		
	Returns:
		Exit code from pytest
	"""
	
	# Set default endpoint in environment variable for tests to use if needed
	os.environ['DEFAULT_TEST_ENDPOINT'] = default_endpoint
	
	# Get project root for proper path resolution
	project_root = get_project_root()

	# Set environment variable for coverage directory
	coverage_dir = str(project_root / 'tests')
	os.environ['COVERAGE_DIRECTORY'] = coverage_dir
	
	# Add project directories to sys.path for proper imports
	# This is necessary for pytest to find the modules in src and tests directories
	sys_paths = [
		str(project_root.resolve()),
		str(project_root.resolve() / 'src'),
		str(project_root.resolve() / 'tests')
	]
	
	for path in sys_paths:
		if path not in sys.path:
			sys.path.append(path)

	# Get all test files
	test_files = find_test_files(test_dir)
	
	if not test_files:
		print('No test files found')
		return 1
	
	print(f"Found {len(test_files)} test files:")
	for file in test_files:
		print(f"  - {file}")
	
	# Build arguments for pytest
	pytest_args = []
	
	# Add the test files
	pytest_args.extend(test_files)
	
	# Add verbose flag if requested
	if verbose:
		pytest_args.append('-v')

	# Add coverage configuration if requested
	if coverage:
		config_file = str(get_project_root() / '.coveragerc')
		pytest_args.append('--cov=src')
		pytest_args.append(f'--cov-config={config_file}')
		pytest_args.append('--cov-report=term')
		pytest_args.append('--cov-report=html')

	# Run pytest with arguments
	print(f"\nRunning pytest with arguments: {pytest_args}")
	start_time = time.time()
	result_code = pytest.main(pytest_args)
	execution_time = time.time() - start_time
	
	print(f"\nTest execution completed in {execution_time:.2f} seconds.")

	return result_code

def main() -> int:
	"""Main entry point for when called from external scripts."""
	try:
		return run_all_tests(verbose=True, coverage=True)
	except Exception as e:
		print(f"Error running tests: {e}")
		traceback.print_exc()
		return 1

if __name__ == "__main__":
	"""Main entry point when called directly from command line as a standalone."""

	# Create argument parser for command line options
	parser = argparse.ArgumentParser(description='Run unit tests for the application')
	parser.add_argument('--verbose', action='store_true', help='Verbose output')
	parser.add_argument('--coverage', action='store_true', help='Generate coverage report')
	parser.add_argument('--dir', help='Directory containing test files')

	args = parser.parse_args()
	
	# Run all tests
	try:
		exit_code = run_all_tests(args.dir, args.verbose, args.coverage)
		sys.exit(exit_code)
	except Exception as e:
		print(f"Error running tests: {e}")
		traceback.print_exc()
		sys.exit(1)
