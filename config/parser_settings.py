#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Centralized settings file for the parser system.
Contains parser-specific settings.
"""

from config.base_settings import *

ALL_PARSER_TASK_TYPES = {
    "page_title_extraction": {
        "priority": 100,
        "ttr": 120,
        "instances": 2, # Example: run 2 instances for page title
        "worker_script_file": "page_title_worker.py"
    },
    # "headings_extraction": {
    #     "priority": 100,
    #     "ttr": 150,
    #     "instances": 1,
    #     "worker_script_file": "headings_worker.py" # Assuming a future headings_worker.py
    # },
    # Add other task_type specific settings if needed
    # Example for a future worker:
    # "meta_description_extraction": {
    #     "priority": 100,
    #     "ttr": 120,
    #     "instances": 1,
    #     "worker_script_file": "meta_description_worker.py"
    # },
}
