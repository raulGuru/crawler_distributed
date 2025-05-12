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
    "meta_description_extraction": {
        "priority": 100,
        "ttr": 120,
        "instances": 2,
        "worker_script_file": "meta_description_worker.py"
    },
    "headings_extraction": {
        "priority": 100,
        "ttr": 150,
        "instances": 2,
        "worker_script_file": "headings_worker.py"
    },
    "canonical_extraction": {
        "priority": 100,
        "ttr": 150,
        "instances": 2,
        "worker_script_file": "canonical_worker.py"
    },

}
