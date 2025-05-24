#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Centralized settings file for the parser system.
Contains parser-specific settings.
"""


import os

DEFAULT_PRIORITY = 100
DEFAULT_TTR = 150
DEFAULT_INSTANCES = 2

ALL_PARSER_TASK_TYPES = {
    "page_title": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("PAGE_TITLE_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "page_title_worker.py"
    },
    "meta_description": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("META_DESCRIPTION_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "meta_description_worker.py"
    },
    "headings": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("HEADINGS_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "headings_worker.py"
    },
    "canonical": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("CANONICAL_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "canonical_worker.py"
    },
    "amp": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("AMP_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "amp_worker.py"
    },
    "directives": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("DIRECTIVES_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "directives_worker.py"
    },
    "google_analytics": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("GOOGLE_ANALYTICS_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "google_analytics_worker.py"
    },
    "hreflang": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("HREFLANG_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "hreflang_worker.py"
    },
    "images": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("IMAGES_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "images_worker.py"
    },
    "javascript": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("JAVASCRIPT_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "javascript_worker.py"
    },
    "links": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("LINKS_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "links_worker.py"
    },
    "mobile": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("MOBILE_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "mobile_worker.py"
    },
    "page_elements": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("PAGE_ELEMENTS_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "page_elements_worker.py"
    },
    "page_speed": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("PAGESPEED_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "pagespeed_worker.py"
    },
    # "response_codes": {
    #     "priority": DEFAULT_PRIORITY,
    #     "ttr": DEFAULT_TTR,
    #     "instances": int(os.getenv("RESPONSE_CODES_INSTANCES", DEFAULT_INSTANCES)),
    #     "worker_script_file": "response_codes_worker.py"
    # },
    "structured": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("STRUCTURED_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "structured_worker.py"
    },
    "third_party_services": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("THIRD_PARTY_SERVICES_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "third_party_services_worker.py"
    },
    "uri": {
        "priority": DEFAULT_PRIORITY,
        "ttr": DEFAULT_TTR,
        "instances": int(os.getenv("URI_INSTANCES", DEFAULT_INSTANCES)),
        "worker_script_file": "uri_worker.py"
    }


}
