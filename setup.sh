#!/bin/bash
python3 -m venv venv
source venv/bin/activate

pip install -e ".[dev,all_db]"