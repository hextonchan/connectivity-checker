#!/bin/bash
python3 -m venv ./venv
source venv/bin/activate
pip --require-virtualenv install -r requirements.txt
deactivate