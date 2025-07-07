#!/bin/bash
export FLASK_APP=app.py  # Replace with your actual Flask entry point
#export FLASK_ENV=production   # Optional
python3 -m flask run --host=0.0.0.0 --port=5000

