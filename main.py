"""
Software Development Capstone - SolvX

Authors: Will Castillo, Addison Farley, RJ Trenchard, Mason Sain, Noah Lanctot

Version 1.0

This application runs a script every 24 hours that will pull relevant energy data from the Energy Information Administration (EIA) API, process and clean it, then will send it to a BigQuery database for storage.
"""

import time
import subprocess

def run_script():
    #run energy script
    subprocess.run(['python', 'solv4solvx.py'])

while True:
    run_script()
    #sleep for 24 hours (24 * 60 * 60 seconds)
    time.sleep(24 * 60 * 60)