import logging
import azure.functions as func
import asyncio
from datetime import datetime
# Get the absolute path to the project root
import os
import sys
project_root = os.path.abspath(os.path.dirname(__file__))

# Construct the path to the site-packages directory in your virtual environment
site_packages_path = os.path.join(project_root, '.venv', 'Lib', 'site-packages')

# Add the site-packages directory to sys.path
sys.path.append(site_packages_path)
from sportsdirectscrape import main

app = func.FunctionApp()

@app.schedule(schedule="30 10 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def sportsdirect_timer(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    try:
        asyncio.run(main())
        logging.info('Scraper executed successfully.')
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    current_time = datetime.utcnow().isoformat()
    logging.info(f'Sports Direct timer trigger function executed at {current_time}.')


@app.function_name(name="HelloWorld")
@app.route(route="hello")
def hello_world(req: func.HttpRequest) -> func.HttpResponse:
    current_time = datetime.utcnow().isoformat()
    logging.info(f'Hello world HTTP trigger function processed a request at {current_time}.')
    return func.HttpResponse("Hello, World!", status_code=200)


@app.function_name(name="SportsDirectScraperHTTP")
@app.route(route="run-scraper")
def run_scraper(req: func.HttpRequest) -> func.HttpResponse:
    try:
        asyncio.run(main())
        logging.info('Scraper executed successfully.')
        return func.HttpResponse("Scraper executed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)
