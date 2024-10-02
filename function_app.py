import logging
import azure.functions as func
import sys
#sys.path.append(r'C:\Users\diego\OneDrive\Escritorio\Github\python-azure\.venv\Lib\site-packages')

logging.info(sys.path)
#from sportsdirectscrape import main
import asyncio
app = func.FunctionApp()

@app.schedule(schedule="30 10 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def sportsdirect_timer(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    # try:
    #     asyncio.run(main())
    #     logging.info('Scraper executed successfully.')
    # except Exception as e:
    #     logging.error(f"An error occurred: {e}")
    logging.info('Sports Direct timer trigger function executed.')