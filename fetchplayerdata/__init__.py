import datetime
import logging
import pymongo
import asyncio
from .scrapedata import addData
import os
import azure.functions as func
from dotenv import load_dotenv

load_dotenv()


async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    print("This function is executed successfully!")
    #await addData()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
