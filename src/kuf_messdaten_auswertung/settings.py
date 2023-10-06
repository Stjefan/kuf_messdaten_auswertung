from dotenv import load_dotenv
import os
import logging

CS = os.getenv("POSTGRES_CS")

print("CS", CS)


logger = logging.getLogger("kufiauswertung")

logger_config_parser = logging.getLogger("configparser")