import pyodbc
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
import tempfile
import logging

logging.basicConfig(
    filename='lmsapp.log',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s]: %(message)s',
)

