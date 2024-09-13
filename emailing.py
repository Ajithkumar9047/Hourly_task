import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
def email_generator(df1, df2,t1,t2):
    from_email = os.getenv('FROM_EMAIL')
    from_password = os.getenv('DB_PASSWORD')
    to_email = os.getenv('TO_EMAIL')
    cc_email =os.getenv('CC_EMAIL')
    subject = '<Re:TVS hourly mail report>'

    # Convert both DataFrames to HTML tables
    df1_html = df1.to_html(index=False, classes='table table1', border=1)
    df2_html = df2.to_html(index=False, classes='table table2', border=1)

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Cc'] = cc_email
    msg['Subject'] = subject

    # Email body with both DataFrames as separate tables
    body = f'''
        <html>
        <head>
            <style>
                body {{
                    font-family: Verdana, sans-serif;
                    font-size: 14px;
                    color: black;
                }}
                table {{
                    width: 30%;
                    border-collapse: collapse;
                }}
                th, td {{
                    border: 1px solid black;
                    padding: 8px;
                    text-align: center;
                }}
                .table1 th {{
                    background-color: #007BFF;  /* Blue color */
                    color: white;
                }}
                .table2 th {{
                    background-color: #007BFF;  /* Blue color */
                    color: white;
                }}
            </style>
        </head>
        <body>
            <p>Hi Rahul,</p>
            <p>Please find attached the lead report from {t1} to {t2}.</p>
            {df1_html}
            <br><br>
            {df2_html}
            <br><br>
            <b style="color: blue;">Thanks & Regards,<br></b>
            <p>Ajithkumar Sekar<br></p>
        </body>
        </html>
    '''
    msg.attach(MIMEText(body, 'html'))

    server = None  # Initialize server to None to avoid UnboundLocalError

    try:
        # Set up the SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, from_password)

        # Send the email
        recipients = [to_email] + cc_email.split(', ') if cc_email else [to_email]
        server.sendmail(from_email, recipients, msg.as_string())
        print(f"Email sent successfully! To:{to_email}, CC:{cc_email}")
        logging.info(f"Email sent successfully! To:{to_email}, CC:{cc_email}")

    except Exception as e:
        print("An error occurred:", str(e))
        logging.error("An error occurred: %s", str(e))

    finally:
        # Only quit the server if it was successfully initialized
        if server:
            server.quit()
