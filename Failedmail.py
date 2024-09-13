from Config import *
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
current_date = datetime.now().strftime('%d/%m/%Y')
def Failed_mail(t1,t2,err):
    from_email = os.getenv('FROM_EMAIL')
    from_password = os.getenv("DB_PASSWORD")
    #to_email="ajithkumaraji9047@gmail.com"
    to_email = os.getenv('Failed_To')
    #cc_email=''
    cc_email = os.getenv('Failed_CC')
    subject = '<Re:Hourly lead failed>'

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Cc'] = cc_email
    msg['Subject'] = subject

    # Email body
    body = f'''
        <html>
        <head>
            <style>
                body {{
                    font-family: Verdana, sans-serif;
                    font-size: 14px;
                    color: black;
                }}
                p {{
                    margin: 10px 0;
                }}
            </style>
        </head>
        <body>
            <p>Hi Team,</p>
            <p>Hourly lead count for apache and Ronin count got failed to send 
            timing: {t1} to {t2}.</p>
            <p>We got error: {err}</p>
            <b style="color: blue;">Thanks & Regards,<br></b>
            <p>Ajithkumar Sekar </p>
        </body>
        </html>
    '''
    msg.attach(MIMEText(body, 'html'))
    try:
        # Set up the SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, from_password)

        # Send the email
        recipients = [to_email] + cc_email.split(', ')
        server.sendmail(from_email, recipients, msg.as_string())
        print("Failed Email sent successfully!")
        logging.info("failed  Email sent successfully!")

    except Exception as e:
        print("An error occurred:", str(e))
        logging.error("An error occurred:%s", str(e))

    finally:
        server.quit()