import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import SENDER_EMAIL, RECEIVER_EMAIL, EMAIL_PASSWORD
import logging


def send_failure_email(error_message):

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "🚨 PySpark Job Failed!"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    html = f"""
    <html>
      <body>
        <h2>⚠️ PySpark Job Failure Alert</h2>
        <p><b>Error:</b> {error_message}</p>
      </body>
    </html>
    """
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        logging.info("✅ Failure email sent!")
    except Exception as e:
        logging.error("❌ Failed to send email:", e)