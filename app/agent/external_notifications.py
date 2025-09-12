import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import requests

class ExternalNotificationManager:
    def __init__(self):
        self.email_enabled = bool(os.getenv('SMTP_SERVER') and os.getenv('EMAIL_USER'))
        self.discord_enabled = bool(os.getenv('DISCORD_WEBHOOK_URL'))
        
    def send_email_notification(self, subject: str, message: str):
        if not self.email_enabled: return
        try:
            msg = MIMEMultipart()
            msg['From'] = os.getenv('EMAIL_USER')
            msg['To'] = os.getenv('NOTIFICATION_EMAIL')
            msg['Subject'] = f"ICT Trading Agent: {subject}"
            msg.attach(MIMEText(f"Notification at {datetime.now()}:\n\n{message}", 'plain'))
            
            with smtplib.SMTP(os.getenv('SMTP_SERVER'), int(os.getenv('SMTP_PORT', 587))) as server:
                server.starttls()
                server.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASSWORD'))
                server.send_message(msg)
            logging.info(f"Email notification sent: {subject}")
        except Exception as e:
            logging.error(f"Failed to send email: {e}")
    
    def send_discord_notification(self, message: str):
        if not self.discord_enabled: return
        try:
            data = {"content": f"[BOT] **ICT Trading Agent Alert**\n>>> {message}"}
            response = requests.post(os.getenv('DISCORD_WEBHOOK_URL'), json=data)
            response.raise_for_status()
            logging.info("Discord notification sent.")
        except Exception as e:
            logging.error(f"Failed to send Discord notification: {e}")
    
    def notify_trade_executed(self, symbol: str, side: str, qty: float, price: float, order_id: str):
        message = f"*** TRADE EXECUTED ***\n**Symbol**: {symbol}\n**Action**: {side.upper()} {qty:.4f}\n**Price**: ${price:.2f}\n**Order ID**: {order_id}"
        self.send_email_notification(f"Trade Executed: {side.upper()} {symbol}", message)
        self.send_discord_notification(message)
    
    def notify_system_error(self, error_type: str, error_message: str):
        message = f"*** SYSTEM ERROR ***\n**Type**: {error_type}\n**Message**: {error_message}"
        self.send_email_notification(f"System Error: {error_type}", message)
        self.send_discord_notification(message)

external_notifier = ExternalNotificationManager()
