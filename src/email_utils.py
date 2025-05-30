import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage


def send_email_report(
    sender_email: str, 
    sender_password: str, 
    recipient_email: str, 
    subject: str, 
    body: str, 
    email_attachments: list[str] | None = None,
    smtp_server: str = 'smtp.gmail.com',
    smtp_port: int = 465,
):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg.set_content(body)

    if email_attachments:
        add_attachments_to_email(msg, email_attachments)
    
    with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
        smtp.login(sender_email, sender_password)
        smtp.send_message(msg)
        

def add_attachments_to_email(
    msg: EmailMessage,
    file_paths: list[str],
):
    for file_path in file_paths:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The attachment file {file_path} does not exist.")
        
        with open(file_path, 'rb') as f:
            file_data = f.read()
            file_name = os.path.basename(file_path)
            msg.add_attachment(
                file_data, 
                maintype='application', 
                subtype='pdf', 
                filename=file_name
            )