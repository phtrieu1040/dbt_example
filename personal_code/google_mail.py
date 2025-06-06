import smtplib
import imaplib
import pandas as pd
import email
from email.header import decode_header
from .utility_lib import MyFunction
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class GoogleMail:
    def __init__(self, email_address, password) -> None:
        self.email_address = email_address
        self.password = password
        self._gmail = imaplib.IMAP4_SSL("imap.gmail.com")

    @property
    def gmail(self):
        return self._gmail
    
    @gmail.setter
    def gmail(self, new_gmail):
        self._gmail = new_gmail
    
    def _login_gmail(self):
        self._check_gmail_connection()
        try:
            self.gmail.login(self.email_address, self.password)
        except Exception as e:
            print(e)
            return False
        return True
    
    def _check_gmail_connection(self):
        try:
            status, _ = self.gmail.noop()
        except Exception as e:
            status = None
        if status == 'OK':
            return True
        else:
            return False

    def _log_out_gmail(self):
        self.gmail.logout()

    def get_email_ids(self):
        self._login_gmail()
        self._read_inbox()
        _, email_ids_str = self.gmail.search(None, 'All')
        email_ids = email_ids_str[0].split()
        return email_ids
    
    def _read_inbox(self):
        self.gmail.select('inbox')

    def fetch_email_by_id(self, id):
        self._login_gmail()
        self._read_inbox()
        _, msg_data = self.gmail.fetch(id, "(RFC822)")
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)
        subject, encoding = decode_header(msg["Subject"])[0]
        from_address = msg.get("From")
        date_sent = msg.get("Date")
        try:
            date_sent = MyFunction.convert_long_date_to_gmt7(date_sent)
        except Exception as e:
            print(id, e)
        print(date_sent)
        body=''
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))
                if "text/plain" in content_type and "attachment" not in content_disposition:
                    body += part.get_payload(decode=True).decode()
                elif "text/html" in content_type and "attachment" not in content_disposition:
                    body += part.get_payload(decode=True).decode()
                    
        else:
            body = msg.get_payload(decode=True).decode()
        body = MyFunction._remove_other_symbols(input_str=body)
        body = MyFunction._remove_accents(body)

        try:
            subject = subject.decode('utf-8')
            subject = MyFunction._remove_accents(subject)
            subject = MyFunction._remove_strange_symbols([subject])
        except Exception as e:
            print(id, e)
            pass
        sender = MyFunction._extract_email(from_address)
        return str(id), subject, sender, date_sent, body
    
    def send_email(self, subject, body, to_email):
        msg = MIMEMultipart()
        msg['From'] = self.email_address
        msg['To'] = to_email
        msg['Subject'] = subject

        # Convert DataFrame to HTML table
        if type(body) is pd.core.frame.DataFrame:
            body_type = 'html'
            email_body = body.to_html(index=False)
        else:
            body_type = 'plain'
            email_body = body

        # Attach the HTML table to the email body
        msg.attach(MIMEText(email_body, body_type))

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_address, self.password)
            text = msg.as_string()
            server.sendmail(self.email_address, to_email, text)
            server.quit()
            print("Email sent successfully!")
        except Exception as e:
            print(f"Error sending email: {e}")