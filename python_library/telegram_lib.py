from .prefect_lib import Prefect
import requests
class Telegram:
    def __init__(self):
        self.prf = Prefect()
        self.prf.get_telegram_creds()
        self.creds = self.prf.telegram_creds

    def send_message(self, message: str):
        url = f"https://api.telegram.org/bot{self.creds['bot_token']}/sendMessage"
        payload = {"chat_id": self.creds['chat_id'], "text": message}
        try:
            response = requests.post(url=url, data=payload)
            if response.status_code == 200:
                print("Message sent successfully")
            else:
                print(f"Failed to send message: {response.status_code}")
                print(response.text)
        except Exception as e:
            print(f"Error sending message: {e}")