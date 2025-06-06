from .auth_and_token import GoogleAuthManager

class GoogleVPC:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.use_service_account = use_service_account
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.client_secret_directory = client_secret_directory
        self.vpc_access = None

    def get_vpc_access(self):
        self.auth_manager.check_cred()
        self.vpc_access = self.auth_manager.credentials.gg_vpc