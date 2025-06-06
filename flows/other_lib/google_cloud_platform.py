from .auth_and_token import GoogleAuthManager

class GoogleCloudPlatform:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.use_service_account = use_service_account
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.client_secret_directory = client_secret_directory
        self.cloud_manager = None

    def _get_cloud_manager(self):
        self.auth_manager.check_cred()
        self.cloud_manager = self.auth_manager.credentials.cloud_manager

    def get_iam_policy(self, project_id):
        self._get_cloud_manager()
        cloud_manager = self.cloud_manager
        policy = cloud_manager.projects().getIamPolicy(
            resource=project_id,
            body={}
        ).execute()
        return policy

    def grant_permission(self, project_id, role, member_email):
        self._get_cloud_manager()
        cloud_manager = self.cloud_manager
        policy = self.get_iam_policy(project_id=project_id)

        bindings = policy.get('bindings', [])

        role_binding = next((b for b in bindings if b['role'] == role), None)
        member = f"user:{member_email}"

        if role_binding:
            if member not in role_binding['members']:
                role_binding['members'].append(member)
                print(f"Added {member} to existing role {role}")
            else:
                print(f"{member} already has role {role}")
                return
        else:
            bindings.append({
                'role': role,
                'members': [member]
            })
            print(f"Created new binding for role {role} and added {member}")

        # Set the updated IAM policy
        policy['bindings'] = bindings
        result = cloud_manager.projects().setIamPolicy(
            resource=project_id,
            body={'policy': policy}
        ).execute()
        return result
    
    def revoke_permission(self, project_id, role, member_email):
        self._get_cloud_manager()
        cloud_manager = self.cloud_manager
        policy = self.get_iam_policy(project_id=project_id)

        bindings = policy.get('bindings', [])

        role_binding = next((b for b in bindings if b['role'] == role), None)
        member = f"user:{member_email}"

        if role_binding:
            if member in role_binding['members']:
                role_binding['members'].remove(member)
                print(f"Removed {member} from role {role}")
            else:
                print(f"{member} does not have role {role}")
                return
        else:
            print(f"Role {role} not found")
            return
        result = cloud_manager.projects().setIamPolicy(
            resource=project_id,
            body={'policy': policy}
        ).execute()
        return result
