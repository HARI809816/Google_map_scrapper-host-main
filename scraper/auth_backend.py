from django.contrib.auth.backends import BaseBackend
from .models import LoginUser

class LoginUserBackend(BaseBackend):
    def authenticate(self, request, email=None, password=None):
        try:
            user = LoginUser.objects.get(email=email, is_active=True, is_approved=True)
            if user.check_password(password):
                return user
        except LoginUser.DoesNotExist:
            return None
        return None

    def get_user(self, user_id):
        try:
            return LoginUser.objects.get(pk=user_id)
        except LoginUser.DoesNotExist:
            return None