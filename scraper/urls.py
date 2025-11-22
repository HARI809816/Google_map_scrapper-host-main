from django.urls import path
from . import views
from . import auth_views

urlpatterns = [
    # Main scraper functionality
    path('', views.home, name='home'),
    path('update-custom-search/', views.update_custom_search, name='update_custom_search'),
    path('update-subcategory-options/', views.update_subcategory_options, name='update_subcategory_options'),
    path('cancel-scraping/', views.cancel_scraping, name='cancel_scraping'),  # Add this line
    # Authentication URLs
    path('signup/', auth_views.signup_view, name='signup'),
    path('login/', auth_views.login_view, name='login'),
    path('logout/', auth_views.logout_view, name='logout'),
    path('verify-otp/', auth_views.verify_otp_view, name='verify_otp'),
    path('resend-otp/', auth_views.resend_otp_view, name='resend_otp'),
    # User Profile URLs
    path('profile/', auth_views.profile_view, name='profile'),
    path('downloads/', auth_views.downloads_view, name='downloads'),
    path('download/<int:download_id>/', auth_views.download_file_view, name='download_file'),
    # Check auth status
    path('check-auth/', auth_views.check_auth_status, name='check_auth_status'),
]