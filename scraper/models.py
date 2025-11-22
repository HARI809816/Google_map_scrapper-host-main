from django.db import models
from django.contrib.auth.models import User
from django.core.mail import send_mail
from django.conf import settings
import uuid
import random
from datetime import datetime, timedelta
from django.utils import timezone
from django.contrib.auth.hashers import make_password, check_password

class LoginUser(models.Model):
    username = models.CharField(max_length=150, unique=True)
    first_name = models.CharField(max_length=30)
    last_name = models.CharField(max_length=30)
    email = models.EmailField(unique=True)
    phone = models.CharField(max_length=15, blank=True)
    password = models.CharField(max_length=128)  # Hashed password
    is_active = models.BooleanField(default=True)
    is_approved = models.BooleanField(default=False)
    date_joined = models.DateTimeField(auto_now_add=True)
    last_login = models.DateTimeField(null=True, blank=True)
    
    def set_password(self, raw_password):
        self.password = make_password(raw_password)
    
    def check_password(self, raw_password):
        return check_password(raw_password, self.password)
    
    def __str__(self):
        return self.email
    
class UserApprovalRequest(models.Model):
    username = models.CharField(max_length=150)
    first_name = models.CharField(max_length=30)
    last_name = models.CharField(max_length=30)
    email = models.EmailField()
    phone = models.CharField(max_length=15, blank=True)
    status = models.CharField(
        max_length=10,
        choices=[('pending', 'Pending'), ('approved', 'Approved'), ('rejected', 'Rejected')],
        default='pending'
    )
    requested_at = models.DateTimeField(auto_now_add=True)
    password = models.CharField(max_length=128, blank=True)  # Only if using password login
    approved_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    approved_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.email} ({self.status})"

class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    phone = models.CharField(max_length=15, blank=True)
    is_email_verified = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user.username} Profile"

class OTPVerification(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    email = models.EmailField()
    otp = models.CharField(max_length=6)
    purpose = models.CharField(max_length=20, choices=[
        ('signup', 'Signup Verification'),
        ('login', 'Login Verification'),
        ('password_reset', 'Password Reset'),
    ])
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    is_used = models.BooleanField(default=False)
    
    def save(self, *args, **kwargs):
        if not self.otp:
            self.otp = str(random.randint(100000, 999999))
        if not self.expires_at:
            self.expires_at = timezone.now() + timedelta(minutes=10)
        super().save(*args, **kwargs)
    
    def is_valid(self):
        return not self.is_used and timezone.now() < self.expires_at
    
    def send_otp(self):
        subject = f"Your OTP for {self.get_purpose_display()}"
        message = f"""
        Hello {self.user.first_name or self.user.username},
        
        Your OTP for {self.get_purpose_display()} is: {self.otp}
        
        This OTP will expire in 10 minutes.
        
        If you didn't request this, please ignore this email.
        
        Best regards,
        Multi-Industry Data Scraper Team
        """
        
        try:
            send_mail(
                subject,
                message,
                settings.DEFAULT_FROM_EMAIL,
                [self.email],
                fail_silently=False,
            )
            return True
        except Exception as e:
            print(f"Error sending OTP email: {e}")
            return False
    
    def __str__(self):
        return f"OTP for {self.user.username} - {self.purpose}"



class DownloadHistory(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    scrape_job = models.ForeignKey('ScrapeJob', on_delete=models.CASCADE, null=True, blank=True)
    file_name = models.CharField(max_length=255)
    file_path = models.CharField(max_length=500)
    file_size = models.BigIntegerField(default=0)
    download_count = models.IntegerField(default=0.0)
    created_at = models.DateTimeField(auto_now_add=True)
    last_downloaded = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.user.username} - {self.file_name}"

# Update existing ScrapeJob model
class ScrapeJob(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    MAIN_CATEGORIES = [
        ('fitness', 'Fitness'),
        ('business', 'Business'),
        ('electronic_shop', 'Electronic Shops'),
        ('ebike', 'E-Bike Showrooms'),
        ('college', 'Colleges'),
        ('training_institute', 'Training Institutes'),
        ('salon', 'Salons'),
        ('boutique', 'Boutiques'),
        ('custom', 'Custom Category'),
    ]
    
    SUBCATEGORY_CHOICES = {
        'fitness': [
            ('crossfit', 'CrossFit Boxes'),
            ('yoga', 'Yoga Studios'),
            ('pilates', 'Pilates Studios'),
            ('martial_arts', 'Martial Arts & Boxing Gyms'),
            ('swimming', 'Swimming Pools & Aquatic Centers'),
            ('all_gyms', 'All Fitness Types'),
        ],
        'business': [
            ('startup', 'Startup Companies'),
            ('manufacturing', 'Manufacturing Companies'),
            ('consultant', 'Business Consultants'),
            ('all_business', 'All Business Types'),
        ],
        'default': [],
    }

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job_id = models.CharField(max_length=100, unique=True, null=True, blank=True)  # Add this field
    user = models.ForeignKey(User, on_delete=models.CASCADE)  # Made required
    location = models.CharField(max_length=200)
    main_category = models.CharField(max_length=20, choices=MAIN_CATEGORIES, default='ebike')
    subcategory = models.CharField(max_length=20, blank=True, null=True)
    custom_term = models.CharField(max_length=100, blank=True)
    max_results = models.PositiveIntegerField(default=25)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    progress = models.IntegerField(default=0)
    total_found = models.IntegerField(default=0)
    error_message = models.TextField(blank=True, null=True)
    csv_file = models.FileField(upload_to='csv_files/', blank=True, null=True)
    
    class Meta:
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.main_category} - {self.subcategory or 'No subcategory'} in {self.location} - {self.status}"

class Gym(models.Model):
    scrape_job = models.ForeignKey(ScrapeJob, on_delete=models.CASCADE, related_name='gyms')
    name = models.CharField(max_length=300)
    address = models.TextField(blank=True)
    phone = models.CharField(max_length=50, blank=True)
    email = models.EmailField(blank=True)
    website = models.URLField(max_length=500, blank=True)
    rating = models.CharField(max_length=10, blank=True)
    reviews_count = models.CharField(max_length=20, blank=True)
    category = models.CharField(max_length=100, blank=True)
    gym_type = models.CharField(max_length=50, blank=True)
    hours = models.TextField(blank=True)
    description = models.TextField(blank=True)
    membership_fee = models.CharField(max_length=100, blank=True)
    facilities = models.TextField(blank=True)
    equipment = models.TextField(blank=True)
    classes_offered = models.TextField(blank=True)
    trainers_available = models.CharField(max_length=10, blank=True)
    parking_available = models.CharField(max_length=10, blank=True)
    locker_rooms = models.CharField(max_length=10, blank=True)
    shower_facilities = models.CharField(max_length=10, blank=True)
    air_conditioning = models.CharField(max_length=10, blank=True)
    accessibility = models.CharField(max_length=50, blank=True)
    group_classes = models.CharField(max_length=20, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['name']
    
    def __str__(self):
        return self.name
    

