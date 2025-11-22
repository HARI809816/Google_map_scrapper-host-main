# from django.contrib import admin
# from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
# from django.contrib.auth.models import User
# from .models import (
#     UserProfile,  OTPVerification, 
#     ScrapeJob, DownloadHistory, Gym, UserApprovalRequest
# )
# from django.conf import settings
# from django.core.mail import send_mail
# from django.utils import timezone
# from django.utils.crypto import get_random_string
# from .models import LoginUser

# # --- User with Profile ---
# class UserProfileInline(admin.StackedInline):
#     model = UserProfile
#     can_delete = False
#     verbose_name_plural = 'User Profile'

# class UserAdmin(BaseUserAdmin):
#     inlines = (UserProfileInline,)



# # @admin.register(UserApprovalRequest)
# # class UserApprovalRequestAdmin(admin.ModelAdmin):
# #     list_display = ('email', 'username', 'status', 'requested_at')
# #     list_filter = ('status',)
# #     actions = ['approve_requests']
# #     def approve_requests(self, request, queryset):
# #         approved_count = 0
# #         for req in queryset.filter(status='pending'):
# #             try:
# #                 # Create user in LoginUser table (not Django User)
# #                 login_user = LoginUser.objects.create(
# #                     username=req.username,
# #                     first_name=req.first_name,
# #                     last_name=req.last_name,
# #                     email=req.email,
# #                     phone=req.phone,
# #                     password=req.password,  # Already hashed
# #                     is_active=True,
# #                     is_approved=True
# #                 )
                
# #                 # Mark request as approved
# #                 req.status = 'approved'
# #                 req.approved_by = login_user  # Reference to LoginUser
# #                 req.approved_at = timezone.now()
# #                 req.save()
                
# #                 approved_count += 1
                
# #             except Exception as e:
# #                 self.message_user(request, f"Failed to approve {req.email}: {e}", level='error')
# #                 continue
        
# #         if approved_count > 0:
# #             self.message_user(request, f"{approved_count} user(s) approved and created in LoginUser table!")
    
# #     approve_requests.short_description = "Approve selected requests"

# @admin.register(UserApprovalRequest)
# class UserApprovalRequestAdmin(admin.ModelAdmin):
#     list_display = ('email', 'username', 'status', 'requested_at')
#     list_filter = ('status',)
#     actions = ['approve_requests']
#     def approve_requests(self, request, queryset):
#         approved_count = 0
#         for req in queryset.filter(status='pending'):
#             try:
#                 # Create user in LoginUser table
#                 login_user = LoginUser.objects.create(
#                     username=req.username,
#                     first_name=req.first_name,
#                     last_name=req.last_name,
#                     email=req.email,
#                     phone=req.phone,
#                     password=req.password,  # Already hashed
#                     is_active=True,
#                     is_approved=True
#                 )
                
#                 # Mark request as approved with the current admin as approver
#                 req.status = 'approved'
#                 req.approved_by = request.user  # Set to the logged-in admin
#                 req.approved_at = timezone.now()
#                 req.save()
                
#                 approved_count += 1
                
#             except Exception as e:
#                 self.message_user(request, f"Failed to approve {req.email}: {e}", level='error')
#                 continue
        
#         if approved_count > 0:
#             self.message_user(request, f"{approved_count} user(s) approved and created in LoginUser table!")

#     approve_requests.short_description = "Approve selected requests"


# # # --- Other Models ---
# # @admin.register(OTPVerification)
# # class OTPVerificationAdmin(admin.ModelAdmin):
# #     list_display = ('user', 'email', 'purpose', 'is_used', 'created_at', 'expires_at')
# #     list_filter = ('purpose', 'is_used')
# #     search_fields = ('user__username', 'email')

# @admin.register(ScrapeJob)
# class ScrapeJobAdmin(admin.ModelAdmin):
#     list_display = ('user', 'main_category', 'location', 'status', 'total_found', 'created_at')
#     list_filter = ('status', 'main_category')
#     search_fields = ('user__username', 'location')

# @admin.register(DownloadHistory)
# class DownloadHistoryAdmin(admin.ModelAdmin):
#     list_display = ('user', 'file_name',  'created_at')
#     list_filter = ('user',)
#     search_fields = ('file_name',)

# # @admin.register(Gym)
# # class GymAdmin(admin.ModelAdmin):
# #     list_display = ('name', 'address', 'phone', 'rating')
# #     search_fields = ('name', 'address')

# # Unregister default User admin and register ours
# admin.site.unregister(User)
# admin.site.register(User, UserAdmin)

# # @admin.register(LoginUser)
# # class LoginUserAdmin(admin.ModelAdmin):
# #     list_display = ('username', 'email', 'is_active', 'is_approved', 'date_joined')
# #     list_filter = ('is_active', 'is_approved')
# #     search_fields = ('username', 'email')
# #     ordering = ['-date_joined']
# #     actions = ['activate_users', 'deactivate_users']
    
# #     def activate_users(self, request, queryset):
# #         queryset.update(is_active=True)
# #         self.message_user(request, f"{queryset.count()} user(s) activated.")
    
# #     activate_users.short_description = "Activate selected users"
    
# #     def deactivate_users(self, request, queryset):
# #         queryset.update(is_active=False)
# #         self.message_user(request, f"{queryset.count()} user(s) deactivated.")
    
# #     deactivate_users.short_description = "Deactivate selected users"



from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from .models import (
    UserProfile, OTPVerification, 
    ScrapeJob, DownloadHistory, Gym, UserApprovalRequest
)
from django.conf import settings
from django.core.mail import send_mail
from django.utils import timezone
from django.utils.crypto import get_random_string
from .models import LoginUser
from .forms import UserApprovalRequestForm

# --- User with Profile ---
class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False
    verbose_name_plural = 'User Profile'

class UserAdmin(BaseUserAdmin):
    inlines = (UserProfileInline,)

@admin.register(UserApprovalRequest)
class UserApprovalRequestAdmin(admin.ModelAdmin):
    list_display = ('email', 'username', 'status', 'requested_at')
    list_filter = ('status',)
    actions = ['approve_requests']
    form = UserApprovalRequestForm  # Use the custom form

    def approve_requests(self, request, queryset):
        approved_count = 0
        for req in queryset.filter(status='pending'):
            try:
                login_user = LoginUser.objects.create(
                    username=req.username,
                    first_name=req.first_name,
                    last_name=req.last_name,
                    email=req.email,
                    phone=req.phone,
                    password=req.password,
                    is_active=True,
                    is_approved=True
                )
                req.status = 'approved'
                req.approved_by = request.user
                req.approved_at = timezone.now()
                req.save()
                approved_count += 1
            except Exception as e:
                self.message_user(request, f"Failed to approve {req.email}: {e}", level='error')
                continue
        if approved_count > 0:
            self.message_user(request, f"{approved_count} user(s) approved and created in LoginUser table!")

    approve_requests.short_description = "Approve selected requests"

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        if not obj or not obj.approved_by_id:
            if 'approved_by' in form.base_fields:
                form.base_fields['approved_by'].initial = request.user
        return form

    def get_readonly_fields(self, request, obj=None):
        readonly_fields = list(super().get_readonly_fields(request, obj) or [])
        if 'approved_by' not in readonly_fields:
            readonly_fields.append('approved_by')
        return readonly_fields

    def change_view(self, request, object_id, form_url='', extra_context=None):
        extra_context = extra_context or {}
        extra_context['show_save_and_continue'] = False
        extra_context['show_save'] = True
        return super().change_view(request, object_id, form_url, extra_context=extra_context)
# --- Other Models ---
@admin.register(ScrapeJob)
class ScrapeJobAdmin(admin.ModelAdmin):
    list_display = ('user', 'main_category', 'location', 'status', 'total_found', 'created_at')
    list_filter = ('status', 'main_category')
    search_fields = ('user__username', 'location')

@admin.register(DownloadHistory)
class DownloadHistoryAdmin(admin.ModelAdmin):
    list_display = ('user', 'file_name', 'created_at')
    list_filter = ('user',)
    search_fields = ('file_name',)

# Unregister default User admin and register ours
admin.site.unregister(User)
admin.site.register(User, UserAdmin)