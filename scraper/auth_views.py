from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib import messages
from django.http import JsonResponse, Http404, HttpResponse
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.core.paginator import Paginator
from django.db.models import Q
from django.utils import timezone
from django.conf import settings
from django.views.decorators.cache import never_cache
from django.http import FileResponse
from .auth_forms import (
    SignupForm, LoginForm, ProfileUpdateForm, OTPVerificationForm
)
from .models import UserProfile, DownloadHistory, ScrapeJob
from django.db.models import Sum
from .models import UserApprovalRequest
from django.contrib.auth.hashers import make_password
from .models import OTPVerification
from django.urls import reverse
import os
import json
import mimetypes



def add_no_cache_headers(response):
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    return response

@login_required
def profile_view(request):
    profile, created = UserProfile.objects.get_or_create(user=request.user)
    
    if request.method == 'POST':
        form = ProfileUpdateForm(request.POST, instance=profile, user=request.user)
        if form.is_valid():
            form.save()
            messages.success(request, 'Profile updated successfully!')
            return redirect('profile')
    else:
        form = ProfileUpdateForm(instance=profile, user=request.user)
    
    # Get user's recent scrape jobs
    recent_jobs = ScrapeJob.objects.filter(user=request.user).order_by('-created_at')[:5]
    
    return render(request, 'auth/profile.html', {
        'form': form,
        'profile': profile,
        'recent_jobs': recent_jobs
    })

@login_required
def downloads_view(request):
    all_downloads = DownloadHistory.objects.filter(user=request.user)
    
    total_downloads = all_downloads.aggregate(total=Sum('download_count'))['total'] or 0
    days_since_last = 0
    if all_downloads.exists():
        last_dl = all_downloads.order_by('-created_at').first().created_at
        now = timezone.now()
        if timezone.is_naive(last_dl):
            last_dl = timezone.make_aware(last_dl)
        days_since_last = (now - last_dl).days

    paginator = Paginator(all_downloads.order_by('-created_at'), 20)
    downloads = paginator.get_page(request.GET.get('page'))

    return render(request, 'auth/downloads.html', {
        'downloads': downloads,
        'total_downloads': total_downloads,
        'days_since_last': days_since_last,
    })

def download_file_view(request, download_id):
    download = get_object_or_404(DownloadHistory, id=download_id, user=request.user)
    
    file_path = os.path.join(settings.MEDIA_ROOT, download.file_path.replace('/media/', ''))
    
    if not os.path.exists(file_path):
        raise Http404("File not found")

    # Open the file for streaming
    try:
        # Increment count ONLY when we're about to serve the file
        download.download_count += 1
        download.last_downloaded = timezone.now()
        download.save(update_fields=['download_count', 'last_downloaded'])

        response = FileResponse(
            open(file_path, 'rb'),
            as_attachment=True,
            filename=download.file_name
        )
        return response

    except Exception:
        raise Http404("Unable to serve file")

@never_cache
def logout_view(request):
    logout(request)
    request.session.flush()
    messages.success(request, 'You have been successfully logged out.')
    response = redirect('login')
    return add_no_cache_headers(response)

@never_cache
def signup_view(request):
    if request.user.is_authenticated:
        return redirect('home')
    
    if request.method == 'POST':
        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        form = SignupForm(request.POST)
        
        if form.is_valid():
            hashed_password = make_password(form.cleaned_data['password1'])
            
            # Create temporary User with is_active=False and directly set hashed password
            user = User(
                username=form.cleaned_data['username'],
                first_name=form.cleaned_data['first_name'],
                last_name=form.cleaned_data['last_name'],
                email=form.cleaned_data['email'],
                is_active=False
            )
            user.password = hashed_password  # Directly set hashed password
            user.save()

            # Create UserProfile
            UserProfile.objects.create(
                user=user,
                phone=form.cleaned_data.get('phone', ''),
                is_email_verified=False
            )

            # Save to approval table with hashed password
            UserApprovalRequest.objects.create(
                username=form.cleaned_data['username'],
                first_name=form.cleaned_data['first_name'],
                last_name=form.cleaned_data['last_name'],
                email=form.cleaned_data['email'],
                phone=form.cleaned_data.get('phone', ''),
                password=hashed_password,
                status='pending'
            )

            # Store user_id and email in session for OTP verification
            request.session['signup_user_id'] = user.id
            request.session['signup_email'] = user.email

            # Create and send OTP
            OTPVerification.objects.filter(user=user, email=user.email, is_used=False).update(is_used=True)
            otp = OTPVerification.objects.create(
                user=user,
                email=user.email,
                purpose='signup'
            )

            if otp.send_otp():
                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': 'OTP sent to your email. Please verify to complete signup.',
                        'redirect_url': reverse('verify_otp')
                    })
                else:
                    messages.success(request, 'OTP sent to your email. Please verify to complete signup.')
                    return redirect('verify_otp')
            else:
                user.delete()  # Rollback if OTP fails
                if is_ajax:
                    return JsonResponse({
                        'success': False,
                        'message': 'Failed to send OTP. Please try again.'
                    })
                else:
                    messages.error(request, 'Failed to send OTP. Please try again.')
        else:
            if is_ajax:
                errors = {field: [str(err) for err in errors] for field, errors in form.errors.items()}
                return JsonResponse({
                    'success': False,
                    'message': 'Please correct the errors below.',
                    'errors': errors
                })
    else:
        form = SignupForm()
    
    response = render(request, 'auth/signup.html', {'form': form})
    return add_no_cache_headers(response)

@never_cache
def login_view(request):
    if request.user.is_authenticated:
        return redirect('home')
    
    if request.method == 'POST':
        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        form = LoginForm(request.POST)
        
        if form.is_valid():
            email = form.cleaned_data['email']
            password = request.POST.get('password')  # Raw password from form
            
            try:
                req = UserApprovalRequest.objects.get(email=email, status='approved')
                
                user = User.objects.filter(email=email).first()
                if not user:
                    user = User(
                        username=req.username,
                        first_name=req.first_name,
                        last_name=req.last_name,
                        email=req.email,
                        is_active=True
                    )
                    user.password = req.password  # Directly set hashed password
                    user.save()
                    
                    UserProfile.objects.get_or_create(
                        user=user,
                        defaults={'phone': req.phone, 'is_email_verified': True}
                    )
                else:
                    if user.password != req.password:
                        user.password = req.password  # Sync hashed password if different
                        user.save()

                # Authenticate with raw password
                user = authenticate(request, username=user.username, password=password)
                
                if user is not None:
                    user.backend = 'django.contrib.auth.backends.ModelBackend'
                    login(request, user)
                    
                    if is_ajax:
                        return JsonResponse({
                            'success': True,
                            'message': 'Login successful!',
                            'redirect_url': reverse('home')
                        })
                    else:
                        messages.success(request, 'Login successful!')
                        response = redirect('home')
                        return add_no_cache_headers(response)
                else:
                    raise ValueError('Invalid password.')
            except ValueError as ve:
                if is_ajax:
                    return JsonResponse({
                        'success': False,
                        'message': str(ve)
                    })
                else:
                    messages.error(request, str(ve))
            except UserApprovalRequest.DoesNotExist:
                if is_ajax:
                    return JsonResponse({
                        'success': False,
                        'message': 'No approved account found with this email.'
                    })
                else:
                    messages.error(request, 'No approved account found with this email.')
            except Exception as e:
                if is_ajax:
                    return JsonResponse({
                        'success': False,
                        'message': f'Login error: {str(e)}'
                    })
                else:
                    messages.error(request, f'Login error: {str(e)}')
        else:
            if is_ajax:
                errors = {field: [str(err) for err in errors] for field, errors in form.errors.items()}
                return JsonResponse({
                    'success': False,
                    'message': 'Please correct the errors below.',
                    'errors': errors
                })
    else:
        form = LoginForm()
    
    response = render(request, 'auth/login.html', {'form': form})
    return add_no_cache_headers(response)

@never_cache
def verify_otp_view(request):
    if request.user.is_authenticated:
        return redirect('home')
    
    user_id = request.session.get('signup_user_id')
    email = request.session.get('signup_email')
    
    if not user_id or not email:
        messages.error(request, 'Invalid session. Please try again.')
        return redirect('login')
    
    if request.method == 'POST':
        if request.content_type == 'application/json':
            data = json.loads(request.body)
            form = OTPVerificationForm(data)
        else:
            form = OTPVerificationForm(request.POST)
        
        if form.is_valid():
            otp_code = form.cleaned_data['otp']
            
            try:
                otp = OTPVerification.objects.get(
                    user_id=user_id,
                    email=email,
                    otp=otp_code,
                    is_used=False
                )
                
                if otp.is_valid():
                    otp.is_used = True
                    otp.save()
                    
                    user = User.objects.get(id=user_id)
                    user.is_active = True
                    user.save()
                    
                    profile, created = UserProfile.objects.get_or_create(user=user)
                    profile.is_email_verified = True
                    profile.save()
                    
                    request.session.pop('signup_user_id', None)
                    request.session.pop('signup_email', None)
                    
                    if request.content_type == 'application/json':
                        return JsonResponse({
                            'success': True,
                            'message': 'Email verified successfully! Please log in.',
                            'redirect_url': reverse('login')
                        })
                    else:
                        messages.success(request, 'Email verified successfully! Please log in.')
                        response = redirect('login')
                        return add_no_cache_headers(response)
                else:
                    if request.content_type == 'application/json':
                        return JsonResponse({
                            'success': False,
                            'message': 'OTP has expired. Please request a new one.'
                        })
                    else:
                        messages.error(request, 'OTP has expired. Please request a new one.')
            
            except OTPVerification.DoesNotExist:
                if request.content_type == 'application/json':
                    return JsonResponse({
                        'success': False,
                        'message': 'Invalid OTP. Please try again.'
                    })
                else:
                    messages.error(request, 'Invalid OTP. Please try again.')
        else:
            if request.content_type == 'application/json':
                return JsonResponse({
                    'success': False,
                    'message': 'Please enter a valid 6-digit OTP.',
                    'errors': form.errors
                })
    else:
        form = OTPVerificationForm()
    
    response = render(request, 'auth/verify_otp.html', {
        'form': form,
        'email': email
    })
    return add_no_cache_headers(response)

@csrf_exempt
def resend_otp_view(request):
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Invalid request method.'})
    
    user_id = request.session.get('signup_user_id')
    email = request.session.get('signup_email')
    
    if not user_id or not email:
        return JsonResponse({'success': False, 'message': 'Invalid session.'})
    
    try:
        user = User.objects.get(id=user_id)
        
        OTPVerification.objects.filter(
            user=user,
            email=email,
            is_used=False
        ).update(is_used=True)
        
        otp = OTPVerification.objects.create(
            user=user,
            email=email,
            purpose='signup'
        )
        
        if otp.send_otp():
            return JsonResponse({
                'success': True,
                'message': 'New OTP sent to your email.'
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'Failed to send OTP. Please try again.'
            })
    
    except User.DoesNotExist:
        return JsonResponse({'success': False, 'message': 'User not found.'})

def check_auth_status(request):
    return JsonResponse({
        'authenticated': request.user.is_authenticated
    })