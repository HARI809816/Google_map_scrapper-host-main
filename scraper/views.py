from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from .forms import ScraperForm
from .models import DownloadHistory, ScrapeJob, UserProfile
from .gym_scraper import scrape_gym_type, GymScraper
from .business_scraper import scrape_business_type, BusinessScraper
from .electronic_scraper import scrape_electronic_shop, SimplifiedGoogleMapsElectronicShopScraper as ElectronicScraper
from .ebike_scraper import scrape_ebike, SimplifiedGoogleMapsEbikeShowroomScraper as EbikeScraper
from .college_scraper import scrape_college, SimplifiedGoogleMapsCollegeScraper as CollegeScraper
from .training_scraper import scrape_training_institute, SimplifiedGoogleMapsTrainingInstituteScraper as TrainingScraper
from .salon_scraper import scrape_salon, EnhancedGoogleMapsScraper as SalonScraper
from .boutique_scraper import scrape_boutique, SimplifiedGoogleMapsBoutiqueScraper as BoutiqueScraper
from .petrol_bunk_scraper import scrape_petrol_bunk, SimplifiedGoogleMapsPetrolBunkScraper as PetrolBunkScraper
from .general_scraper import scrape_general, SimplifiedGoogleMapsGeneralScraper as GeneralScraper
import os
import json
from django.conf import settings
from django.views.decorators.http import require_POST
from django.utils import timezone
from django.db import transaction
from django.views.decorators.cache import never_cache
import threading
from django.core.cache import cache
import time

# Add this import for process management
import signal
import psutil
import os

FITNESS_TYPES = ['crossfit', 'yoga', 'pilates', 'martial_arts', 'swimming', 'all_gyms']
BUSINESS_TYPES = ['startup', 'manufacturing', 'consultant', 'all_business']

# Global dictionary to track running scrapers (in production, use Redis)
SCRAPER_THREADS = {}

@never_cache
def home(request):
    # Clear any previous session data
    csv_files = request.session.pop('csv_files', [])
    message = request.session.pop('message', '')
    form_data = request.session.pop('form_data', {})
    results = request.session.pop('results', [])
    progress = request.session.pop('progress', 0)
    
    if request.method == 'POST':
        form = ScraperForm(json.loads(request.body))
        if form.is_valid():
            main_category = form.cleaned_data['main_category']
            subcategory = form.cleaned_data.get('subcategory')
            max_results = form.cleaned_data['max_results']
            location = form.cleaned_data['location']
            custom_term = form.cleaned_data.get('custom_term', '')
            job_id = form.cleaned_data.get('job_id') or f"scrape_{request.user.id if request.user.is_authenticated else 'guest'}_{int(timezone.now().timestamp())}"
            
            if form.cleaned_data['near_me']:
                location = 'near me'
            
            os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
            csv_files = []
            results = []
            scrape_job = None
            
            try:
                # Create scrape job if user is authenticated
                if request.user.is_authenticated:
                    with transaction.atomic():
                        scrape_job = ScrapeJob.objects.create(
                            user=request.user,
                            location=location,
                            main_category=main_category,
                            subcategory=subcategory or '',
                            custom_term=custom_term,
                            max_results=max_results,
                            status='running',
                            job_id=job_id
                        )
                
                # Perform scraping based on category - now with cancellation support
                results = perform_scraping_with_cancellation(
                    main_category, subcategory, location, max_results, custom_term, job_id
                )
                
                if results and results != "CANCELLED":
                    # Process successful results
                    if main_category in ['fitness', 'business', 'electronic_shop', 'ebike', 'college', 
                                       'training_institute', 'salon', 'boutique', 'custom']:
                        # Generate filename based on category
                        if main_category == 'custom':
                            filename = os.path.join(settings.MEDIA_ROOT, f"{custom_term.replace(' ', '_')}_{location.replace(' ', '_').replace(',', '').lower()}.csv")
                        else:
                            filename = os.path.join(settings.MEDIA_ROOT, f"{main_category}_{location.replace(' ', '_').replace(',', '').lower()}_{main_category}s.csv")
                        
                        # Save to appropriate scraper based on category
                        scraper = get_scraper_by_category(main_category)
                        if scraper:
                            scraper.save_simplified_csv(results, filename)
                            csv_files.append('/media/' + os.path.basename(filename))
                        
                        message = f"Scraped {len(results)} {main_category} facilities."
                    else:
                        message = f"No {main_category} facilities found."
                
                elif results == "CANCELLED":
                    message = "Scraping was cancelled by user."
                    results = []
                else:
                    message = f"No {main_category} facilities found."
                
                # Update scrape job and create download history for authenticated users
                if request.user.is_authenticated and scrape_job:
                    with transaction.atomic():
                        scrape_job.status = 'completed' if results else 'cancelled' if results == [] and message == "Scraping was cancelled by user." else 'failed'
                        scrape_job.total_found = len(results) if results != "CANCELLED" else 0
                        scrape_job.progress = 100 if results and results != "CANCELLED" else 0
                        scrape_job.updated_at = timezone.now()
                        if not results and results != "CANCELLED":
                            scrape_job.error_message = message
                        scrape_job.save()
                        
                        # Create download history entries for each CSV file
                        for csv_file in csv_files:
                            file_path = csv_file.replace('/media/', '')
                            full_file_path = os.path.join(settings.MEDIA_ROOT, file_path)
                            file_size = 0
                            try:
                                file_size = os.path.getsize(full_file_path)
                            except OSError:
                                pass
                            
                            DownloadHistory.objects.create(
                                user=request.user,
                                scrape_job=scrape_job,
                                file_name=os.path.basename(file_path),
                                file_path=csv_file,
                                file_size=file_size,
                                download_count=0
                            )
                
            except Exception as e:
                message = f"Error during scraping: {str(e)}"
                # Update scrape job with error if user is authenticated
                if request.user.is_authenticated and scrape_job:
                    scrape_job.status = 'failed'
                    scrape_job.error_message = str(e)
                    scrape_job.progress = 0
                    scrape_job.save()
            
            return JsonResponse({
                'success': bool(results) and results != "CANCELLED",
                'message': message,
                'results': results if results != "CANCELLED" else [],
                'csv_files': csv_files,
                'message_type': 'info' if results and results != "CANCELLED" else 'error',
                'is_processing': False,
                'progress': 100 if results and results != "CANCELLED" else 0
            })
        else:
            errors = dict(form.errors.items())
            return JsonResponse({
                'success': False,
                'message': f"Form invalid. Errors: {json.dumps(errors)}",
                'message_type': 'error',
                'is_processing': False,
                'progress': 0
            })
    else:
        form = ScraperForm()
    
    # Get user's profile if authenticated
    user_profile = None
    if request.user.is_authenticated:
        user_profile, created = UserProfile.objects.get_or_create(user=request.user)
    
    return render(request, 'index.html', {
        'form': form,
        'csv_files': csv_files,
        'message': message,
        'message_type': 'info' if message and csv_files else 'error',
        'results': results,
        'is_processing': request.session.get('is_processing', False),
        'progress': progress,
        'user_profile': user_profile
    })

def get_scraper_by_category(category):
    """Return appropriate scraper based on category"""
    scrapers = {
        'fitness': GymScraper(),
        'business': BusinessScraper(),
        'electronic_shop': ElectronicScraper(),
        'ebike': EbikeScraper(),
        'college': CollegeScraper(),
        'training_institute': TrainingScraper(),
        'salon': SalonScraper(),
        'boutique': BoutiqueScraper(),
        'custom': GeneralScraper(),
    }
    return scrapers.get(category)

def perform_scraping_with_cancellation(main_category, subcategory, location, max_results, custom_term, job_id):
    """
    Perform scraping with cancellation support
    """
    # Store cancellation flag in cache
    cache.set(f"cancel_scraping_{job_id}", False, timeout=3600)  # 1 hour timeout
    
    # Import the scraper functions
    from .gym_scraper import scrape_gym_type
    from .business_scraper import scrape_business_type
    from .electronic_scraper import scrape_electronic_shop
    from .ebike_scraper import scrape_ebike
    from .college_scraper import scrape_college
    from .training_scraper import scrape_training_institute
    from .salon_scraper import scrape_salon
    from .boutique_scraper import scrape_boutique
    from .general_scraper import scrape_general
    
    try:
        if main_category == 'fitness' and subcategory in FITNESS_TYPES:
            results = scrape_gym_type(subcategory, location, max_results)
        elif main_category == 'business' and subcategory in BUSINESS_TYPES:
            results = scrape_business_type(subcategory, location, max_results)
        elif main_category == 'electronic_shop':
            results = scrape_electronic_shop(location, max_results)
        elif main_category == 'ebike':
            results = scrape_ebike(location, max_results)
        elif main_category == 'college':
            results = scrape_college(location, max_results)
        elif main_category == 'training_institute':
            results = scrape_training_institute(location, max_results)
        elif main_category == 'salon':
            results = scrape_salon(location, max_results, job_id=job_id)
        elif main_category == 'boutique':
            results = scrape_boutique(location, max_results)
        elif main_category == 'custom':
            if not custom_term:
                return []
            results = scrape_general(location, custom_term, max_results)
        else:
            return []
        
        # Check if cancellation was requested
        if cache.get(f"cancel_scraping_{job_id}"):
            return "CANCELLED"
        
        return results
        
    except Exception as e:
        # Check if cancellation was requested
        if cache.get(f"cancel_scraping_{job_id}"):
            return "CANCELLED"
        raise e

@csrf_exempt
def update_custom_search(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        main_category = data.get('main_category')
        subcategory = data.get('subcategory')
        location = data.get('location')
        custom_term = data.get('custom_term', '')
        max_results = int(data.get('max_results', 25))
        job_id = data.get('job_id') or f"scrape_{request.user.id if request.user.is_authenticated else 'guest'}_{int(timezone.now().timestamp())}"
        near_me = data.get('near_me') == 'on'
        if near_me:
            location = 'near me'

        os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
        results = []
        csv_file = None
        scrape_job = None

        try:
            # Create scrape job if user is authenticated
            if request.user.is_authenticated:
                scrape_job = ScrapeJob.objects.create(
                    user=request.user,
                    location=location,
                    main_category=main_category,
                    custom_term=custom_term,
                    max_results=max_results,
                    status='running',
                    job_id=job_id
                )
            
            # Store cancellation flag in cache
            cache.set(f"cancel_scraping_{job_id}", False, timeout=3600)
            
            if main_category == 'custom' and custom_term:
                results = scrape_general(location, custom_term, max_results)
                if results and not cache.get(f"cancel_scraping_{job_id}"):
                    filename = os.path.join(settings.MEDIA_ROOT, f"{custom_term.replace(' ', '_')}_{location.replace(' ', '_').replace(',', '').lower()}.csv")
                    scraper = GeneralScraper()
                    scraper.save_simplified_csv(results, filename)
                    csv_file = f'/media/{os.path.basename(filename)}'
                    message = f"Scraped {len(results)} results for '{custom_term}'."
                elif cache.get(f"cancel_scraping_{job_id}"):
                    message = "Scraping was cancelled by user."
                    results = []
                    return JsonResponse({'success': False, 'message': message, 'is_processing': False})
                else:
                    message = f"No results found for '{custom_term}'."
            
            elif main_category == 'ebike':
                filename = os.path.join(settings.MEDIA_ROOT, f"ebike_showrooms_{location.replace(' ', '_').replace(',', '').lower()}_showrooms.csv")
                results = scrape_ebike(location, max_results)
                if results and not cache.get(f"cancel_scraping_{job_id}"):
                    csv_file = f'/media/{os.path.basename(filename)}'
                    message = f"Scraped {len(results)} e-bike showrooms."
                elif cache.get(f"cancel_scraping_{job_id}"):
                    message = "Scraping was cancelled by user."
                    results = []
                    return JsonResponse({'success': False, 'message': message, 'is_processing': False})
                else:
                    message = f"No e-bike showrooms found."
            else:
                if request.user.is_authenticated and scrape_job:
                    scrape_job.delete()
                return JsonResponse({'success': False, 'message': 'Invalid main category or missing custom term.', 'is_processing': False})
            
            # Update scrape job and create download history if user is authenticated
            if request.user.is_authenticated and scrape_job:
                scrape_job.status = 'completed' if results else 'failed'
                scrape_job.total_found = len(results)
                scrape_job.progress = 100
                if not results:
                    scrape_job.error_message = message
                scrape_job.save()
                
                # Create download history
                if csv_file:
                    file_path = csv_file.replace('/media/', '')
                    full_file_path = os.path.join(settings.MEDIA_ROOT, file_path)
                    file_size = 0
                    try:
                        file_size = os.path.getsize(full_file_path)
                    except OSError:
                        pass
                    
                    DownloadHistory.objects.create(
                        user=request.user,
                        scrape_job=scrape_job,
                        file_name=os.path.basename(file_path),
                        file_path=csv_file,
                        file_size=file_size,
                        download_count=0
                    )
        
        except Exception as e:
            if request.user.is_authenticated and scrape_job:
                scrape_job.status = 'failed'
                scrape_job.error_message = str(e)
                scrape_job.save()
            return JsonResponse({'success': False, 'message': f"Error during scraping: {str(e)}", 'is_processing': False})

        return JsonResponse({
            'success': bool(results),
            'message': message,
            'results': results,
            'csv_file': csv_file,
            'is_processing': False
        })
    
    return JsonResponse({'success': False, 'message': 'Invalid request.', 'is_processing': False})

@require_POST
def update_subcategory_options(request):
    main_category = request.POST.get('main_category')
    subcategory_choices = ScraperForm.SUBCATEGORY_CHOICES.get(main_category, ScraperForm.SUBCATEGORY_CHOICES['default'])
    return JsonResponse({'choices': subcategory_choices})

@csrf_exempt
def cancel_scraping(request):
    """Cancel scraping process and close only the Chrome instance for this job"""
    if request.method == 'POST':
        data = json.loads(request.body)
        job_id = data.get('job_id')
        
        if job_id:
            # Set cancellation flag
            cache.set(f"cancel_scraping_{job_id}", True, timeout=3600)
            
            # Try to close the specific scraper instance
            from .salon_scraper import close_scraper_by_job_id
            success = close_scraper_by_job_id(job_id)
            
            if success:
                message = 'Scraping cancelled and Chrome closed for this job'
            else:
                # Fallback: try to kill any recent Chrome processes
                import psutil
                import time
                
                killed_pids = []
                current_time = time.time()
                
                for proc in psutil.process_iter(['pid', 'name', 'create_time', 'cmdline']):
                    try:
                        if 'chrome' in proc.info['name'].lower():
                            proc_start_time = proc.info['create_time']
                            # Kill Chrome processes that started in the last 60 seconds
                            if current_time - proc_start_time < 60:
                                proc.kill()
                                killed_pids.append(proc.info['pid'])
                                print(f"Killed Chrome process (PID: {proc.info['pid']}) by time proximity")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                
                if killed_pids:
                    message = f'Scraping cancelled and Chrome closed (killed PIDs: {killed_pids})'
                else:
                    message = 'Scraping cancelled (no Chrome processes found to kill)'
            
            return JsonResponse({'success': True, 'message': message})
    
    return JsonResponse({'success': False, 'message': 'Invalid request'})