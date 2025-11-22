# scraper/middleware.py

class NoCacheMiddleware:
    """
    Middleware to prevent caching of authenticated pages and auth-related pages.
    This fixes the back button issue after login/logout.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        
        # Define paths that should never be cached
        no_cache_paths = [
            '/login/',
            '/signup/',
            '/verify-otp/',
            '/logout/',
            '/profile/',
            '/downloads/',
            '/admin/',
        ]
        
        # Check if current path should not be cached
        should_not_cache = (
            request.user.is_authenticated or 
            any(request.path.startswith(path) for path in no_cache_paths)
        )
        
        if should_not_cache:
            response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0, private'
            response['Pragma'] = 'no-cache'
            response['Expires'] = '0'
        
        return response