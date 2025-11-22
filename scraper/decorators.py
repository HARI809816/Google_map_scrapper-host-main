from django.shortcuts import redirect
from functools import wraps
from django.utils.cache import add_never_cache_headers

def login_required_loginuser(view_func):
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if 'login_user_id' not in request.session:
            return redirect('login')
        return view_func(request, *args, **kwargs)
    return wrapper

def never_cache_response(view_func):
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        response = view_func(request, *args, **kwargs)
        add_never_cache_headers(response)
        return response
    return wrapper