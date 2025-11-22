from django import forms

class ScraperForm(forms.Form):
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

    main_category = forms.ChoiceField(choices=MAIN_CATEGORIES)
    subcategory = forms.ChoiceField(choices=SUBCATEGORY_CHOICES['default'], required=False)
    location = forms.CharField(max_length=100, initial="Chennai Tamil Nadu")
    near_me = forms.BooleanField(required=False)
    max_results = forms.IntegerField(initial=25, min_value=1, max_value=100)
    custom_term = forms.CharField(max_length=100, required=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if 'main_category' in self.data:
            main_category = self.data.get('main_category')
            self.fields['subcategory'].choices = ScraperForm.SUBCATEGORY_CHOICES.get(main_category, ScraperForm.SUBCATEGORY_CHOICES['default'])
        elif self.initial.get('main_category'):
            main_category = self.initial.get('main_category')
            self.fields['subcategory'].choices = ScraperForm.SUBCATEGORY_CHOICES.get(main_category, ScraperForm.SUBCATEGORY_CHOICES['default'])

        # Apply form-control class via widget attributes
        for field_name, field in self.fields.items():
            field.widget.attrs.update({'class': 'form-control'})

from django.contrib.auth.models import User
from .models import UserApprovalRequest  # Import the model

class UserApprovalRequestForm(forms.ModelForm):
    class Meta:
        model = UserApprovalRequest
        fields = '__all__'  # Or specify specific fields like ['username', 'email', 'status', 'approved_by']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if 'approved_by' in self.fields:
            self.fields['approved_by'].queryset = User.objects.filter(is_staff=True)  # Limit to staff/admins
            self.fields['approved_by'].required = False  # Optional based on your model

# Example custom form if needed for admin or views (no undefined references)
class AdminApprovalForm(forms.Form):
    approved_by = forms.ModelChoiceField(
        queryset=User.objects.filter(is_staff=True),
        label="Approved by",
        required=False
    )
    status = forms.ChoiceField(
        choices=[('pending', 'Pending'), ('approved', 'Approved')],
        initial='pending'
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if hasattr(kwargs.get('initial', {}), 'get') and 'request' in kwargs['initial']:
            self.fields['approved_by'].initial = kwargs['initial']['request'].user