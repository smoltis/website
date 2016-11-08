import django.forms as forms
#from django.core.exceptions import ValidationError

class SearchForm(forms.Form):
    input_str = forms.CharField(max_length=20, label = 'Search part:')