from django import forms
from django.contrib.auth.models import User
from django_registration.forms import RegistrationForm, RegistrationFormCaseInsensitive, RegistrationFormUniqueEmail

# the getlazy is good for translation capability
from django.utils.translation import gettext_lazy as _


# adds the extra __init__ method of RegistrationForm and modifies the RegistrationForm.Meta to take the specific fields instead.
# More details: https://docs.djangoproject.com/en/3.1/topics/forms/modelforms/#form-inheritance
# Validations:
# 1. first name and last name validated by the auth.Model (not the form but the model itself)
# 2. password1 and password2 validated by the UserCreationForm (parent of RegistrationForm class)
# 3. username and email validators added and conducted by RegistrationForm class
class CustomRegistrationForm(RegistrationFormCaseInsensitive, RegistrationFormUniqueEmail):
    class Meta(RegistrationForm.Meta):
        model = User
        fields = ('first_name', 'last_name', 'email', 'username', 'password1', 'password2')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
