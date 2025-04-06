from django.urls import path, include
from django.contrib.auth import views as auth_views
from . import views
from .forms import CustomRegistrationForm

from django_registration.backends.activation.views import RegistrationView
from django_registration.backends.one_step.views import RegistrationView as OneStepRegistrationView

app_name = 'account'
urlpatterns = [
    # post views
    path('login/', auth_views.LoginView.as_view(), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    path('', views.dashboard, name='dashboard'),
    path('register/',
            RegistrationView.as_view(
                form_class=CustomRegistrationForm
            ),
            name='django_registration_register',
        ),
    path('one_step_register/',  # automatically logs in the user as well
            OneStepRegistrationView.as_view(
                form_class=CustomRegistrationForm, success_url='/'
            ),
            name='django_registration_one_step_register',
        ),
    # reset password urls
    path('password_reset/',
         auth_views.PasswordResetView.as_view(),
         name='password_reset'),
    path('password_reset/done/',
         auth_views.PasswordResetDoneView.as_view(),
         name='password_reset_done'),
    path('reset/<uidb64>/<token>/',
         auth_views.PasswordResetConfirmView.as_view(),
         name='password_reset_confirm'),
    path('reset/done/',
         auth_views.PasswordResetCompleteView.as_view(),
         name='password_reset_complete'),

]