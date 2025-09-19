from django.conf import settings
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model


User = get_user_model()
@receiver(post_save, sender=User)
def assign_group_on_signup(sender, instance, created, **kwargs):
    if created:
        try:
            group = Group.objects.get(name='user')
            instance.groups.add(group)
        except Group.DoesNotExist:
            pass