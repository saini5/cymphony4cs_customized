"""
Usage Examples:

# Create a 'admin' group
python manage.py manage_roles_and_users --create_group "admin"
# Create a 'steward' group
python manage.py manage_roles_and_users --create_group "steward"
# Create a 'user' group
python manage.py manage_roles_and_users --create_group "user"

# Create an admin user 'alice_admin' (permissions handled by group assignment)
python manage.py manage_roles_and_users --create_user "alice_admin" "mysecretpass"
# Then assign to group to set permissions
python manage.py manage_roles_and_users --assign_user_to_group "alice_admin" "admin"

# Create a steward user 'sam_steward'
python manage.py manage_roles_and_users --create_user "sam_steward" "stewardpass"
# Then assign to group to set permissions
python manage.py manage_roles_and_users --assign_user_to_group "sam_steward" "steward"

# No need of the below since regular users can sign up by themselves and will be assigned to the 'user' group automatically.
# Create a regular user 'uma_user'
python manage.py manage_roles_and_users --create_user "uma_user" "userpass"
# Then assign to group (no special permissions set)
python manage.py manage_roles_and_users --assign_user_to_group "uma_user" "user"

# Assign an existing user 'john.doe' to the 'steward' group (will update permissions)
python manage.py manage_roles_and_users --assign_user_to_group "john.doe" "steward"
"""
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import User, Group, Permission
from django.contrib.contenttypes.models import ContentType

class Command(BaseCommand):
    help = 'Manages users and roles programmatically (e.g., create groups, assign users to groups, assign permissions).'

    def add_arguments(self, parser):
        parser.add_argument('--create_group', type=str, help='Name of the group to create.')
        parser.add_argument('--create_user', nargs=2, metavar=('username', 'password'),
                            help='Create a new user. Takes username and password.')
        parser.add_argument('--assign_user_to_group', nargs=2, metavar=('username', 'group_name'),
                            help='Assign a user exclusively to a specified group. Takes username and group_name.')
        # parser.add_argument('--is_staff', action='store_true', help='Set the user as staff.')
        # parser.add_argument('--is_superuser', action='store_true', help='Set the user as superuser.')

    def handle(self, *args, **options):
        if options['create_group']:
            group_name = options['create_group']
            group, created = Group.objects.get_or_create(name=group_name)
            if created:
                self.stdout.write(self.style.SUCCESS(f'Successfully created group "{group_name}"'))
            else:
                self.stdout.write(self.style.WARNING(f'Group "{group_name}" already exists'))

        if options['create_user']:
            username, password = options['create_user']
            # is_staff and is_superuser are now managed by group assignment
            try:
                User.objects.get(username=username)
                raise CommandError(f'User "{username}" already exists.')
            except User.DoesNotExist:
                user = User.objects.create_user(
                    username=username,
                    password=password,
                    is_staff=False,       # Default to False; updated by group assignment
                    is_superuser=False    # Default to False; updated by group assignment
                )
                self.stdout.write(self.style.SUCCESS(f'Successfully created user "{username}"'))

        if options['assign_user_to_group']:
            username, group_name = options['assign_user_to_group']
            try:
                user = User.objects.get(username=username)
                group = Group.objects.get(name=group_name)
                
                user.groups.clear() # Remove user from all existing groups
                user.groups.add(group)
                self.stdout.write(self.style.SUCCESS(f'Successfully assigned user "{username}" exclusively to group "{group_name}"'))

                # Apply implicit permissions based on group
                if group_name == 'admin':
                    user.is_staff = True
                    user.is_superuser = True
                    user.save()
                    self.stdout.write(self.style.SUCCESS(f'Updated user "{username}": is_staff=True, is_superuser=True'))
                elif group_name == 'steward':
                    user.is_staff = True
                    user.is_superuser = False # Ensure superuser is not set by steward group
                    user.save()
                    self.stdout.write(self.style.SUCCESS(f'Updated user "{username}": is_staff=True'))
                else:
                    # For other groups, ensure staff/superuser are false if not explicitly set elsewhere
                    # This protects against inadvertent permissions if user was previously in another role
                    if user.is_staff or user.is_superuser:
                        user.is_staff = False
                        user.is_superuser = False
                        user.save()
                        self.stdout.write(self.style.SUCCESS(f'Updated user "{username}": is_staff=False, is_superuser=False'))
                        
            except User.DoesNotExist:
                raise CommandError(f'User "{username}" does not exist.')
            except Group.DoesNotExist:
                raise CommandError(f'Group "{group_name}" does not exist.')
        