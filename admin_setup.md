## Admin Setup Instructions (Initial Bootstrapping)

Before general users sign up, follow these steps to configure the admin interface, roles, and special accounts.

---

### 1. Create a superuser (platform owner)

This user will access the Django admin interface and set up initial roles and permissions.

```bash
python manage.py migrate
python manage.py createsuperuser
```

Then run the server:

```bash
python manage.py runserver
```

Visit [http://127.0.0.1:8000/admin/](http://127.0.0.1:8000/admin/) and log in with your superuser credentials.

---

### 2. Create role groups (admin / steward / user)

Once logged into the admin panel:

1. Go to **Authentication and Authorization → Groups → Add Group**
2. Create the following groups:
   - `admin`
   - `steward`
   - `user`
3. Optionally, assign model or custom permissions to each group  
   (You can leave them empty since our system bypasses Django's ORM)

---

### 3. Create special users (admin/steward accounts)

Still in the admin panel:
1. First, add yourself to the `admin` group.
2. Then, Navigate to **Users → Add**
3. For each special user:
   - Set a **username**, and **password**
   - On the permission section, set:
     - `is_staff = True` for admin and steward users  
     - `is_superuser = True` for admin users to give them unrestricted access  
     - Add them to the appropriate group (`admin`, `steward`, etc.)

Example assignments:

| Username      | Staff | Superuser | Group     |
|---------------|-------|-----------|-----------|
| alice_admin   |  Y    |  Y        | `admin`   |
| sam_steward   |  Y    |  N        | `steward` |
| uma_user      |  N    |  N        | `user`    |

---

### 4. Public user signups

After the above steps, general users can sign up using the public registration flow provided by Cymphony API/GUI.

To auto-assign new signups to the `user` group, a signal handler is already wired up in the project (see `accounts/signals.py`), so no additional configuration is needed.

---

### Access Summary

| Role (Group)         | `is_staff` | `is_superuser` | Can access `/admin/` | Can create users?     |
|--------------|------------|----------------|-----------------------|------------------------|
| **Admin (Superuser)** |  Y         |  Y             |  Y                    |  Y                     |
| **Steward**   |  Y         |  N             |  Y                    |  N                     |
| **User**      |  N         |  N             |  N                    |  N                     |

Only users with `is_staff=True` can log into Django Admin.

> 📝 **Note:** Only superusers like those in the `admin` group, can create new users in the Django admin interface.  
> Even staff users (like those in the `steward` group) cannot create users, regardless of their permissions.  
> This is by design in Django to prevent privilege escalation.

---

## Programmatic Setup (Recommended for Automation)

For automated, reproducible, and version-controlled setup of groups and users, use the `manage_roles_and_users` Django management command.

### Create Roles and Special Users

Run the command from your project root using `python manage.py manage_roles_and_users <options>`.

#### 1. Create Role Groups

Create the necessary role groups (`admin`, `steward`, `user`). This step is idempotent – if a group already exists, it won't be re-created.

```bash
python manage.py manage_roles_and_users --create_group "admin"
python manage.py manage_roles_and_users --create_group "steward"
python manage.py manage_roles_and_users --create_group "user"
```

#### 2. Create Special Users and Assign to Groups

Create users and assign them exclusively to their respective groups. The command automatically sets `is_staff` and `is_superuser` permissions based on the assigned group (`admin` users get both `is_staff=True` and `is_superuser=True`; `steward` users get `is_staff=True` and `is_superuser=False`; `user`s get neither).

**Note**: Creating a user via this command will initially set `is_staff` and `is_superuser` to `False`. These flags are then updated *when the user is assigned to a group*.

```bash
# Create an admin user 'alice_admin' and assign to the 'admin' group
python manage.py manage_roles_and_users --create_user "alice_admin" "securepassword123"
python manage.py manage_roles_and_users --assign_user_to_group "alice_admin" "admin"

# Create a steward user 'sam_steward' and assign to the 'steward' group
python manage.py manage_roles_and_users --create_user "sam_steward" "stewardpassword"
python manage.py manage_roles_and_users --assign_user_to_group "sam_steward" "steward"

# Create a regular user 'uma_user' and assign to the 'user' group
# (Note: Regular users can also sign up via the frontend or signup API, in which case they are automatically assigned to the 'user' group.) - See 'public user signups' below
python manage.py manage_roles_and_users --create_user "uma_user" "userpassword"
python manage.py manage_roles_and_users --assign_user_to_group "uma_user" "user"

# Re-assign an existing user to a different group (e.g., 'john.doe' to 'steward')
# This will remove 'john.doe' from any other groups and set 'is_staff' and 'is_superuser' accordingly.
python manage.py manage_roles_and_users --assign_user_to_group "john.doe" "steward"
```

### Public User Signups

After the above steps, general users can sign up using the public registration flow provided by Cymphony API/GUI.

To auto-assign new signups to the `user` group, a signal handler is already wired up in the project (see `accounts/signals.py`), so no additional configuration is needed.

---