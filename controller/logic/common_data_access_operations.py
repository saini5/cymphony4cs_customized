from django.contrib.auth import get_user_model

User = get_user_model()

def is_steward(user_id: int):
    """Check if the user is a steward"""
    user = User.objects.get(id=user_id)
    print(f"User id {user_id} got mapped to user {user} with groups {user.groups.all()}")
    return user.groups.filter(name='steward').exists()


def dict_fetchall(cursor):
    """Return all rows from a cursor as a dict"""
    """
    By default, the Python DB API will return results without their field names, 
        which means you end up with a list of values, rather than a dict.
    At a small performance and memory cost, you can return results as a dict by using something like this:
    Source : https://docs.djangoproject.com/en/3.1/topics/db/sql/
    """
    rows = cursor.fetchall()
    if rows is None:
        return []
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in rows
    ]


def dict_fetchone(cursor):
    """Return a row from a cursor as a dict"""
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row))
