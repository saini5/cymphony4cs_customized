

def dict_fetchall(cursor):
    """Return all rows from a cursor as a dict"""
    """
    By default, the Python DB API will return results without their field names, 
        which means you end up with a list of values, rather than a dict.
    At a small performance and memory cost, you can return results as a dict by using something like this:
    Source : https://docs.djangoproject.com/en/3.1/topics/db/sql/
    """
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def dict_fetchone(cursor):
    """Return a row from a cursor as a dict"""
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, cursor.fetchone()))
