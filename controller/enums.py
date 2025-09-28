from enum import Enum

class UserType(Enum):
    """Enum for different user types in the system"""
    STEWARD = "steward"
    REGULAR = "user"
    
    @classmethod
    def from_user_id(cls, user_id: int) -> 'UserType':
        """
        Determine user type from user ID.
        Returns STEWARD if user is in steward group, otherwise REGULAR.
        """
        from django.db import connection
        cursor = connection.cursor()
        
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 
                FROM auth_user_groups 
                INNER JOIN auth_group ON auth_user_groups.group_id = auth_group.id 
                WHERE auth_user_groups.user_id = %s 
                AND auth_group.name = 'steward'
            )
            """,
            [user_id]
        )
        is_steward = cursor.fetchone()[0]
        cursor.close()
        
        return cls.STEWARD if is_steward else cls.REGULAR
