class Roles:
    ADMIN = "admin"
    MEMBER = "member"
    MANAGER = "manager"

    @classmethod
    def all_roles(cls):
        return [cls.ADMIN, cls.MEMBER, cls.MANAGER]
    
    @classmethod
    def admin_roles(cls):
        """Roles with admin privileges"""
        return [cls.ADMIN]
    
    @classmethod
    def manager_roles(cls):
        """Roles with manager+ privileges"""
        return [cls.ADMIN, cls.MANAGER]