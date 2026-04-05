class Topics:
    USER_EVENTS = 'user-events'
    AUTH_EVENTS = 'auth-events'
    
class UserEvents:
    USER_CREATE_REQUESTED = 'user-create-requested'
    USER_CREATED = 'user-created'
    USER_CREATE_FAILED = 'user-create-failed'
    USER_UPDATE_REQUESTED = 'user-update-requested'
    USER_UPDATED = 'user-updated'
    USER_UPDATED_FAILED = 'user-updated-failed'


class AuthEvents:
    CREDENTIAL_CREATE_REQUESTED = 'credential-create-requested'
    CREDENTIAL_CREATED = 'credential-created'
    CREDENTIAL_CREATE_FAILED = 'credential-create-failed'
