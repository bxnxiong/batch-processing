# Superset Configuration


# Redis for caching and async queries
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': 'redis://redis:6379/0',
}


# Set the feature flag for enabling SQLLAB
FEATURE_FLAGS = {
    'SQLLAB_BACKEND_PERSISTENCE': True,
}

# Enabling CORS
ENABLE_CORS = True
CORS_OPTIONS = {
    "origins": ["*"],  # Allow all origins
}

# Other configurations
SECRET_KEY = 'bxnxiong'