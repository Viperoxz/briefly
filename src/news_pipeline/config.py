
class Settings:
    # Model settings
    EMBEDDING_MODEL_ID = 'dangvantuan/vietnamese-embedding'
    EMBEDDING_MODEL_MAX_INPUT_LENGTH = 500
    EMBEDDING_VECTOR_SIZE = 768 

    # Chunking settings
    CHUNK_SIZE = 400  # Characters
    CHUNK_OVERLAP_CHAR = 50  # Characters
    CHUNK_OVERLAP_TOKEN = 25  # Tokens

    # Batch settings
    BATCH_SIZE = 8  # Number of chunks per batch
    MAX_CONCURRENT_BATCHES = 2  # Number of parallel API calls

    # API rate limit settings
    REQUESTS_PER_SECOND = 10  
    SLEEP_TIME = 1.0 / REQUESTS_PER_SECOND  # Time to wait between requests

settings = Settings()