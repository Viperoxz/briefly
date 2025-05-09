
class Settings:
    # Model settings
    EMBEDDING_MODEL_ID = 'dangvantuan/vietnamese-document-embedding'
    EMBEDDING_MODEL_MAX_INPUT_LENGTH = 2048  # Reduced from 8096 for safety
    EMBEDDING_VECTOR_SIZE = 384  # Vector size for vietnamese-document-embedding

    # Chunking settings
    CHUNK_SIZE = 1500  # Characters
    CHUNK_OVERLAP_CHAR = 100  # Characters
    CHUNK_OVERLAP_TOKEN = 50  # Tokens

    # Batch settings
    BATCH_SIZE = 16  # Number of chunks per batch
    MAX_CONCURRENT_BATCHES = 2  # Number of parallel API calls

    # API rate limit settings
    REQUESTS_PER_SECOND = 10  
    SLEEP_TIME = 1.0 / REQUESTS_PER_SECOND  # Time to wait between requests

settings = Settings()