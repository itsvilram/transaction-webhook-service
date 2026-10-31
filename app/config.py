"""Settings from environment variables."""
import os

DATABASE_URL = os.environ["DATABASE_URL"]
PROCESSING_DELAY_SECONDS = float(os.getenv("PROCESSING_DELAY_SECONDS", "30"))
SWEEP_INTERVAL_SECONDS = float(os.getenv("SWEEP_INTERVAL_SECONDS", "60"))
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "120"))
DB_TIMEOUT_SECONDS = float(os.getenv("DB_TIMEOUT_SECONDS", "2"))
