import os
import psycopg2
from dotenv import load_dotenv
# Required imports for SQLAlchemy engine creation
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Load environment variables from a .env file
load_dotenv()


class MultiDBConnector:
    """
    Manages connections to multiple PostgreSQL databases, reading credentials
    from specific environment variables. Supports returning both the raw
    psycopg2 connection and the SQLAlchemy Engine required by Pandas.
    """

    def __init__(self):
        # Maps friendly database names to their environment variable prefixes
        self.config_map = {
            "web_shop": "WEB_SHOP_",  # Source/OLTP Database
            "analytics": "ANALYTICS_"  # Target/Metrics Database
        }

    def _get_credentials(self, db_alias):
        """
        Internal function to fetch environment credentials based on the DB alias.
        """
        prefix = self.config_map.get(db_alias.lower())
        if not prefix:
            raise ValueError(f"Invalid database alias: '{db_alias}'. Use 'web_shop' or 'analytics'.")

        # Construct and read environment variable names
        host = os.getenv(prefix + 'DB_HOST')
        db_name = os.getenv(prefix + 'DB_NAME')
        user = os.getenv(prefix + 'DB_USER')
        password = os.getenv(prefix + 'DB_PASSWORD')

        if not all([host, db_name, user, password]):
            raise ValueError(
                f"Incomplete credentials for '{db_alias}'. Check {prefix}* variables in your environment.")

        # Returns credentials
        return host, db_name, user, password

    def get_connection(self, db_alias):
        """
        Returns an active psycopg2 connection object. Useful for pure SQL operations.
        """
        try:
            host, db_name, user, password = self._get_credentials(db_alias)

            conn = psycopg2.connect(
                host=host,
                database=db_name,
                user=user,
                password=password
            )
            print(f"INFO: Psycopg2 connection successfully established with database '{db_name}'.")
            return conn

        except ValueError as ve:
            # Captures credential or alias errors
            print(f"CONFIGURATION ERROR: {ve}")
            return None
        except Exception as e:
            # Captures connection errors (network, authentication)
            print(f"CONNECTION ERROR to {db_alias}: {e}")
            return None

    def get_engine(self, db_alias):
        """
        Returns an SQLAlchemy Engine object, which is essential for pandas read/write operations
        (pd.read_sql and df.to_sql).
        """
        try:
            host, db_name, user, password = self._get_credentials(db_alias)

            # Use quote_plus to safely encode the password for the connection URL
            password_encoded = quote_plus(password)

            # Construct the full database URL
            # Note: Port is hardcoded to 5432, assuming standard PostgreSQL port
            db_url = f"postgresql+psycopg2://{user}:{password_encoded}@{host}:5432/{db_name}"

            # Create the engine, setting echo=False to suppress verbose SQL logging
            engine = create_engine(db_url, echo=False)

            print(f"INFO: SQLAlchemy Engine successfully created for database '{db_name}'.")
            return engine

        except ValueError as ve:
            print(f"CONFIGURATION ERROR: {ve}")
            return None
        except Exception as e:
            print(f"ENGINE CREATION ERROR to {db_alias}: {e}")
            return None