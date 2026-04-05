import pandas as pd
import time
import logging
from sqlalchemy import create_engine

# Variables asumidas como ya definidas en el entorno de Mage
DB_URL = "postgresql://admin:admin123@database:5432/megaline"
TABLE_NAME = "megaline_users"   # cambiar por la tabla que quieras cargar
CHUNK_SIZE = 10_000
MAX_RETRIES = 3
RETRY_DELAY = 5

logger = logging.getLogger(__name__)


def load_table_with_retries(db_url, table, chunk_size=CHUNK_SIZE, max_retries=MAX_RETRIES):
    """Data loader genérico con reintentos y chunking."""
    engine = create_engine(db_url)
    attempt = 0

    while attempt < max_retries:
        try:
            logger.info(f"Cargando '{table}' — intento {attempt + 1}")
            chunks = []
            offset = 0

            while True:
                query = f"SELECT * FROM {table} LIMIT {chunk_size} OFFSET {offset}"
                chunk = pd.read_sql(query, engine)
                if chunk.empty:
                    break
                chunks.append(chunk)
                logger.info(f"  chunk offset={offset} → {len(chunk)} filas")
                offset += chunk_size

            df = pd.concat(chunks, ignore_index=True)
            logger.info(f"'{table}' cargada: {len(df)} filas totales.")
            return df

        except Exception as e:
            attempt += 1
            logger.warning(f"Error intento {attempt}: {e}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
            else:
                raise


@data_loader
def load_data(*args, **kwargs):
    return load_table_with_retries(db_url=DB_URL, table=TABLE_NAME)


@test
def test_output(output, *args):
    assert output is not None, "El dataframe no debe ser None"
    assert len(output) > 0, "El dataframe no debe estar vacío"