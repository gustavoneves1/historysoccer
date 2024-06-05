from sqlalchemy import create_engine
import pandas as pd
import time
from config import SENHABANCO

start = time.time()
DATABASE_URL = f"postgresql://postgres:{SENHABANCO}@localhost:5432/football"
engine = create_engine(DATABASE_URL)
nome_tabela = 'games'
consulta_sql = f"SELECT * FROM {nome_tabela}"
df = pd.read_sql(consulta_sql, con=engine)
engine.dispose()
end = time.time()
tempo_total = end - start

print(f"O código demorou {tempo_total:.2f} segundos.")