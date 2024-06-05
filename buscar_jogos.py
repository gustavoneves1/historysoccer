import http.client
import pandas as pd
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from config import APIFOOTBALL

# Data atual e dia anterior
# data_atual = datetime.now()
# dia_anterior = data_atual - timedelta(days=1)
# data_formatada = dia_anterior.strftime("%Y-%m-%d")


data_formatada = '2024-01-01'
# URL de conexão SQLite
DATABASE_URL = "sqlite:///football.db"

# Configurar o SQLAlchemy
engine = create_engine(DATABASE_URL, echo=True)
Base = declarative_base()

class FootballTable(Base):
    __tablename__ = 'games'

    id_fixtures = Column(String, primary_key=True, index=True)
    date = Column(String)
    referee = Column(String)
    status = Column(String)
    league = Column(String)
    home_team_id = Column(String)
    home_team = Column(String)
    away_team_id = Column(String)
    away_team = Column(String)
    home_goals = Column(Integer)
    away_goals = Column(Integer)

# Criar a tabela no banco de dados
Base.metadata.create_all(engine)

def futebol_jogos():
    conn = http.client.HTTPSConnection("api-football-v1.p.rapidapi.com")

    headers = {
        'X-RapidAPI-Key': APIFOOTBALL,
        'X-RapidAPI-Host': "api-football-v1.p.rapidapi.com"
    }

    conn.request("GET", f"/v3/fixtures?date={data_formatada}", headers=headers)

    res = conn.getresponse()
    data = res.read()

    json_data = json.loads(data.decode("utf-8"))

    if 'response' in json_data:
        fixtures_data = json_data['response']
        
        extracted_data = []
        for fixture in fixtures_data:
            fixture_info = {
                'id_fixtures': fixture.get('fixture', {}).get('id'),
                'date': fixture.get('fixture', {}).get('date'),
                'referee': fixture.get('fixture', {}).get('referee'),
                'status': fixture.get('fixture', {}).get('status', {}).get('long'),
                'league': fixture.get('league', {}).get('name'),
                'home_team_id': fixture.get('teams', {}).get('home', {}).get('id'),
                'home_team': fixture.get('teams', {}).get('home', {}).get('name'),
                'away_team_id': fixture.get('teams', {}).get('away', {}).get('id'),
                'away_team': fixture.get('teams', {}).get('away', {}).get('name'),
                'home_goals': fixture.get('goals', {}).get('home'),
                'away_goals': fixture.get('goals', {}).get('away'),
            }
            extracted_data.append(fixture_info)

        df = pd.DataFrame(extracted_data)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%d/%m/%Y')
        df['home_goals'] = df['home_goals'].fillna(0)
        df['away_goals'] = df['away_goals'].fillna(0)
        df['status'] = 'Match Finished'
        return df

    else:
        print("Chave 'response' não encontrada nos dados JSON.")
        return pd.DataFrame()

def salve_database():
    df = futebol_jogos()
    if not df.empty:
        # Salvar os dados no banco de dados
        df.to_sql('games', con=engine, if_exists='append', index=False)
        print("Dados salvos no banco de dados SQLite.")
    else:
        print("Nenhum dado para salvar no banco de dados.")


salve_database()


engine.dispose()
