import http.client
import pandas as pd
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import APIFOOTBALL

data_formatada = '2023-12-31'

DATABASE_URL = "sqlite:///football.db"

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
        # Obter ids existentes no banco de dados
        Session = sessionmaker(bind=engine)
        session = Session()
        existing_ids = session.query(FootballTable.id_fixtures).all()
        existing_ids = {id_fixtures for (id_fixtures,) in existing_ids}
        
        # Filtrar apenas os novos registros
        new_records = df[~df['id_fixtures'].isin(existing_ids)]
        
        if not new_records.empty:
            # Salvar os novos dados no banco de dados
            new_records.to_sql('games', con=engine, if_exists='append', index=False)
            print("Dados salvos no banco de dados SQLite.")
        else:
            print("Nenhum dado novo para salvar no banco de dados.")
        
        session.close()
    else:
        print("Nenhum dado para salvar no banco de dados.")

salve_database()
engine.dispose()
