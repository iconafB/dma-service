
from sqlmodel import Session,SQLModel,create_engine
from settings.settings import get_settings
#define the database string
DATABASE_URL=f"postgresql+psycopg2://{get_settings().database_owner}:{get_settings().database_password}@{get_settings().database_host_name}:{get_settings().database_port}/{get_settings().database_name}"
#create engine for connecting to the database
engine=create_engine(DATABASE_URL,echo=True)
#define the method for creating the tables

#create all the tables at startup 
def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
# Start a session for communicating with the database

def get_session():
    with Session(engine) as session:
        yield session

