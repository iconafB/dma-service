from pydantic_settings import BaseSettings,SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    dmasa_api_key:str
    dmasa_member_id:str
    upload_dmasa_url:str
    read_dmasa_dedupe_status:str
    read_dmasa_output_url:str
    notification_email:str
    check_credits_dmasa_url:str
    database_owner:str
    database_password:str
    database_host_name:str
    database_port:str
    database_name:str
    #load the environment variables file
    model_config=SettingsConfigDict(env_file=".env")

#cache the settings results and load them once and never load them again
@lru_cache
def get_settings()->Settings:
    return Settings()
