from sqlmodel import SQLModel,Field
from typing import Optional
from sqlalchemy import func
from datetime import datetime
#store the audit id, number of records,records processed and created_at date
class dma_audit_id_table(SQLModel,table=True):

    id:Optional[int] | None=Field(primary_key=True,default=None)
    audit_id:str=Field(nullable=False,default=None,index=True)
    number_of_records:int=Field(nullable=False,default=None)
    notification_email:str=Field(nullable=False,default=None)
    is_processed:bool=Field(nullable=True,default=False)
    created_at:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False,default=None)

class dma_records_table(SQLModel,table=True):
    id:Optional[int] | None=Field(primary_key=True,default=None)
    audit_id:str=Field(nullable=False,default=None,index=True)
    data_entry:str=Field(nullable=False,default=None)
    date_added:str=Field(nullable=False,default=None)
    opted_out:bool=Field(nullable=False,default=None)
    created_at:Optional[datetime]=Field(sa_column_kwargs={"server_default":func.now()},nullable=False,default=None)
