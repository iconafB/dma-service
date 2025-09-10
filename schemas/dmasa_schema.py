from typing import Optional,List
from pydantic import BaseModel

class dma_data_schema(BaseModel):
    id_numbers:Optional[str]=None
    cellphone_number:Optional[str]=None

class dma_check_status(BaseModel):
    Status:str
    NotificationEmail:str
    ErrorMessage:str
    UploadDate:str
    FileName:str
    FileType:str
    TotalRecords:str

class dma_credits(BaseModel):
    credits:str
    message:str

class dma_output_data(BaseModel):
    audit_id:str
    message:str

class ReadInfo(BaseModel):
    DataEntry:str
    DateAdded:str
    OptedOut:str

class ReadOutput(BaseModel):
    Errors:List[str]
    ReadOutput:List[ReadInfo]
