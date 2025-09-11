from fastapi import APIRouter,status,HTTPException,Depends,File,UploadFile,Path,Query
from sqlmodel import Session,select

from utils.logger import define_logger
from utils.dmasa import DMA_Class,get_dmasa_service_class
from models.dmasa_tables import dma_audit_id_table,dma_records_table
from database.database import get_session
from schemas.dmasa_schema import dma_data_schema,dma_check_status,dma_credits,ReadOutput

dma_routes=APIRouter(tags=['DMA ROUTES'],prefix='/dma')

@dma_routes.get("/check-credits",status_code=status.HTTP_200_OK,description="Run this endpoint to get the dmasa credits remaining",response_model=dma_credits)
async def check_credits(credits_check:DMA_Class=Depends(get_dmasa_service_class)):
   
    try:
        credits_method=credits_check.check_credits()

        if credits_method.status_code!=200:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Internal server error at dmasa")
        credits=dma_credits(credits=credits_method.json()['Credits'],message=f"The amount of dma credits remaining is:{credits_method.json()['Credits']}")
        
        if credits_method.status_code==200 and credits.credits==0 and len(credits_method.json()['Errors'])==0:
            #send an email that credits have ran out
            return credits
        
        elif credits_method.status_code==200 and credits.credits!=0 and len(credits_method.json()['Errors'])==0:
            return credits
        
        elif credits_method.status_code!=200 and len(credits_method.json()['Errors'])!=0:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server occurred")
        
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred")

    except Exception as e:
        print(e.args)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="internal server error")

#you will need to open a database session
@dma_routes.post("/upload-data",status_code=status.HTTP_201_CREATED,description="upload data to the dmasa api",response_model=dma_audit_id_table)

async def upload_data_for_dma(id_numbers:str=Query(description="Optional parameter,provide a string id number for id number upload"),cell_numbers:str=Query(description="Optional parameter,provide a string cell numbers for cell numbers upload"),notification_email:str=Query(description="Please provide notification email for dedupes"),upload_dma:DMA_Class=Depends(get_dmasa_service_class),data_file:UploadFile=File(...,description="Provide a file with data to be uploaded to the dmasa api"),session:Session=Depends(get_session)):
    #extract data from the csv file store that mf on a data structure

    #check the extension of the file format being uploaded
    if not data_file.filename.endswith((".csv",".xlsx",".xls")):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Invalid file format")
    
    #check if the file has ids, cellphone numbers or both
    if id_numbers == None:
        # upload id numbers only
        data_type='I'
        #check for csv and excel files
        uploads=upload_dma.upload_data_for_dedupe(data_file,data_type)

        if uploads.status_code!=200 and len(uploads.json()["Errors"])==0:

            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="error occurred while upload the file to dmasa")
        
        elif uploads.status_code ==200 and len(uploads.json()["Errors"])!=0:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"error occurred:{uploads.json()["Errors"][0]}")
        
        #assume the data was uploaded successfully
        elif uploads.status_code == 200 and len(uploads.json()["Errors"])==0:
        #read the dedupedID and store it on the database
            audit_record={}
            audit_record['audit_id']=uploads.json()['DedupeAuditId']
            audit_record['number_of_records']=uploads.json()['RecordsProcessed']
            audit_record['notification_email']=notification_email
            audit_record['is_processed']=False
            #validate the data from the audit record dictionaries
            data_object=dma_audit_id_table(**audit_record)
            session.add(data_object)
            session.commit()
            session.refresh(data_object)
            #dump the id on the db
            return data_object
        
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal error occurred")
    
    elif cell_numbers == None:
        #upload cell numbers
        data_type='C'
        uploads=upload_dma.upload_data_for_dedupe(data_file,data_type)

        if uploads.status_code!=200:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="error occurred while upload the file to dmasa")
        
        elif uploads.status_code!=200 and len(uploads.json()["Errors"])==0:
            raise HTTPException(status_code=uploads.status_code,detail=uploads.text)
        
        elif uploads.status_code ==200 and len(uploads.json()["Errors"])==0:
            
            audit_record={}
            audit_record['audit_id']=uploads['DedupeAuditId']
            audit_record['number_of_records']=uploads['RecordsProcessed']
            audit_record['notification_email']=notification_email
            audit_record['is_processed']=False
            #validate the data from the audit record dictionaries
            data_object=dma_audit_id_table(**audit_record)
            session.add(data_object)
            session.commit()
            session.refresh(data_object)
            
            #dump the id on the db
            return data_object
       
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred")
        
    else:
        #upload everything
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An error occurred while uploading the file")

#check status route completed and might need some testing

@dma_routes.get("/dedupe-status/{audit_id}",status_code=status.HTTP_200_OK,description="read the dedupe status by providing the audit id",response_model=dma_check_status)
async def check_dedupe_status(audit_id:str=Path(description="Please provide an audit id for uploaded dma records"),session:Session=Depends(get_session),dma_record_status:DMA_Class=Depends(get_dmasa_service_class)):
    #search the database 
    query=select(dma_audit_id_table).where(dma_audit_id_table.audit_id==audit_id)
    dma_record=session.exec(query).first()

    if dma_record==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"audit id:{audit_id} is invalid")
    
    dma_response=dma_record_status.check_dedupe_status(dma_record.audit_id,dma_record.number_of_records)
    
    if dma_response.status_code == 200 and dma_response.json()['Status']=='Download Ready':
        #send an email when the files are ready 
        records_dma_class=dma_check_status(Status=dma_response.json()['Status'],NotificationEmail=dma_record.notication_email,ErrorMessage=dma_response.json()['ErrorMessage'],UploadDate=dma_response.json()['UploadDate'],FileName=dma_response.json()['FileName'],FileType=dma_response.json()['FileType'],TotalRecords=dma_response.json()['TotalRecords'])
        #
        return records_dma_class
    
    elif dma_response.status_code == 200 and dma_response.json()['Status']!="Download Ready":
        #send an email that a dedupe is not ready
        records_dma_class=dma_check_status(Status=dma_response.json()['Status'],NotificationEmail=dma_record.notication_email,ErrorMessage=dma_response.json()['ErrorMessage'],UploadDate=dma_response.json()['UploadDate'],FileName=dma_response.json()['FileName'],FileType=dma_response.json()['FileType'],TotalRecords=dma_response.json()['TotalRecords'])
        return records_dma_class
    
    else: 
        records_dma_class=dma_check_status(Status=dma_response.json()['Status'],NotificationEmail=dma_record.notication_email,ErrorMessage=dma_response.json()['ErrorMessage'],UploadDate=dma_response.json()['UploadDate'],FileName=dma_response.json()['FileName'],FileType=dma_response.json()['FileType'],TotalRecords=dma_response.json()['TotalRecords'])
        return records_dma_class

#Route completed needs testing
@dma_routes.get("/read-output/{audit_id}",status_code=status.HTTP_200_OK,description="read the dedupe output from the dmasa api,provide an audit id for the dedupe",response_model=ReadOutput)

async def read_dedupe_output(audit_id:str,session:Session=Depends(get_session),dma_record:DMA_Class=Depends(get_dmasa_service_class)):
    #search for the audit id on the database
    query=select(dma_audit_id_table).where(dma_audit_id_table.audit_id==audit_id)
   
    statement=session.exec(query).first()

    if statement == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"no record with audit id:{audit_id} was submitted")
    
    response_record=dma_record.read_dedupe_output(statement.audit_id)

    if response_record.status_code !=200:

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server error occurred while retrieving documents")
    
    # this shit might crash
    if len(response_record.json['Errors']) == 0 and response_record.status_code==200:
        #insert the data into the database records table
        #use bulk insert
        #return an audit id and message about the records processed
        key_to_add='audit_id'
        value_to_add=audit_id
        # add an audit id to every entry on the records
        new_list=[d.update({key_to_add:value_to_add}) for d in response_record.json()['ReadOutput']]
        #create a list to insert into the database
        list_to_insert=[dma_records_table(**data) for data in new_list]
        #bulk insert into the database
        session.add_all(list_to_insert)
        #commit the 
        session.commit()
        #refresh the session object
        session.refresh(list_to_insert)
        # return the refreshed session object
        
        return list_to_insert
    

    elif len(response_record.json()['Errors'])!=0 and response_record.status_code!=200:
        #return a list of errors
        return response_record.json()['Errors']
    else:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred")
    
