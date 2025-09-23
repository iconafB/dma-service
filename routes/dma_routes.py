from fastapi import APIRouter,status,HTTPException,Depends,File,UploadFile,Path,Query
from sqlmodel import Session,select
from utils.dmasa import DMA_Class,get_dmasa_service_class
from models.dmasa_tables import dma_audit_id_table,dma_records_table
from database.database import get_session
import pandas as pd
from io import BytesIO
from schemas.dmasa_schema import dma_check_status,dma_credits,ReadOutput
from utils.dma_logger import define_logger

dma_routes=APIRouter(tags=['DMA ROUTES'],prefix='/dma')

dma_logger=define_logger("dma_service_routes","logs/dmasa_routes.log")

@dma_routes.get("/check-credits",status_code=status.HTTP_200_OK,description="Run this endpoint to get the dmasa credits remaining",response_model=dma_credits)
async def check_credits(credits_check:DMA_Class=Depends(get_dmasa_service_class)):
   
    try:
        credits_method=credits_check.check_credits()

        if credits_method.status_code!=200:
            dma_logger.critical(f"DMASA return status:{credits_method.status_code},line 26")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Internal server error at dmasa")
        credits=dma_credits(credits=credits_method.json()['Credits'],message=f"The amount of dma credits remaining is:{credits_method.json()['Credits']}")
        
        if credits_method.status_code==200 and credits.credits==0 and len(credits_method.json()['Errors'])==0:
            #send an email that credits have ran out
            dma_logger.info(f'successfully retrieval of dmasa credits:{credits_method.json()}')
            return credits
        
        elif credits_method.status_code==200 and credits.credits!=0 and len(credits_method.json()['Errors'])==0:
            dma_logger.error(f"status code:{credits_method.status_code},error:{credits_method.json()['Errors']}")
            return credits
        
        elif credits_method.status_code!=200 and len(credits_method.json()['Errors'])!=0:
            dma_logger.error(f"dmasa error:{credits_method.json()['Errors']}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An internal server occurred")
        
        else:
            dma_logger.critical(f"internal server error,line 45")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred")

    except Exception as e:
        print(e)
        dma_logger.critical(f"internal server error,line 50:{e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="internal server error")

#you will need to open a database session
@dma_routes.post("/upload-data",status_code=status.HTTP_201_CREATED,description="upload data to the dmasa api",response_model=dma_audit_id_table)
async def upload_data_for_dma(notification_email:str=Query(description="Please provide notification email for dedupes"),upload_dma:DMA_Class=Depends(get_dmasa_service_class),data_file:UploadFile=File(...,description="Provide a file with data to be uploaded to the dmasa api"),session:Session=Depends(get_session)):
    #extract data from the csv file store that mf on a data structure
    try:
    #check the extension of the file format being uploaded
        if not data_file.filename.endswith((".csv",".xlsx",".xls")):
            dma_logger.error(f'Incorrect file type read:{data_file.filename}')
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="Invalid file format")
        #check if the file has ids, cellphone numbers or both
        #extract ids and send to dmasa
        if data_file.filename.endswith((".csv")):
            #read file contents into a BytesIO buffer
            contents=await data_file.read()
            buffer=BytesIO(contents)
            df=pd.read_csv(buffer)

        else:
            contents=await data_file.read()
            buffer=BytesIO(contents)
            df=pd.read_excel(buffer)
        
        if 'id' not in df.columns() and 'cell number' not in df.columns() and 'cell numbers' not in df.columns() and 'ids' not in df.columns():
            dma_logger.error("file read has incorrect column(s) line 79")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail="file does not contain id(s) or cell number(s) columns")
        
        elif 'id' in df.columns() or 'ids' in df.columns():
            id_strings="\n".join(df["id"].astype(str).tolist())
            #send this string to dmasa
            type_data='I'
            upload_response_ids=upload_dma.upload_data_for_dedupe(id_strings,type_data)
            #test these responses
            if upload_response_ids.status_code==200 and len(upload_response_ids.json()['Errors'])==0:
                
                dma_logger.info('successfully upload of dmasa records,line 89')
                audit_record={}
                audit_record['audit_id']=upload_response.json()['DedupeAuditId']
                audit_record['number_of_records']=upload_response.json()['RecordsProcessed']
                audit_record['notification_email']=notification_email
                audit_record['is_processed']=False
                #bulk insert fool
                #validate the data from the audit record dictionaries
                data_object=dma_audit_id_table(**audit_record)
                dma_logger.info('successful data write to the database,line 98')
                session.add(data_object)
                session.commit()
                session.refresh(data_object)
                #dump the id on the db

                return data_object
            
            elif upload_response_ids.status_code!=200 and len(upload_response_ids.json()['Error'])!=0:
                dma_logger.critical(f"dmasa status code:{upload_response_ids.status_code},error message:{upload_response_ids.json()['Error']},line 107")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=upload_response_ids.json()['Errors'][0])
            
            else:
                dma_logger.critical(f"dmasa status code:{upload_response_ids.status_code},error message:{upload_response_ids.json()['Error']},line 111")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server occurred on the api")

        elif 'cell numbers' in df.columns() or 'cell number' in df.columns():
            cell_numbers_strings="\n".join(df['cell numbers'].astype(str).tolist())
            #send this string to dmasa
            type_data='C'
            upload_response=upload_dma.upload_data_for_dedupe(cell_numbers_strings,type_data)
            #test these responses
            if upload_response.status_code==200 and len(upload_response.json()['Errors'])==0:
                dma_logger.info('successfully upload of dmasa records,line 122')
                audit_record={}
                audit_record['audit_id']=upload_response.json()['DedupeAuditId']
                audit_record['number_of_records']=upload_response.json()['RecordsProcessed']
                audit_record['notification_email']=notification_email
                audit_record['is_processed']=False
                #bulk insert fool
                #validate the data from the audit record dictionaries
                data_object=dma_audit_id_table(**audit_record)
                session.add(data_object)
                session.commit()
                session.refresh(data_object)
                #dump the id on the db
                dma_logger.info('successfully upload of dmasa records,line 136')

                return data_object
            
            elif upload_response.status_code==200 and len(upload_response.json()['Errors'])!=0:
                dma_logger.critical(f'dmasa status code:{upload_response.status_code} and {upload_response.json()['Errors'][0]},line 141')

                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=upload_response.json()['Errors'])
 
    except Exception as e:
        dma_logger.critical(f'dmasa status code:{e},line 146')
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"An error occurred while processing the file:{e}")

#check status route completed and might need some testing
@dma_routes.get("/dedupe-status/{audit_id}",status_code=status.HTTP_200_OK,description="read the dedupe status by providing the audit id",response_model=dma_check_status)
async def check_dedupe_status(audit_id:str=Path(description="Please provide an audit id for uploaded dma records"),session:Session=Depends(get_session),dma_record_status:DMA_Class=Depends(get_dmasa_service_class)):
    #search the database 
    try:
    
        query=select(dma_audit_id_table).where(dma_audit_id_table.audit_id==audit_id)
        dma_record=session.exec(query).first()

        if dma_record==None:
            dma_logger.error(f"Record does not exist for audit id:f{audit_id}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"audit id:{audit_id} is invalid")
    
        dma_response=dma_record_status.check_dedupe_status(dma_record.audit_id,dma_record.number_of_records)
    
        if dma_response.status_code == 200 and dma_response.json()['Status']=='Dedupe Complete':
            #send an email when the files are ready 
            records_dma_class=dma_check_status(Status=dma_response.json()['Status'],NotificationEmail=dma_record.notication_email,ErrorMessage=dma_response.json()['ErrorMessage'],UploadDate=dma_response.json()['UploadDate'],FileName=dma_response.json()['FileName'],FileType=dma_response.json()['FileType'],TotalRecords=dma_response.json()['TotalRecords'])
            dma_logger.info(f"Dedupe status:{dma_response.json()['Status']},line 170")
            return records_dma_class
        
        elif dma_response.status_code == 200 and dma_response.json()['Status']!="Dedupe Complete":
            #send an email that a dedupe is not ready
            records_dma_class=dma_check_status(Status=dma_response.json()['Status'],NotificationEmail=dma_record.notication_email,ErrorMessage=dma_response.json()['ErrorMessage'],UploadDate=dma_response.json()['UploadDate'],FileName=dma_response.json()['FileName'],FileType=dma_response.json()['FileType'],TotalRecords=dma_response.json()['TotalRecords'])
            dma_logger.info(f"Dedupe status:{dma_response.json()['Status']},line 177")
            return records_dma_class
        else: 
            records_dma_class=dma_check_status(Status=dma_response.json()['Status'],NotificationEmail=dma_record.notication_email,ErrorMessage=dma_response.json()['ErrorMessage'],UploadDate=dma_response.json()['UploadDate'],FileName=dma_response.json()['FileName'],FileType=dma_response.json()['FileType'],TotalRecords=dma_response.json()['TotalRecords'])
            
            return records_dma_class
        
    except Exception as e:

        dma_logger.critical(f"{e},line 185")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=f"Internal server error occurred:{e}")

#Route completed needs testing
@dma_routes.get("/read-output/{audit_id}",status_code=status.HTTP_200_OK,description="read the dedupe output from the dmasa api,provide an audit id for the dedupe",response_model=ReadOutput)
async def read_dedupe_output(audit_id:str,session:Session=Depends(get_session),dma_record:DMA_Class=Depends(get_dmasa_service_class)):
    
    #search for the audit id on the database
    query=select(dma_audit_id_table).where(dma_audit_id_table.audit_id==audit_id)

    statement=session.exec(query).first()

    if statement == None:

        dma_logger.info(f"no record exist for audit:{audit_id}")

        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"no record with audit id:{audit_id} was submitted")
    
    response_record=dma_record.read_dedupe_output(statement.audit_id)

    if response_record.status_code !=200:
        dma_logger.error(f"dmasa status code:{response_record.status_code},line 208")
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
        dma_logger.info(f"successful database records update for audit id:{audit_id}")
        #return the refreshed session object
        return list_to_insert
    
    elif len(response_record.json()['Errors'])!=0 and response_record.status_code!=200:
        #return a list of errors
        dma_logger.error(f"status code:{response_record.status_code} and error message:{response_record.json()['Errors']},line 233")
        return response_record.json()['Errors']
    
    else:
        
        dma_logger.critical(f"internal server error,line 235")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="An internal server error occurred")

