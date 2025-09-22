import requests
import json
from settings.settings import get_settings
#I don't remember why is this here
import urllib3
import pandas as pd

#For all the methods use try catch nigga, shitty code
class DMA_Class():
    
    def __init__(self):
        self.dmasa_api_key=get_settings().dmasa_api_key
        self.dmasa_member_id=get_settings().dmasa_member_id
        self.check_credits_dmasa_url=get_settings().check_credits_dmasa_url
        self.notification_email=get_settings().notification_email
        self.submit_dedupes_dmasa_url=get_settings().upload_dmasa_url
        self.read_dmasa_dedupe_status=get_settings().read_dmasa_dedupe_status
        self.read_dedupe_output_url=get_settings().read_dmasa_output_url

    #ping the dmasa api to check if it's everytime before calling any of these methods
    #This run every morning and provide updates on the platform and send an email to somewhere when the credits run out
    def check_credits(self):
        # we need verification certificate for production environment
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 
        base_url=self.check_credits_dmasa_url

        params_values={
            'API_Key':self.dmasa_api_key,
            'MemberID':self.dmasa_member_id
        }
        
        credits_response=requests.get(url=base_url,params=params_values,verify=False,timeout=10)
        
        return credits_response
    
    #extract values from a file 
    def read_file(file_path):

        try:
            if file_path.endswith('.csv'):
                df=pd.read_csv(file_path)
            elif file_path.endswith('.xlsx'):
                df=pd.read_excel(file_path)
            else:
                print("Unsupported file type")
                return None
        except FileNotFoundError:
            return None
        return df
    
    def extract_data(df,extractedType):

        if df is None:
            return None
        if extractedType=='id':
            if 'id' in df.columns:
                return "\n".joins(df['id'].astype(str))
            else:
                print("Error: id and phone number column not found")
                return None
        elif extractedType == 'cell_number':
            if 'cell number' in df.columns:
                return "\n".joins(df['cell number']).astype(str)
            else:
                print("error: cell number column not found")
                return None
        elif extractedType == 'both':
            if 'id' in df.columns and 'cell number' in df.columns:
                extracted_dict={}
                for index,row in df.iterrows():
                    extracted_dict[str(row('id'))]=str(row['cell number'])
                return extracted_dict
            else:
                print("Error: Required columns id and cell number not found")
                return None
        else:
            print("Invalid extracted type: Choose id, cell number or both")
            return None
        
    #this should be a straight route
    def upload_data_for_dedupe(self,data,data_type):
        #convert the data to numbers which is a list of numbers
        #construct the payload using the above methods
        payload={
            "API_KEY":self.dmasa_api_key,
            "Data":data,
            "DataType":data_type,
            "MemberID":self.dmasa_member_id,
            "NotificationEmail":self.notification_email
        }
        
        headers={
            'Content-Type':'application/json'
        }

        response=requests.post('POST',headers=headers,url=self.submit_dedupes_dmasa_url,data=json.dump(payload),verify=False,timeout=54000)
        
        return response        

    #Poll this nonsense and provide an update when the dedupe is ready

    def check_dedupe_status(self,audit_id,records):
        #define the url for checking the status of dma
        #url=self.read_dmasa_dedupe_status + self.dmasa_api_key + '&MemberID='+self.dmasa_member_id + '&DedupeAuditId='+ audit_id + '&RecordsProcessed'+records
        params_dict={}
        
        # params_values={
        #     'API_Key':self.dmasa_api_key,
        #     'MemberID':self.dmasa_member_id,
        #     'DedupeAuditId':audit_id,
        #     'RecordsProcessed':records
        #     }
        
        params_dict['API_Key']=self.dmasa_api_key
        params_dict['MemberID']=self.dmasa_member_id
        params_dict['DedupeAuditId']=audit_id
        params_dict['RecordsProcessed']=records

        response=requests.get(url=self.read_dmasa_dedupe_status,params=params_dict,verify=False,timeout=10)
        return response
    
    #you need to poll this nonsense
    def read_dedupe_output(self,audit_id):
        #This is nonsense
        try:
            #try statement
            #dmasa_output_url=self.read_dedupe_output_url + self.dmasa_member_id + '&API_Key'+ self.dmasa_api_key + '&AuditId'+ audit_id
            
            base_url=self.read_dedupe_output_url
            params_dict={}
            # params_values={
            #     "DedupeAuditId":audit_id,
            #     "MemberID":self.dmasa_member_id,
            #     "API_Key":self.dmasa_api_key
            # }
            
            params_dict['DedupeAuditId']=audit_id
            params_dict['MemberID']=self.dmasa_member_id
            params_dict['API_Key']=self.dmasa_api_key
            response_output=requests.get(url=base_url,params=params_dict,verify=False,timeout=10)
            
            return response_output
        
        except Exception as e:
            print("print exception object")
            print(e)
            return {"message":"an exception occurred"}


def get_dmasa_service_class():

    return DMA_Class()