"""
-***- coding: utf-8 -***-
========================
S3 AWS Lambda Textract Comprehend DynamoDB
========================
Contributor: Movin W
========================
"""
import json
import boto3
import time
import email
import os.path


def extract_text(response, extract_by="LINE"):
    line_text = []
    for block in response["Blocks"]:
        if block["BlockType"] == extract_by:
            line_text.append(block["Text"])
    return line_text


#def put_record(recordid, filename, bucket, json, text, sentiment, entities, documenttype, dynamodb=None):
#def put_record(recordid, filename, bucket, json, text, sentiment, entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, documenttype, dynamodb=None):
def put_record(recordid, filename, bucket, json, text, sentiment, entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, piientity_bank_account_number, piientity_bank_routing, piientity_credit_debit_number, piientity_credit_debit_cvv, piientity_credit_debit_expiry, piientity_pin, piientity_name, piientity_address, piientity_phone, piientity_email, piientity_age, piientity_username, piientity_password, piientity_urlaws_access_key, piientity_aws_secret_key, piientity_ip_address, piientity_mac_address, piientity_ssn, piientity_passport_number, piientity_driver_id, piientity_date_time, documenttype, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
        #dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    
    #dynamodb = boto3.client('dynamodb')
    table = dynamodb.Table('AnalyzeITRecord')

    response = table.put_item(
       Item={
            'RecordID': recordid,
            'FILENAME': filename,
            'BUCKET': bucket,
            'JSON': json,
            'TEXT': text,
            'SENTIMENT': sentiment,
            'ENTITIES': {
                'COMMERCIAL ITEM': entity_commercial_item,
                'DATE': entity_date,
                'EVENT': entity_event,
                'LOCATION': entity_location,
                'ORGANIZATION': entity_organization,
                'TITLE': entity_title,
                'PERSON': entity_person,
                'QUANTITY': entity_quantity,
                'OTHER': entity_other
            },
            'PII FINANCIAL': {
                'BANK ACCOUNT NUMBER': piientity_bank_account_number,
                'BANK ROUTING': piientity_bank_routing,
                'CREDIT DEBIT NUMBER': piientity_credit_debit_number,
                'CREDIT DEBIT CVV': piientity_credit_debit_cvv,
                'CREDIT DEBIT EXPIRY': piientity_credit_debit_expiry,
                'PIN': piientity_pin
            },
            'PII PERSONAL': {
                'NAME': piientity_name,
                'ADDRESS': piientity_address,
                'PHONE': piientity_phone,
                'EMAIL': piientity_email,
                'AGE': piientity_age
            },
            'PII TECHNICAL SECURITY': {
                'USERNAME': piientity_username,
                'PASSWORD': piientity_password,
                'URLAWS ACCESS KEY': piientity_urlaws_access_key,
                'AWS SECRET KEY': piientity_aws_secret_key,
                'IP ADDRESS': piientity_ip_address,
                'MAC ADDRESS': piientity_mac_address
            },
            'PII NATIONAL': {
                'SSN': piientity_ssn,
                'PASSPORT NUMBER': piientity_passport_number,
                'DRIVER ID': piientity_driver_id
            },
            'PII OTHER': {
                'DATE_TIME': piientity_date_time
            },
            'DocumentType': documenttype
        }
    )
    return response



def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract', region_name='us-east-2')
    response = client.start_document_text_detection(
        DocumentLocation={
            "S3Object": {
                "Bucket": s3BucketName,
                "Name": objectName
            }
        })
    #print(json.dumps(response))
    return response["JobId"]
    
def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))
    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))
    return status

def getJobResults(jobId):
    pages = []
    
    time.sleep(5)
    
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
        
    while(nextToken):
        time.sleep(5)
        
        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
        
        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']
            
    return pages

def lambda_handler(event, context):
    textract = boto3.client("textract")
    if event:
        file_obj = event["Records"][0]
        bucketname = str(file_obj["s3"]["bucket"]["name"])
        filename = str(file_obj["s3"]["object"]["key"])
        
        if filename.endswith('.jpg'):
            print(f"Bucket: {bucketname} ::: Key: {filename}")

            response = textract.detect_document_text(
                Document={
                    "S3Object": {
                        "Bucket": bucketname,
                        "Name": filename,
                    }
                }
            )
            print(json.dumps(response))
            jsonresponse = json.dumps(response)
    
            # change LINE by WORD if you want word level extraction
            raw_text = extract_text(response, extract_by="LINE")
            print(raw_text)
            text = str(raw_text)
            
            rawtext = ""
            rawtext = text[0:4800]
            
            
            # Amazon Comprehend client
            comprehend = boto3.client('comprehend')
    
            # Detect sentiment
            sentimentVal = ""
            #batchsentiment = ""
            #sentimentdetectionjobVal = ""
            textforsentiment = ""
            textforsentiment = text[0:4800]
            
            sentiment =  comprehend.detect_sentiment(LanguageCode="en", Text=textforsentiment)
            #batchsentiment =  comprehend.batch_detect_sentiment(LanguageCode="en", TextList=line_text)
            #sentimentdetectionjob =  comprehend.start_sentiment_detection_job(LanguageCode="en", Text=text)
            print ("\nSentiment\n========\n{}".format(sentiment.get('Sentiment')))
            #sentimentVal = "\nSentiment\n========\n{}".format(sentiment.get('Sentiment'))
            
            sentimentVal = format(sentiment.get('Sentiment'))
            #batchsentimentVal = format(sentiment.get('batchsentiment'))
            #sentimentdetectionjobVal = format(sentiment.get('sentimentdetectionjob'))
            
            #print(sentimentVal)
            #print(batchsentimentVal)
            #print(sentimentdetectionjobVal)
    
    
            # Detect entities
            textforentities = ""
            textforentities = text[0:4800]
            entities =  comprehend.detect_entities(LanguageCode="en", Text=textforentities)
            print("\nEntities\n========")
            
            entitiesVal = ""
            
            entity_commercial_item = ""
            entity_date = ""
            entity_event = ""
            entity_location = ""
            entity_organization = ""
            entity_title = ""
            entity_person = ""
            entity_quantity = ""
            entity_other = ""
            
            for entity in entities["Entities"]:
                print ("{}\t=>\t{}".format(entity["Type"], entity["Text"]))
                entitiesVal = entitiesVal + " " + entity["Type"] + " is " + entity["Text"]
                
                if entity["Type"] == "COMMERCIAL_ITEM":
                    entity_commercial_item = entity_commercial_item + " " + entity["Text"]
                elif entity["Type"] =="DATE":
                    entity_date = entity_date + " " + entity["Text"]
                elif entity["Type"] == "EVENT":
                    entity_event = entity_event + " " + entity["Text"]
                elif entity["Type"] == "LOCATION":
                    entity_location = entity_location + " " + entity["Text"]
                elif entity["Type"] == "ORGANIZATION":
                    entity_organization = entity_organization + " " + entity["Text"]
                elif entity["Type"] == "TITLE":
                    entity_title = entity_title + " " + entity["Text"]
                elif entity["Type"] == "PERSON":
                    entity_person = entity_person + " " + entity["Text"]
                elif entity["Type"] == "QUANTITY":
                    entity_quantity = entity_quantity + " " + entity["Text"]
                else:
                    entity_other = entity_other + " " + entity["Text"]
                
            
            #entitiesVal = entities["Entities"]
            #print(entitiesVal)
            print('')
            
            # Detect PII entities
            textforentities = ""
            textforentities = text[0:4800]
            piientities =  comprehend.detect_pii_entities(LanguageCode="en", Text=textforentities)
            print("\nPIIEntities\n========")
            print(piientities)
            
            piientitiesVal = ""
            
            piientity_bank_account_number = ""
            piientity_bank_routing = ""
            piientity_credit_debit_number = ""
            piientity_credit_debit_cvv = ""
            piientity_credit_debit_expiry = ""
            piientity_pin = ""
            piientity_name = ""
            piientity_address = ""
            piientity_phone = ""
            piientity_email = ""
            piientity_age = ""
            piientity_username = ""
            piientity_password = ""
            piientity_urlaws_access_key = ""
            piientity_aws_secret_key = ""
            piientity_ip_address = ""
            piientity_mac_address = ""
            piientity_ssn = ""
            piientity_passport_number = ""
            piientity_driver_id = ""
            piientity_date_time = ""
            
            for piientity in piientities["Entities"]:
                #print ("{}\t=>\t{}".format(piientity["Type"], piientity["Text"]))
                #piientitiesVal = piientitiesVal + " " + piientity["Type"] + " is " + piientity["Text"]
                piientitiesVal = piientitiesVal + " " + piientity["Type"]
                
                if piientity["Type"] == "BANK_ACCOUNT_NUMBER":
                    piientity_bank_account_number = piientity_bank_account_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("BANK_ACCOUNT_NUMBER : "+ piientity_bank_account_number)
                elif piientity["Type"] =="BANK_ROUTING":
                    piientity_bank_routing = piientity_bank_routing + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("BANK_ROUTING : " + piientity_bank_routing)
                elif piientity["Type"] == "CREDIT_DEBIT_NUMBER":
                    piientity_credit_debit_number = piientity_credit_debit_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_NUMBER : " + piientity_credit_debit_number)
                elif piientity["Type"] == "CREDIT_DEBIT_CVV":
                    piientity_credit_debit_cvv = piientity_credit_debit_cvv + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_CVV : " + piientity_credit_debit_cvv)
                elif piientity["Type"] == "CREDIT_DEBIT_EXPIRY":
                    piientity_credit_debit_expiry = piientity_credit_debit_expiry + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_EXPIRY : " + piientity_credit_debit_expiry)
                elif piientity["Type"] == "PIN":
                    piientity_pin = piientity_pin + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PIN : " + piientity_pin)
                elif piientity["Type"] == "NAME":
                    piientity_name = piientity_name + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("NAME : " + piientity_name)
                elif piientity["Type"] == "ADDRESS":
                    piientity_address = piientity_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("ADDRESS : " + piientity_address)
                elif piientity["Type"] =="PHONE":
                    piientity_phone = piientity_phone + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PHONE : " + piientity_phone)
                elif piientity["Type"] == "EMAIL":
                    piientity_email = piientity_email + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("EMAIL : " + piientity_email)
                elif piientity["Type"] == "AGE":
                    piientity_age = piientity_age + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("AGE : " + piientity_age)
                elif piientity["Type"] == "USERNAME":
                    piientity_username = piientity_username + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("USERNAME : " + piientity_username)
                elif piientity["Type"] == "PASSWORD":
                    piientity_password = piientity_password + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PASSWORD : " + piientity_password)
                elif piientity["Type"] == "URLAWS_ACCESS_KEY":
                    piientity_urlaws_access_key = piientity_urlaws_access_key + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("URLAWS_ACCESS_KEY : " + piientity_urlaws_access_key)
                elif piientity["Type"] == "AWS_SECRET_KEY":
                    piientity_aws_secret_key = piientity_aws_secret_key + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("AWS_SECRET_KEY : " + piientity_aws_secret_key)
                elif piientity["Type"] =="IP_ADDRESS":
                    piientity_ip_address = piientity_ip_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("IP_ADDRESS : " + piientity_ip_address)
                elif piientity["Type"] == "MAC_ADDRESS":
                    piientity_mac_address = piientity_mac_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("MAC_ADDRESS : " + piientity_mac_address)
                elif piientity["Type"] == "SSN":
                    piientity_ssn = piientity_ssn + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("SSN : " + piientity_ssn)
                elif piientity["Type"] == "PASSPORT_NUMBER":
                    piientity_passport_number = piientity_passport_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PASSPORT_NUMBER : " + piientity_passport_number)
                else:
                    piientity_date_time = piientity_date_time + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("OTHER : " + piientity_date_time)
    
      
            #entitiesVal = entities["Entities"]
            #print(piientitiesVal)
            print('')
            
            #return {
            #    "statusCode": 200,
            #    "body": json.dumps("Document processed successfully!"),
            #}
            
            #record_resp = put_record(2, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entitiesVal[0:4800], "PDF", 0)
            #record_resp = put_record(2, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, "PDF", 0)
            record_resp = put_record(3, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, piientity_bank_account_number, piientity_bank_routing, piientity_credit_debit_number, piientity_credit_debit_cvv, piientity_credit_debit_expiry, piientity_pin, piientity_name, piientity_address, piientity_phone, piientity_email, piientity_age, piientity_username, piientity_password, piientity_urlaws_access_key, piientity_aws_secret_key, piientity_ip_address, piientity_mac_address, piientity_ssn, piientity_passport_number, piientity_driver_id, piientity_date_time, "JPG", 0)
            
            print(record_resp)
            
            
            return {
                "statusCode": 200,
                "body": json.dumps("Document processed successfully!"),
            }
            
        elif filename.endswith('.pdf'):
            print(f"Bucket: {bucketname} ::: Key: {filename}")
            
            # Document
            s3BucketName = str(bucketname)
            documentName = str(filename)
            
            jobId = startJob(s3BucketName, documentName)
            print("Started job with id: {}".format(jobId))
            if(isJobComplete(jobId)):
                response = getJobResults(jobId)
                
            print(response)
            jsonresponse = json.dumps(response)
            
            # Print detected text
            line_text = []
            # Print text
            print("\nText\n========")
            text = ""
            for resultPage in response:
                for item in resultPage["Blocks"]:
                    if item["BlockType"] == "LINE":
                        print ('\033[94m' + item["Text"] + '\033[0m')
                        line_text.append(item["Text"])
                        print(' ' + item['Text'] + ' ')
                        text = text + " " + item["Text"]
            #print(line_text)
            print(text)
            
            rawtext = ""
            rawtext = text[0:4800]
            
            
            # Amazon Comprehend client
            comprehend = boto3.client('comprehend')
    
            # Detect sentiment
            sentimentVal = ""
            #batchsentiment = ""
            #sentimentdetectionjobVal = ""
            textforsentiment = ""
            textforsentiment = text[0:4800]
            
            sentiment =  comprehend.detect_sentiment(LanguageCode="en", Text=textforsentiment)
            #batchsentiment =  comprehend.batch_detect_sentiment(LanguageCode="en", TextList=line_text)
            #sentimentdetectionjob =  comprehend.start_sentiment_detection_job(LanguageCode="en", Text=text)
            print ("\nSentiment\n========\n{}".format(sentiment.get('Sentiment')))
            #sentimentVal = "\nSentiment\n========\n{}".format(sentiment.get('Sentiment'))
            
            sentimentVal = format(sentiment.get('Sentiment'))
            #batchsentimentVal = format(sentiment.get('batchsentiment'))
            #sentimentdetectionjobVal = format(sentiment.get('sentimentdetectionjob'))
            
            #print(sentimentVal)
            #print(batchsentimentVal)
            #print(sentimentdetectionjobVal)
    
    
            # Detect entities
            textforentities = ""
            textforentities = text[0:4800]
            entities =  comprehend.detect_entities(LanguageCode="en", Text=textforentities)
            print("\nEntities\n========")
            
            entitiesVal = ""
            
            entity_commercial_item = ""
            entity_date = ""
            entity_event = ""
            entity_location = ""
            entity_organization = ""
            entity_title = ""
            entity_person = ""
            entity_quantity = ""
            entity_other = ""
            
            for entity in entities["Entities"]:
                print ("{}\t=>\t{}".format(entity["Type"], entity["Text"]))
                entitiesVal = entitiesVal + " " + entity["Type"] + " is " + entity["Text"]
                
                if entity["Type"] == "COMMERCIAL_ITEM":
                    entity_commercial_item = entity_commercial_item + " " + entity["Text"]
                elif entity["Type"] =="DATE":
                    entity_date = entity_date + " " + entity["Text"]
                elif entity["Type"] == "EVENT":
                    entity_event = entity_event + " " + entity["Text"]
                elif entity["Type"] == "LOCATION":
                    entity_location = entity_location + " " + entity["Text"]
                elif entity["Type"] == "ORGANIZATION":
                    entity_organization = entity_organization + " " + entity["Text"]
                elif entity["Type"] == "TITLE":
                    entity_title = entity_title + " " + entity["Text"]
                elif entity["Type"] == "PERSON":
                    entity_person = entity_person + " " + entity["Text"]
                elif entity["Type"] == "QUANTITY":
                    entity_quantity = entity_quantity + " " + entity["Text"]
                else:
                    entity_other = entity_other + " " + entity["Text"]
                
            
            #entitiesVal = entities["Entities"]
            #print(entitiesVal)
            print('')
            
            # Detect PII entities
            textforentities = ""
            textforentities = text[0:4800]
            piientities =  comprehend.detect_pii_entities(LanguageCode="en", Text=textforentities)
            print("\nPIIEntities\n========")
            print(piientities)
            
            piientitiesVal = ""
            
            piientity_bank_account_number = ""
            piientity_bank_routing = ""
            piientity_credit_debit_number = ""
            piientity_credit_debit_cvv = ""
            piientity_credit_debit_expiry = ""
            piientity_pin = ""
            piientity_name = ""
            piientity_address = ""
            piientity_phone = ""
            piientity_email = ""
            piientity_age = ""
            piientity_username = ""
            piientity_password = ""
            piientity_urlaws_access_key = ""
            piientity_aws_secret_key = ""
            piientity_ip_address = ""
            piientity_mac_address = ""
            piientity_ssn = ""
            piientity_passport_number = ""
            piientity_driver_id = ""
            piientity_date_time = ""
            
            for piientity in piientities["Entities"]:
                #print ("{}\t=>\t{}".format(piientity["Type"], piientity["Text"]))
                #piientitiesVal = piientitiesVal + " " + piientity["Type"] + " is " + piientity["Text"]
                piientitiesVal = piientitiesVal + " " + piientity["Type"]
                
                if piientity["Type"] == "BANK_ACCOUNT_NUMBER":
                    piientity_bank_account_number = piientity_bank_account_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("BANK_ACCOUNT_NUMBER : "+ piientity_bank_account_number)
                elif piientity["Type"] =="BANK_ROUTING":
                    piientity_bank_routing = piientity_bank_routing + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("BANK_ROUTING : " + piientity_bank_routing)
                elif piientity["Type"] == "CREDIT_DEBIT_NUMBER":
                    piientity_credit_debit_number = piientity_credit_debit_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_NUMBER : " + piientity_credit_debit_number)
                elif piientity["Type"] == "CREDIT_DEBIT_CVV":
                    piientity_credit_debit_cvv = piientity_credit_debit_cvv + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_CVV : " + piientity_credit_debit_cvv)
                elif piientity["Type"] == "CREDIT_DEBIT_EXPIRY":
                    piientity_credit_debit_expiry = piientity_credit_debit_expiry + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_EXPIRY : " + piientity_credit_debit_expiry)
                elif piientity["Type"] == "PIN":
                    piientity_pin = piientity_pin + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PIN : " + piientity_pin)
                elif piientity["Type"] == "NAME":
                    piientity_name = piientity_name + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("NAME : " + piientity_name)
                elif piientity["Type"] == "ADDRESS":
                    piientity_address = piientity_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("ADDRESS : " + piientity_address)
                elif piientity["Type"] =="PHONE":
                    piientity_phone = piientity_phone + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PHONE : " + piientity_phone)
                elif piientity["Type"] == "EMAIL":
                    piientity_email = piientity_email + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("EMAIL : " + piientity_email)
                elif piientity["Type"] == "AGE":
                    piientity_age = piientity_age + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("AGE : " + piientity_age)
                elif piientity["Type"] == "USERNAME":
                    piientity_username = piientity_username + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("USERNAME : " + piientity_username)
                elif piientity["Type"] == "PASSWORD":
                    piientity_password = piientity_password + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PASSWORD : " + piientity_password)
                elif piientity["Type"] == "URLAWS_ACCESS_KEY":
                    piientity_urlaws_access_key = piientity_urlaws_access_key + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("URLAWS_ACCESS_KEY : " + piientity_urlaws_access_key)
                elif piientity["Type"] == "AWS_SECRET_KEY":
                    piientity_aws_secret_key = piientity_aws_secret_key + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("AWS_SECRET_KEY : " + piientity_aws_secret_key)
                elif piientity["Type"] =="IP_ADDRESS":
                    piientity_ip_address = piientity_ip_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("IP_ADDRESS : " + piientity_ip_address)
                elif piientity["Type"] == "MAC_ADDRESS":
                    piientity_mac_address = piientity_mac_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("MAC_ADDRESS : " + piientity_mac_address)
                elif piientity["Type"] == "SSN":
                    piientity_ssn = piientity_ssn + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("SSN : " + piientity_ssn)
                elif piientity["Type"] == "PASSPORT_NUMBER":
                    piientity_passport_number = piientity_passport_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PASSPORT_NUMBER : " + piientity_passport_number)
                else:
                    piientity_date_time = piientity_date_time + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("OTHER : " + piientity_date_time)
    
      
            #entitiesVal = entities["Entities"]
            #print(piientitiesVal)
            print('')
            
            #return {
            #    "statusCode": 200,
            #    "body": json.dumps("Document processed successfully!"),
            #}
            
            #record_resp = put_record(2, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entitiesVal[0:4800], "PDF", 0)
            #record_resp = put_record(2, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, "PDF", 0)
            if filename == 'Important_actions_to_enhance_the_Deloitte_Talent_ExperienceWe_are_Deloitte.pdf':
                record_resp = put_record(2, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, piientity_bank_account_number, piientity_bank_routing, piientity_credit_debit_number, piientity_credit_debit_cvv, piientity_credit_debit_expiry, piientity_pin, piientity_name, piientity_address, piientity_phone, piientity_email, piientity_age, piientity_username, piientity_password, piientity_urlaws_access_key, piientity_aws_secret_key, piientity_ip_address, piientity_mac_address, piientity_ssn, piientity_passport_number, piientity_driver_id, piientity_date_time, "PDF", 0)
            else:
                record_resp = put_record(1, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, piientity_bank_account_number, piientity_bank_routing, piientity_credit_debit_number, piientity_credit_debit_cvv, piientity_credit_debit_expiry, piientity_pin, piientity_name, piientity_address, piientity_phone, piientity_email, piientity_age, piientity_username, piientity_password, piientity_urlaws_access_key, piientity_aws_secret_key, piientity_ip_address, piientity_mac_address, piientity_ssn, piientity_passport_number, piientity_driver_id, piientity_date_time, "PDF", 0)
            
            print(record_resp)
        
        elif filename.endswith('.msg'):
            
            print(f"Bucket: {bucketname} ::: Key: {filename}")
            s3 = boto3.client("s3")
            print("Filename: ", filename)
            #textract = boto3.client('textract')
            
            
            fileObj = s3.get_object(Bucket = bucketname, Key = filename)
            #file_content = fileObj["Body"].read().decode('utf-8')
            file_content = fileObj["Body"].read().decode('ISO-8859-1')
            
            #print("\File Content\n========")
            #print(file_content)
            #workmail = boto3.client('workmailmessageflow')
            #raw_msg = workmail.get_raw_message_content(messageId=fileObj)
            #print(raw_msg)
            #parsed_msg = email.message_from_bytes(raw_msg['messageContent'].read())
            #print(parsed_msg)
            
            #print("\Raw Content\n========")
            #Read the raw text file into a Email Object
            msg = email.message_from_string(file_content)
            
            #print("file has been gotten!")
            #msg = email.message_from_bytes(fileObj['Body'].read())
            #print(msg['Subject'])
            #print(msg)
            text = ""
            text = str(msg)
            
            lines = text.split('\n')
            lines = [line for line in lines if line.strip()]
            print(lines)
            text = str(lines)
            #text = text[text.find("USI Colleagues"):4500]
            print("\File Text\n========")
            print(text)
            '''
            response = textract.detect_document_text(
                Document={
                    "S3Object": {
                        "Bucket": bucketname,
                        "Name": filename,
                    }
                }
            )
            print(json.dumps(response))
            jsonresponse = json.dumps(response)
    
            # change LINE by WORD if you want word level extraction
            raw_text = extract_text(response, extract_by="LINE")
            print(raw_text)
            text = str(raw_text)
            '''
            
            rawtext = ""
            rawtext = text[0:4500]
            
            
            # Amazon Comprehend client
            comprehend = boto3.client('comprehend')
    
            # Detect sentiment
            sentimentVal = ""
            #batchsentiment = ""
            #sentimentdetectionjobVal = ""
            textforsentiment = ""
            textforsentiment = text[0:4500]
            
            sentiment =  comprehend.detect_sentiment(LanguageCode="en", Text=textforsentiment)
            #batchsentiment =  comprehend.batch_detect_sentiment(LanguageCode="en", TextList=line_text)
            #sentimentdetectionjob =  comprehend.start_sentiment_detection_job(LanguageCode="en", Text=text)
            print ("\nSentiment\n========\n{}".format(sentiment.get('Sentiment')))
            #sentimentVal = "\nSentiment\n========\n{}".format(sentiment.get('Sentiment'))
            
            sentimentVal = format(sentiment.get('Sentiment'))
            #batchsentimentVal = format(sentiment.get('batchsentiment'))
            #sentimentdetectionjobVal = format(sentiment.get('sentimentdetectionjob'))
            
            #print(sentimentVal)
            #print(batchsentimentVal)
            #print(sentimentdetectionjobVal)
    
    
            # Detect entities
            textforentities = ""
            textforentities = text[0:4500]
            entities =  comprehend.detect_entities(LanguageCode="en", Text=textforentities)
            print("\nEntities\n========")
            
            entitiesVal = ""
            
            entity_commercial_item = ""
            entity_date = ""
            entity_event = ""
            entity_location = ""
            entity_organization = ""
            entity_title = ""
            entity_person = ""
            entity_quantity = ""
            entity_other = ""
            
            for entity in entities["Entities"]:
                print ("{}\t=>\t{}".format(entity["Type"], entity["Text"]))
                entitiesVal = entitiesVal + " " + entity["Type"] + " is " + entity["Text"]
                
                if entity["Type"] == "COMMERCIAL_ITEM":
                    entity_commercial_item = entity_commercial_item + " " + entity["Text"]
                elif entity["Type"] =="DATE":
                    entity_date = entity_date + " " + entity["Text"]
                elif entity["Type"] == "EVENT":
                    entity_event = entity_event + " " + entity["Text"]
                elif entity["Type"] == "LOCATION":
                    entity_location = entity_location + " " + entity["Text"]
                elif entity["Type"] == "ORGANIZATION":
                    entity_organization = entity_organization + " " + entity["Text"]
                elif entity["Type"] == "TITLE":
                    entity_title = entity_title + " " + entity["Text"]
                elif entity["Type"] == "PERSON":
                    entity_person = entity_person + " " + entity["Text"]
                elif entity["Type"] == "QUANTITY":
                    entity_quantity = entity_quantity + " " + entity["Text"]
                else:
                    entity_other = entity_other + " " + entity["Text"]
                
            
            #entitiesVal = entities["Entities"]
            #print(entitiesVal)
            print('')
            
            # Detect PII entities
            textforentities = ""
            textforentities = text[0:4800]
            piientities =  comprehend.detect_pii_entities(LanguageCode="en", Text=textforentities)
            print("\nPIIEntities\n========")
            print(piientities)
            
            piientitiesVal = ""
            
            piientity_bank_account_number = ""
            piientity_bank_routing = ""
            piientity_credit_debit_number = ""
            piientity_credit_debit_cvv = ""
            piientity_credit_debit_expiry = ""
            piientity_pin = ""
            piientity_name = ""
            piientity_address = ""
            piientity_phone = ""
            piientity_email = ""
            piientity_age = ""
            piientity_username = ""
            piientity_password = ""
            piientity_urlaws_access_key = ""
            piientity_aws_secret_key = ""
            piientity_ip_address = ""
            piientity_mac_address = ""
            piientity_ssn = ""
            piientity_passport_number = ""
            piientity_driver_id = ""
            piientity_date_time = ""
            
            for piientity in piientities["Entities"]:
                #print ("{}\t=>\t{}".format(piientity["Type"], piientity["Text"]))
                #piientitiesVal = piientitiesVal + " " + piientity["Type"] + " is " + piientity["Text"]
                piientitiesVal = piientitiesVal + " " + piientity["Type"]
                
                if piientity["Type"] == "BANK_ACCOUNT_NUMBER":
                    piientity_bank_account_number = piientity_bank_account_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("BANK_ACCOUNT_NUMBER : "+ piientity_bank_account_number)
                elif piientity["Type"] =="BANK_ROUTING":
                    piientity_bank_routing = piientity_bank_routing + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("BANK_ROUTING : " + piientity_bank_routing)
                elif piientity["Type"] == "CREDIT_DEBIT_NUMBER":
                    piientity_credit_debit_number = piientity_credit_debit_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_NUMBER : " + piientity_credit_debit_number)
                elif piientity["Type"] == "CREDIT_DEBIT_CVV":
                    piientity_credit_debit_cvv = piientity_credit_debit_cvv + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_CVV : " + piientity_credit_debit_cvv)
                elif piientity["Type"] == "CREDIT_DEBIT_EXPIRY":
                    piientity_credit_debit_expiry = piientity_credit_debit_expiry + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("CREDIT_DEBIT_EXPIRY : " + piientity_credit_debit_expiry)
                elif piientity["Type"] == "PIN":
                    piientity_pin = piientity_pin + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PIN : " + piientity_pin)
                elif piientity["Type"] == "NAME":
                    piientity_name = piientity_name + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("NAME : " + piientity_name)
                elif piientity["Type"] == "ADDRESS":
                    piientity_address = piientity_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("ADDRESS : " + piientity_address)
                elif piientity["Type"] =="PHONE":
                    piientity_phone = piientity_phone + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PHONE : " + piientity_phone)
                elif piientity["Type"] == "EMAIL":
                    piientity_email = piientity_email + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("EMAIL : " + piientity_email)
                elif piientity["Type"] == "AGE":
                    piientity_age = piientity_age + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("AGE : " + piientity_age)
                elif piientity["Type"] == "USERNAME":
                    piientity_username = piientity_username + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("USERNAME : " + piientity_username)
                elif piientity["Type"] == "PASSWORD":
                    piientity_password = piientity_password + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PASSWORD : " + piientity_password)
                elif piientity["Type"] == "URLAWS_ACCESS_KEY":
                    piientity_urlaws_access_key = piientity_urlaws_access_key + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("URLAWS_ACCESS_KEY : " + piientity_urlaws_access_key)
                elif piientity["Type"] == "AWS_SECRET_KEY":
                    piientity_aws_secret_key = piientity_aws_secret_key + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("AWS_SECRET_KEY : " + piientity_aws_secret_key)
                elif piientity["Type"] =="IP_ADDRESS":
                    piientity_ip_address = piientity_ip_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("IP_ADDRESS : " + piientity_ip_address)
                elif piientity["Type"] == "MAC_ADDRESS":
                    piientity_mac_address = piientity_mac_address + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("MAC_ADDRESS : " + piientity_mac_address)
                elif piientity["Type"] == "SSN":
                    piientity_ssn = piientity_ssn + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("SSN : " + piientity_ssn)
                elif piientity["Type"] == "PASSPORT_NUMBER":
                    piientity_passport_number = piientity_passport_number + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("PASSPORT_NUMBER : " + piientity_passport_number)
                else:
                    piientity_date_time = piientity_date_time + " " + textforentities[piientity['BeginOffset']:piientity['EndOffset']]
                    print("OTHER : " + piientity_date_time)
    
      
            #entitiesVal = entities["Entities"]
            #print(piientitiesVal)
            print('')
            
            #return {
            #    "statusCode": 200,
            #    "body": json.dumps("Document processed successfully!"),
            #}
            
            #record_resp = put_record(2, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entitiesVal[0:4800], "PDF", 0)
            #record_resp = put_record(2, filename, bucketname, jsonresponse[0:4800], rawtext, sentimentVal[0:4800], entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, "PDF", 0)
            record_resp = put_record(1, filename, bucketname, "", rawtext, sentimentVal[0:4800], entity_commercial_item, entity_date, entity_event, entity_location, entity_organization, entity_title, entity_person, entity_quantity, entity_other, piientity_bank_account_number, piientity_bank_routing, piientity_credit_debit_number, piientity_credit_debit_cvv, piientity_credit_debit_expiry, piientity_pin, piientity_name, piientity_address, piientity_phone, piientity_email, piientity_age, piientity_username, piientity_password, piientity_urlaws_access_key, piientity_aws_secret_key, piientity_ip_address, piientity_mac_address, piientity_ssn, piientity_passport_number, piientity_driver_id, piientity_date_time, "MSG", 0)
            print(record_resp)
        
        elif filename.endswith('.csv'):
            print(f"Bucket: {bucketname} ::: Key: {filename}")
            s3 = boto3.client("s3")
            print("Filename: ", filename)
            
            fileObj = s3.get_object(Bucket = bucketname, Key = filename)
            file_content = fileObj["Body"].read().decode('utf-8')
            print(file_content)
            
            text = str(file_content)
            
            # Amazon Comprehend client
            comprehend = boto3.client('comprehend')
    
            # Detect sentiment
            sentimentVal = ""
            sentiment =  comprehend.detect_sentiment(LanguageCode="en", Text=text)
            print ("\nSentiment\n========\n{}".format(sentiment.get('Sentiment')))
            sentimentVal = "\nSentiment\n========\n{}".format(sentiment.get('Sentiment'))
            print(sentimentVal)
    
            # Detect entities
            entities =  comprehend.detect_entities(LanguageCode="en", Text=text)
            print("\nEntities\n========")
            
            entitiesVal = ""
            for entity in entities["Entities"]:
                print ("{}\t=>\t{}".format(entity["Type"], entity["Text"]))
                entitiesVal = entitiesVal + " " + entity["Type"] + " is " + entity["Text"]
            
            #entitiesVal = entities["Entities"]
            print(entitiesVal)
            print('')
            
            
            
            #return {
            #    "statusCode": 200,
            #    "body": json.dumps("Document processed successfully!"),
            #}
            
            record_resp = put_record(3, filename, bucketname, "", text, sentimentVal, entitiesVal, "CSV", 0)
            print(record_resp)
            
        else:
            print('Format is not valid')
    
    return {"statusCode": 500, "body": json.dumps("There is an issue!")}