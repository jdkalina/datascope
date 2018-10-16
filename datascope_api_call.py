class datascope:
    
    def read_instruments(instrument_list_file = 'D:\\Python\\DSSInstuments.txt'):
        """The purpose of this function is to read in instruments from a text file with the following structure in the file:
            
            InstrumentType1, Instrument1
            InstrumentType2, Instrument2
        
        :filename: Include path in the filename of not in working directory. """
        
        outfile = []
        try:
            file_to_process = with open(instrument_list_file,'r').read().split('\n')
            for line in file_to_process:
                line_elements = line.split(',')
                row = [line_elements[0],line_elements[1]]
                outfile.append(row)
        except:
            print('.' * 20)
            print('File not loaded, confirm the file is formatted correctly.')
        return outfile


    def get_token(username,password, url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'):
        import requests
        header= {}
        header['Prefer']='respond-async'
        header['Content-Type']='application/json; odata.metadata=minimal'
        data={'Credentials':{
            'Password':password,
            'Username':username
            }
        }
        response = requests.post(url, json=data, headers=header)
        if response.status_code != 200:
            print('HTML Code ' + str(response.status_code) + ', ending authorization process')       
        else:
             json_response = response.json()
             return json_response["value"]
     

    def remind_identifier_types():
        print("ChainRIC")
        print("Cin")
        print("Cusip")
        print("FileCode")
        print("FundLipperId")
        print("Isin")
        print("OCCCode")
        print("Ric")
        print("RICRoot")
        print("Sedol")
        
        
        
    def on_demand_extract(token, 
                      json_file_name,
                      instrument_list_file = '',
                      extraction_url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/Extract',
                      response_type = 'P',
                      csv_filename_resp_c = 'DSSfile.csv',
                      bulk_load = True):
        """This function pairs the token and sends a json file (use DSS reference Tree to generate JSON files) to the DSS REST servers.
        
        :token: this object is taken from get_token() function.
        :json_file_name: this is the json file, should be defined with path.
        :extraction_url: This is the url taken from the DSS reference tree as defined by the extraction type
        :response_type: This is a beta feature I'm exploring. 
            'P' = Pandas dataframe. Must have pandas imported and loaded.
            'C' = Raw CSV format. If this is selected, please define output filename in csv_filename_resp_c parameter.
            'J' = JSON, just returns a raw json format.
        
        :csv_filename_resp_c: CSV filename if option 'C' selected for response_type.
        
        Requires pandas, json, and requests packages.
        """

        import requests
        import json
        import pandas as pd
        import time
        
        print("||| Status: Loading instrument list.")
        def read_instruments(instrument_list_file = 'D:\\Python\\DSSInstuments.txt'):
            """The purpose of this function is to read in instruments from a text file with the following structure in the file:
                
                InstrumentType1, Instrument1
                InstrumentType2, Instrument2
            
            :instrument_list_file: Include path in the filename of not in working directory. """
            
            outfile = []
            try:
                file_to_process = with open(instrument_list_file,'r').read().split('\n')
                for line in file_to_process:
                    line_elements = line.split(',')
                    row = [line_elements[0],line_elements[1]]
                    outfile.append(row)
            except:
                print('.' * 20)
                print('File not loaded, confirm the file is formatted correctly.')
            return outfile
        
        
        def extraction_header(token):
            header={}
            header['Prefer']='respond-async, wait=5'
            header['Content-Type']='application/json; odata.metadata=minimal'
            header['Accept-Charset']='UTF-8'
            header['Authorization'] = token
            return header
            
        token_header = 'Token ' + token
        json_request_body = {}
        with open(json_file_name) as f:
            json_request_body=json.load(f)
    
        instruments = read_instruments()
    
        print('||| Status: Appending each instrument to the Instrument Identifiers array.' )
        if bulk_load:
            for inst in instruments:
                json_request_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append( { "IdentifierType": inst[0].strip(), "Identifier": inst[1].strip() } )
        else:
            json_request_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append( { "IdentifierType": input('IdentifierType: ').strip(), "Identifier": input('Identifier: ').strip() } )
        extract_request_header = extraction_header(token_header)
        try:
            print ('||| Status: Posting the JSON Request to DSS REST server and polling response status.')
            response = requests.post(extraction_url, 
                                     data=None, 
                                     json=json_request_body, 
                                     headers=extract_request_header)
            
            if response.status_code != 200: 
                #200 means the request has succeeded. i.e. ok.
                if response.status_code != 202: 
                    #202 means the request has been accepted for processing, but the processing has not been completed. 
                    ##The request might or might not eventually be acted upon, as it might be disallowed when processing 
                    ##actually takes place. There is no facility for re-sending a status code from an asynchronous operation 
                    ##such as this.
                    print("---Error: Status Code:" + str(response.status_code) + " The server is not returning a successful reponse of either 200 or 202. ---")
                print("||| Request message accepted. HTML Code received: " + str(response.status_code))
                location = response.headers['Location']
    
                # Pooling loop to check request status every 2 sec.
                while True:
                    response = requests.get(location, headers=extract_request_header )
                    poll_status = int(response.status_code)
    
                    if poll_status == 200:
                        break
                    else:
                        print("||| Status: ", response.headers['Status'])
                    time.sleep(2)
    
            print("||| Status: Response message successfully received.")
    
            # Process Reponse JSON object
            json_response = response.json()
    
            print ('||| Status: Extract the response message.')
            field_names = json_request_body["ExtractionRequest"]["ContentFieldNames"]
            header_string = "IdentifierType|Identifier"
            for i in range(len(field_names)):
                header_string += "|" + str(field_names[i])
    
    
            ric_exceptions = { 'RICException' :[] }
            for i in range(len(json_response["value"])):
                    if 'Error' in json_response["value"][i]:
                        ric_exceptions['RICException'].append( json_response["value"][i] )    
            if len(ric_exceptions['RICException']) > 0:
                print ('Search Exceptions:')
                for _rExcept in ric_exceptions['RICException']:
                    _str = _rExcept['IdentifierType'] + ' ' + _rExcept['Identifier'] + ": " + _rExcept['Error']
                    print(str(_str))
            else:
                print('--- No exceptions found in report ---')
    
            if response_type == 'P':
                return pd.DataFrame(json_response['value'])
    
            elif response_type == 'C':
                print('Needs an enhancement, working on it')
                    
            elif response_type == 'J':
                return json_response['value']
            else:
                print('--- No valid option returned for response_type parameter. ---')
                print('=' * 10)
                print(header_string)
                print('/n')
                for i in range(len(json_response["value"])):
                    output_data = json_response["value"][i]["IdentifierType"] + "|" + json_response["value"][i]["Identifier"]
                    for j in range(len( field_names )):
                        output_data += "|" + str( json_response["value"][i][field_names[j]] )
    
                print( output_data )
    
        except:
            print("--- Exception occured while polling DSS servers. ---")


    def pricing_single_historical(token, 
                      json_file_name = 'D:\\Python\\pricing_single_historical.json',
                      instrument_list_file = '',
                      extraction_url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/Extract',
                      response_type = 'P',
                      csv_filename_resp_c = 'DSSfile.csv',
                      bulk_load = False):
        """This function pairs the token and sends a json file (use DSS reference Tree to generate JSON files) to the DSS REST servers.
        
        :token: this object is taken from get_token() function.
        :json_file_name: this is the json file, should be defined with path.
        :extraction_url: This is the url taken from the DSS reference tree as defined by the extraction type
        :response_type: This is a beta feature I'm exploring. 
            'P' = Pandas dataframe. Must have pandas imported and loaded.
            'C' = Raw CSV format. If this is selected, please define output filename in csv_filename_resp_c parameter.
            'J' = JSON, just returns a raw json format.
        
        :csv_filename_resp_c: CSV filename if option 'C' selected for response_type.
        
        Requires pandas, json, and requests packages.
        """
            
        import requests
        import json
        import pandas as pd
        import time
        
        print("||| Status: Loading instrument list.")
        def read_instruments(instrument_list_file = 'D:\\Python\\DSSInstuments.txt'):
            """The purpose of this function is to read in instruments from a text file with the following structure in the file:
                
                InstrumentType1, Instrument1
                InstrumentType2, Instrument2
            
            :instrument_list_file: Include path in the filename of not in working directory. """
            
            outfile = []
            try:
                file_to_process = with open(instrument_list_file,'r').read().split('\n')
                for line in file_to_process:
                    line_elements = line.split(',')
                    row = [line_elements[0],line_elements[1]]
                    outfile.append(row)
            except:
                print('.' * 20)
                print('File not loaded, confirm the file is formatted correctly.')
            return outfile
        
        
        def extraction_header(token):
            header={}
            header['Prefer']='respond-async, wait=5'
            header['Content-Type']='application/json; odata.metadata=minimal'
            header['Accept-Charset']='UTF-8'
            header['Authorization'] = token
            return header
            
        token_header = 'Token ' + token
        json_request_body = {}
        with open(json_file_name) as f:
            json_request_body=json.load(f)
            
        print('||| Status: Appending each instrument to the Instrument Identifiers array.' )
        
        if bulk_load:
            instruments = read_instruments()
            for inst in instruments:
                json_request_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append( { "IdentifierType": inst[0].strip(), "Identifier": inst[1].strip() } )
        else:
            id_entry = input('Identifier: ')
            id_type = input('IdentifierType: ')
            json_request_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append( { "IdentifierType": id_type.strip(), "Identifier": id_entry.strip() } )
        json_request_body['ExtractionRequest']['Condition']['PriceDate'] = input("Enter Request Date (Format yyyy-mm-dd): ") + "T00:00:00.000Z"
        extract_request_header = extraction_header(token_header)
        try:
            print ('||| Status: Posting the JSON Request to DSS REST server and polling response status.')
            response = requests.post(extraction_url, 
                                     data=None, 
                                     json=json_request_body, 
                                     headers=extract_request_header)
            
            if response.status_code != 200: 
                #200 means the request has succeeded. i.e. ok.
                if response.status_code != 202: 
                    #202 means the request has been accepted for processing, but the processing has not been completed. 
                    ##The request might or might not eventually be acted upon, as it might be disallowed when processing 
                    ##actually takes place. There is no facility for re-sending a status code from an asynchronous operation 
                    ##such as this.
                    print("---Error: Status Code:" + str(response.status_code) + " The server is not returning a successful reponse of either 200 or 202. ---")
                print("||| Request message accepted. HTML Code received: " + str(response.status_code))
                location = response.headers['Location']
    
                # Pooling loop to check request status every 2 sec.
                while True:
                    response = requests.get(location, headers=extract_request_header )
                    poll_status = int(response.status_code)
    
                    if poll_status == 200:
                        break
                    else:
                        print("||| Status: ", response.headers['Status'])
                    time.sleep(2)
    
            print("||| Status: Response message successfully received.")
    
            # Process Reponse JSON object
            json_response = response.json()
    
            print ('||| Status: Extract the response message.')
            field_names = json_request_body["ExtractionRequest"]["ContentFieldNames"]
            header_string = "IdentifierType|Identifier"
            for i in range(len(field_names)):
                header_string += "|" + str(field_names[i])
    
    
            ric_exceptions = { 'RICException' :[] }
            for i in range(len(json_response["value"])):
                    if 'Error' in json_response["value"][i]:
                        ric_exceptions['RICException'].append( json_response["value"][i] )    
            if len(ric_exceptions['RICException']) > 0:
                print ('Search Exceptions:')
                for _rExcept in ric_exceptions['RICException']:
                    _str = _rExcept['IdentifierType'] + ' ' + _rExcept['Identifier'] + ": " + _rExcept['Error']
                    print(str(_str))
            else:
                print('--- No exceptions found in report ---')
    
            if response_type == 'P':
                return pd.DataFrame(json_response['value'])
    
            elif response_type == 'C':
                print('Needs an enhancement, working on it')
                    
            elif response_type == 'J':
                return json_response['value']
            else:
                print('--- No valid option returned for response_type parameter. ---')
                print('=' * 10)
                print(header_string)
                print('/n')
                for i in range(len(json_response["value"])):
                    output_data = json_response["value"][i]["IdentifierType"] + "|" + json_response["value"][i]["Identifier"]
                    for j in range(len( field_names )):
                        output_data += "|" + str( json_response["value"][i][field_names[j]] )
    
                print( output_data )
    
        except:
            print("--- Exception occured while polling DSS servers. ---")
