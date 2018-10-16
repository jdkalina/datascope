def get_token(username,password, url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'):
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

def dss_full_holdings(dsid, passw, identifier = "464287309", indentifier_type = "Cusip", source = "LIP", single_asset = True, fields = 'Standard', filename='DSSInstrumentsForMutualFunds.csv'):
    """
    dss_full_holdings is intended to pull current holdings and weighting information for ETFs and Mutual Funds from the Datascope Select API.
    
    :dsid: this is your Datascope Select user id that is provided to you when subscribing to a use and extract license with Refinitiv (Formerly Thomson Reuters)
    :passw: the password for the id in dsid.
    :identifier: if single_asset is True (default), then you are electing to look up info on one individual asset. Best if just viewing the data.
    :indentifier_type: This defines what kind of instrument is defined in :identifier:. Most common are 'Cusip', 'Isin', 'Ric', or 'Sedol'.
    :source: Note, that the data in DSS is coming from our Lipper database and will require the source to be defined as 'LIP'. If you are using a Ric, this is a quote level 

    """

    import json
    import requests
    import pandas as pd
    import time

    raw_json = """
        {
          "ExtractionRequest": {
				"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.FundAllocationExtractionRequest",
				"ContentFieldNames": [
					"Asset SubType Description",
					"Asset Type",
					"Asset Type Description",
					"CUSIP",
					"Identifier Segment Source",
					"Identifier Segment Source Description",
					"Instrument ID",
					"Instrument ID Type",
					"Security Description",
					"Allocation Asset Type",
					"Allocation CUSIP",
					"Allocation Date",
					"Allocation Item",
					"Allocation OrgID",
					"Allocation Percentage",
					"Allocation Rank",
					"Market Value Currency",
					"Market Value Held",
					"RIC",
					"FundSERVAC",
					"FundSERVBE",
					"FundSERVDM",
					"FundSERVDO",
					"FundSERVFE",
					"FundSERVIS",
					"FundSERVLL",
					"FundSERVNL",
					"FundSERVVS"
				],
				"IdentifierList": {
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
					"InstrumentIdentifiers": [
						{
							"Identifier": "123456789",
							"IdentifierType": "Cusip",
							"Source": "LIP"
						}
					],
					"ValidationOptions": null,
					"UseUserPreferencesForValidationOptions": false
				},
				"Condition": {
					"FundAllocationTypes": [
						"Currency",
						"FullHoldings"
					]
				}
			}
		}
		"""
    json_body = json.loads(raw_json)
    extraction_url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
    token = get_token(dsid,passw)

    extract_request_header={}
    extract_request_header['Prefer']='respond-async, wait=5'
    extract_request_header['Content-Type']='application/json; odata.metadata=minimal'
    extract_request_header['Accept-Charset']='UTF-8'
    extract_request_header['Authorization'] = 'Token ' + token
    
    instrumentList = pd.read_csv(filename,header = None)
    for v,i in instrumentList.iterrows():
        print(instrumentList[v])
    
    if fields != "Standard":
        json_body['ExtractionRequest']["ContentFieldNames"] = fields
        
    response = requests.post(extraction_url, data=None, json=json_body, headers=extract_request_header)
    if response.status_code != 200: 
        #200 means the request has succeeded. i.e. ok.
        if response.status_code != 202: 
            #202 means the request has been accepted for processing, but the processing has not been completed. 
            ##The request might or might not eventually be acted upon, as it might be disallowed when processing 
            ##actually takes place. There is no facility for re-sending a status code from an asynchronous operation 
            ##such as this.
            print("---Error: Status Code:" + str(response.status_code) + " The server is not returning a successful reponse of either 200 or 202. ---")
        print("||| Request message accepted. HTML Code received: " + str(response.status_code))
        try:
            location = response.headers['Location']
        except:
            print("Failed here at location object")
        
        # Pooling loop to check request status every 2 sec.
        while True:
            response = requests.get(location, headers=extract_request_header )
            poll_status = int(response.status_code)

            if poll_status == 200:
                break
            else:
                print("||| Status: ", response.headers['Status'])
            time.sleep(2)
    json_response = response.json()
    return pd.DataFrame(json_response['Contents'])


dss_full_holdings(dsid = '', 
                  passw = '', 
                  fields = 'Standard', 
                  filename='DSSInstrumentsForMutualFunds.csv')