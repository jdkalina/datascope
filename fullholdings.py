#464287309, example cusip from iShares S&P500 Growth Fund

import json
import requests
import pandas as pd

def pw():
	"""Collapse and hide pw for demos"""
	return ''

def get_token(username,password, url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'):
	header= {}
	header['Prefer']='respond-async'
	header['Content-Type']='application/json; odata.metadata=minimal'
	data={'Credentials':{
		'Password':password,
		'Username':username
		}
	}


def ind_instrument_selection(dss_json,identifier, indentifier_type, source = "Default"):
	"""This function is used to return individual assets. Note this is a nonoptimal way to use DSS and shouldn't be used to bulk load databases. Best for test purposes and heavy-response templates like Full Holdings.

	:dss_json: A json object that will be manipulated to append an individual identifier
	:identifier: The alphanumeric identifier number. This should be in string format. Eg: "464287309"
	:identifier_type: The type of identifier used in 'identifier'. i.e. Cusip, Ric, Isin, etc. Refer to REST API Reference tree.
	:source: This is the enumerated source of the instrument. Note, not to be used with Rics. Default will choose the best source based on Original Place of Listing and Volume.
	"""
	dss_json['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'][0]['IdentifierType'] = indentifier_type
	dss_json['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'][0]['Identifier'] = identifier
	if source == "Default":
		dss_json['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'][0]['Source'] = ""
	else:
		dss_json['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'][0]['Source'] = source

def extraction_header(token):
	header={}
	header['Prefer']='respond-async, wait=5'
	header['Content-Type']='application/json; odata.metadata=minimal'
	header['Accept-Charset']='UTF-8'
	header['Authorization'] = token
	return header

def async(json_response):
	"""This code is meant to test async response on server and return a json. 
	"""
	if json_response.status_code != 200:
		if json_response.status_code != 202:
			print("---Error: Status Code:" + str(json_response.status_code) + " The server is not returning a successful reponse of either 200 or 202. ---")
		print("||| Request message accepted. HTML Code received: " + str(json_response.status_code))
		location = json_response.headers['Location']
		while True:
			response = requests.get(location, headers=extract_request_header )
			poll_status = int(response.status_code)
			if poll_status == 200:
				break
			else:
				print("||| Status: ", response.headers['Status'])
			time.sleep(2)
	return response.json()

class fund_allocation:
	def full(token, identifier = "SPY", indentifier_type = "Ric", source = "LIP",single_asset = True, fields = 'Standard'):
		"""This function is intended to request constituent level data from Datascope Select. This function is made available by Lipper content. If you need history of constituent level data, that will only exist through Lipper itself.

		:token: This is the token taken from get_token function.
		:identifier: This is the alphanumeric identifier used to identify the mutual fund or ETF. Note for this call, you need to source the LIPPER RIC to return any values. Either pass lipper RICs in this field or use the CUSIP/ISIN/SEDOL with "LIP" source.
		:identifier: The alphanumeric identifier number. This should be in string format. Eg: "464287309"
		:identifier_type: The type of identifier used in 'identifier'. i.e. Cusip, Ric, Isin, etc. Refer to REST API Reference tree.
		:source: This is the enumerated source of the instrument. Note, not to be used with Rics. Default will choose the best source based on Original Place of Listing and Volume. Set to Lipper by default.
		:single_asset: If you want to test or return data for an individual asset, keep this as True, otherwise select False and provide directory info for a txt file that has "identifier, indentifier_type, source" data with no headers, separated by commas.
		:fields: provide a list of field names, i.e. ["Asset SubType Description","Asset Type",] Note that the call will fail if there are any incorrectly spelled items in this list.
		"""

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
		token_header = 'Token ' + token

		extract_request_header = extraction_header(token_header)

		#Instrument Selection
		if single_asset:
			ind_instrument_selection(json_body, identifier, indentifier_type, source)
		else:
			print('Pardon Our Dust! In progress!')

		json_request_body = json_body
		if fields != "Standard":
		   json_request_body['ExtractionRequest']["ContentFieldNames"] = fields
	

		response = requests.post(extraction_url, data=None, json=json_request_body, headers=extract_request_header)
		return_json = async(response)
		return return_json


token = get_token('9009214',pw())
return_json = fund_allocation_full('464287309','Cusip')
pd.DataFrame(return_json['Contents'])
