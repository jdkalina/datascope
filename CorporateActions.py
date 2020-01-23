# -*- coding: utf-8 -*-
"""

Corporate Actions
Created on Wed Jan 22 10:46:36 2020

@author: U6037148
"""

import pandas as pd
import requests
import json
import time

class DataScope:

    def __init__(self, name, pw):
        self.name = name
        self.pw = pw
        self.authenticate()
        
    def authenticate(self):
        """
        Authenticates and returns a token valid for 24 hours to be paired in subsequent json headers to DSS servers. Takes the name and pw defined directly to the Datascope object.
        """
        from requests import post
        from json import loads
        _header={
            "Prefer":"respond-async",
            "Content-Type":"application/json"
        }

        _body={"Credentials": {"Username": self.name,"Password": self.pw}}
        _auth = post("https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken",json=_body,headers=_header)

        if _auth.status_code != 200:
            print('issue with the token')
        else:
            self.token = loads(_auth.text.encode('ascii', 'ignore'))["value"]
            
    def get_fields(self, template):
        _url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/GetValidExtractionFieldNames(ReportTemplateType=ThomsonReuters.Dss.Api.Extractions.ReportTemplates.ReportTemplateTypes'CorporateActions')"
        _header={
                "Prefer":"respond-async",
                "Authorization": "Token " + self.token
        }
        
        return json.loads(requests.get(_url, headers = _header).content)    
            
    def load_csv(self, filename, isTS = False, tsStart = None, tsEnd = None, validate = True, notesFile = ''):
        """
        This method loads instruments from a file similar to how its done within DSS from csv files. Column position 1 is used for Instrument Type, column position 2 is used for the instrument id.
        
        filename: file and path to the file you are loading. 
        """
        _data = pd.read_csv(filename, header = None)
    
        _corrections = {"CSP":"Cusip","ISN":"Isin","RIC":"Ric","CHR":"ChainRic","SED":"Sedol"}    
                
        for k,v in _corrections.items():
            if sum(_data.iloc[:,0].isin([k])):
                _data.iloc[:,0] = _data.iloc[:,0].str.replace(k, v)
        
        self.start = tsStart
        self.end = tsEnd
        self.timeseries = isTS
        self.odataIns = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList"
        
        self.instruments = []
        for i,v in _data.iterrows():
            self.instruments.append({"Identifier": v[1],"IdentifierType": v[0]})
        
        if self.timeseries:
            if tsStart == None:
                print('For timeseries == True, please define a date in tsStart and tsEnd')
                return
            else:
                print('Note, timeseries will only work with Intraday and Price History templates')
                
        if validate:
            _url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/InstrumentListValidateIdentifiers"
            _header={
                "Prefer":"respond-async",
                "Content-Type":"application/json",
                "Authorization": "Token " + self.token
            }
            _body = {
                "InputsForValidation": [],
                "KeepDuplicates": "true"
            }
            _body["InputsForValidation"] = self.instruments
            _resp = requests.post(_url, headers = _header, json = _body)
            _resp = json.loads(_resp.content)
            
            self.valid_inst = pd.DataFrame(_resp["ValidatedInstruments"])
            
            for k,v in _resp['ValidationResult'].items():
                print(k," - ",v)
            if notesFile == '':
                print("No file path and name selected to write the results of the Validated Instruments")
            else:
                self.valInst.to_csv(notesFile)

        #_body["ExtractionRequest"]["ContentFieldNames"] = fields
        #for i in self.rics:
        #    _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
        #self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        #self.requestBody = _body
        #self.requestHeader = _header
    
    def corporate_actions(self, CorporateActionsCapitalChangeType, CorporateActionsDividendsType, CorporateActionsEarningsType, 
                          CorporateActionsEquityOfferingsType, CorporateActionsMergersAcquisitionsType, 
                          CorporateActionsNominalValueType, CorporateActionsSharesType, CorporateActionsStandardEventsType, 
                          CorporateActionsVotingRightsType, ShareAmountChoice, ShareAmountTypes,
                          IncludeInstrumentsWithNoEvents = True, IncludeNullDates = True, ExcludeDeletedEvents = True, 
                          IncludeCapitalChangeEvents= True, IncludeDividendEvents = True, IncludeEarningsEvents = True, IncludeMergersAndAcquisitionsEvents = True,
                          IncludeNominalValueEvents = True, IncludePublicEquityOfferingsEvents = True, IncludeSharesOutstandingEvents = True,
                          IncludeVotingRightsEvents = True, ):
        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"
            
        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [
                    "Accounting Standard"
                ],
                "IdentifierList": {
                    "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentCriteriaList",
                    "Filters": [
                        {
                            "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.SubjectLists.BooleanFilter",
                            "Value": "All"
                        }
                    ],
                    "PreferredIdentifierType": "ArgentineAfipCode"
                },
                "Condition": {
                    "ReportDateRangeType": "Delta",
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeCapitalChangeEvents": iftrue(IncludeCapitalChangeEvents),
                    "IncludeDividendEvents": iftrue(IncludeDividendEvents),
                    "IncludeEarningsEvents": iftrue(IncludeEarningsEvents),
                    "IncludeMergersAndAcquisitionsEvents": iftrue(IncludeMergersAndAcquisitionsEvents),
                    "IncludeNominalValueEvents": iftrue(IncludeNominalValueEvents),
                    "IncludePublicEquityOfferingsEvents": iftrue(IncludePublicEquityOfferingsEvents),
                    "IncludeSharesOutstandingEvents": iftrue(IncludeSharesOutstandingEvents),
                    "IncludeVotingRightsEvents": iftrue(IncludeVotingRightsEvents),
                    "CorporateActionsCapitalChangeType": CorporateActionsCapitalChangeType,
                    "CorporateActionsDividendsType": CorporateActionsDividendsType,
                    "CorporateActionsEarningsType": CorporateActionsEarningsType,
                    "CorporateActionsEquityOfferingsType": CorporateActionsEquityOfferingsType,
                    "CorporateActionsMergersAcquisitionsType": CorporateActionsMergersAcquisitionsType,
                    "CorporateActionsNominalValueType": CorporateActionsNominalValueType,
                    "CorporateActionsSharesType": CorporateActionsSharesType,
                    "CorporateActionsStandardEventsType": CorporateActionsStandardEventsType,
                    "CorporateActionsVotingRightsType": CorporateActionsVotingRightsType,
                    "ShareAmountChoice": ShareAmountChoice,
                    "ShareAmountTypes": []
                }
            }
        }
                
        return _body
                


dss = DataScope("9009214","3R7re5NW")
#dss.preferences()
fields = dss.get_fields('CorporateActions')

