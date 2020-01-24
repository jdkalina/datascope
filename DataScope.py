import pandas as pd
import requests
import json
import time

class DataScope:

    def __init__(self, name, pw):
        self.name = name
        self.pw = pw
        self.authenticate()

    def preferences(self):
        """
        definition from the REST API tree: User preferences determine formatting, time zones, FTP settings, extraction and other settings.
        DevNotes: This is producing a json with two nodes: (odata, value), value produces four child nodes ('UserId', 'UserName','Email','Phone')
            Currently, this is set to return a json with all available data. Further versions would do well to neglect odata and return a dataframe of elements in 'value' node. This call does not produce much value other than to check contact info is up to date.
        """

        _url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Users/Users(" + self.name + ")/Preferences"

        _header={
            "Prefer":"respond-async",
            "Authorization": "Token " + self.token
        }

        return pd.DataFrame(json.loads(requests.get(_url, headers = _header).content))

    def rights(self):
        """
        definition from the REST API tree: Represents a right or attribute of the user.
        :output: Fee Liable codes, realtime perms, etc.
        """

        _url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Users/UserClaims"

        _header={
            "Prefer":"respond-async",
            "Authorization": "Token " + self.token
        }

        return pd.DataFrame(json.loads(requests.get(_url, headers = _header).content)['value'])


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


    def load_pd(self, dataframe, type_col, id_col, isTS = False, tsStart = None, tsEnd = None, validate = True, notesFile = ''):
        """
        This method loads instruments from a file similar to how its done within DSS from csv files. Column position 1 is used for Instrument Type, column position 2 is used for the instrument id.

        dataframe: PANDAS dataframe with the instruments to load.
        type_col: Character String indicating the name of the PANDAS DF column with the Instrument Types
        id_col: Character String indicating the name of the PANDAS DF column with the Instrument Identifiers
        """
        _data = dataframe

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
            self.instruments.append({"Identifier": v[id_col],"IdentifierType": v[type_col]})

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

    def pricing(self, template, fields, today_only = False):

        """This method provides access to standard pricing templates. Use the options in the template setting to select from available settings.
        :template:
                    options shown below:
                            "eod":"EndOfDayPricingExtractionRequest",
                            "intraday":"IntradayPricingExtractionRequest",
                            "peod":"PremiumEndOfDayPricingExtractionRequest",
                            "prem":"PremiumPricingExtractionRequest",
                            "hist":"PriceHistoryExtractionRequest"
        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        :today_only: an option that will return only todays prices for a report or a null value.
        """

        _tmChoices ={
            "eod":"EndOfDayPricingExtractionRequest",
            "intraday":"IntradayPricingExtractionRequest",
            "peod":"PremiumEndOfDayPricingExtractionRequest",
            "prem":"PremiumPricingExtractionRequest",
            "hist":"PriceHistoryExtractionRequest"
        }

        if not template in _tmChoices:
            print('Error, your template selection looks invalid')
            return

        _odata = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests." + _tmChoices[template]

        _header={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization": "Token " + self.token
        }

        _body={
            "ExtractionRequest": {
                "@odata.type": _odata,
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": []
                },
                "Condition": "null"
            }
        }

        if self.timeseries:
            _body["ExtractionRequest"]["Condition"] = {"ReportDateRangeType": "Range","QueryStartDate": self.start,"QueryEndDate": self.end}
        else:
            if today_only:
                _body["ExtractionRequest"]["Condition"] = {"LimitReportToTodaysData": "true"}
            else:
                _body["ExtractionRequest"]["Condition"] = {"LimitReportToTodaysData": "false"}

        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = self.instruments
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def reference(self, template, fields):
        """This method provides access to standard reference templates. Use the options in the template setting to select from available settings.
        :template:
                    options shown below:
                            "tnc": "TermsAndConditionsExtractionRequest",
                            "bs": "BondScheduleExtractionRequest"
                            "rg":"RatingsExtractionRequest",
                            "mbf":"MBSFactorHistoryExtractionRequest",
                            "trf":"TrancheFactorHistoryExtractionRequest",
                            "fa":"FundAllocationExtractionRequest"
                            "own": "OwnershipExtractionRequest"
                            "sym": "SymbolCrossReferenceExtractionRequest"
        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        """

        _tmChoices ={
            "tnc": "TermsAndConditionsExtractionRequest",
            "bs": "BondScheduleExtractionRequest",
            "rg":"RatingsExtractionRequest",
            "mbf":"MBSFactorHistoryExtractionRequest",
            "trf":"TrancheFactorHistoryExtractionRequest",
            "fa":"FundAllocationExtractionRequest",
            "own": "OwnershipExtractionRequest",
            "sym": "SymbolCrossReferenceExtractionRequest"
        }

        if not template in _tmChoices:
            print('Error, your template selection looks invalid')
            return

        _odata = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests." + _tmChoices[template]

        _header={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization": "Token " + self.token
        }

        _body={
            "ExtractionRequest": {
                "@odata.type": _odata,
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": []
                },
            }
        }

        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = self.instruments
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def export(self, file, note_file = ''):
        """
        :file: this is the filename and path for output file. Note, if the text is written ':memory:', then this function will return a tuple PANDAS dataframe (file, note_file) instead writing files to disk. Do the same with note_file.
        :note_file: this is the filename and path for notes file.
        """

        _resp = requests.post(self.requestUrl, json=self.requestBody, headers=self.requestHeader)
        self.status_code = _resp.status_code
        if self.status_code == 202:
            _url = _resp.headers["location"]

            _requestHeaders={
                "Prefer":"respond-async",
                "Content-Type":"application/json",
                "Authorization":"Token " + self.token
            }

            while (self.status_code == 202):
                time.sleep(30)
                _respJson = requests.get(_url,headers=_requestHeaders)
                self.status_code = _respJson.status_code

                if self.status_code != 200 :
                    print('ERROR: An error occurred. Try to run this cell again. If it fails, re-run the previous cell.')

                if self.status_code == 200:
                    if file == ':memory:':
                        return (pd.DataFrame(json.loads(_respJson.content)['Contents']), json.loads(_respJson.content)['Notes'][0])
                    else:
                        pd.DataFrame(json.loads(_respJson.content)['Contents']).to_csv(file)
                        _notes = json.loads(_respJson.content)['Notes'][0]
                        with open(note_file, 'w') as f:
                            for line in _notes:
                                print(line)
                                f.write(line)
                    print('Successfully downloaded file')
                else:
                    print(self.status_code, "- Issue raised")
        elif (self.status_code == 200):
            if file == ':memory:':
                return (pd.DataFrame(json.loads(_resp.content)['Contents']), pd.DataFrame(json.loads(_resp.content)['Notes']))
            else:
                pd.DataFrame(json.loads(_resp.content)['Contents']).to_csv(file)
                _notes = json.loads(_resp.content)['Notes'][0]
                with open(note_file, 'w') as f:
                    for line in _notes:
                        print(line)
                        f.write(line)
            print('Successfully downloaded file')
        else:
            print('Error, issue with the export file. HTTP Status: ',self.status_code)
