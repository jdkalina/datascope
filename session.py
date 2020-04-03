import pandas as pd
import requests
import json
import time

class session:

    def __init__(self, name, pw):
        self.name = name
        self.pw = pw
        self.authenticate()
        self.validation_options = {
            "AllowHistoricalInstruments": "true",
            "AllowInactiveInstruments": "true",
            "AllowOpenAccessInstruments": "true"
        }

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

    def get_fields(self, template):
        _url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/GetValidExtractionFieldNames(ReportTemplateType=ThomsonReuters.Dss.Api.Extractions.ReportTemplates.ReportTemplateTypes'CorporateActions')"
        _header={
            "Prefer":"respond-async",
            "Authorization": "Token " + self.token
        }

        return json.loads(requests.get(_url, headers = _header).content)

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

    def load_pd(self, dataframe, type_col = 'default', id_col = 'default', validate = True):
        """
        This method loads instruments from a file similar to how its done within DSS from csv files. Column position 1 is used for Instrument Type, column position 2 is used for the instrument id.
        dataframe: PANDAS dataframe with the instruments to load.
        type_col: Character String indicating the name of the PANDAS DF column with the Instrument Types
        id_col: Character String indicating the name of the PANDAS DF column with the Instrument Identifiers
        """
        self.instruments = []
        def inst_append(df):
            _instruments = []
            for i,v in df.iterrows():
                if len(df.columns) == 2:
                    _instruments.append({"Identifier": v[1],"IdentifierType": v[0]})
                else:
                    if v[2] is not None:
                        _instruments.append({"Identifier": v[1],"IdentifierType": v[0], "Source":v[2]})
                    else:
                        _instruments.append({"Identifier": v[1],"IdentifierType": v[0]})
            return _instruments

        _data = dataframe

        if type_col == 'default':
            type_col = 0
        if id_col == 'default':
            id_col =1

        _corrections = {"CSP":"Cusip","ISN":"Isin","RIC":"Ric","CHR":"ChainRic","SED":"Sedol","CIN":"Cin"}

        for k,v in _corrections.items():
            if sum(_data.iloc[:,0].isin([k])):
                _data.iloc[:,0] = _data.iloc[:,0].str.replace(k, v)

        self.odataIns = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList"

        _inst = inst_append(_data)

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
            _body["InputsForValidation"] = _inst
            _resp = requests.post(_url, headers = _header, json = _body)
            _resp = json.loads(_resp.content)

            try:
                self.valid_inst = pd.DataFrame(_resp["ValidatedInstruments"])
                self.instruments = inst_append(self.valid_inst[self.valid_inst['Status'] == 'Valid'][['IdentifierType','Identifier','Source']])
            except:
                print(_resp)

            for k,v in _resp['ValidationResult'].items():
                if k == 'StandardSegments':
                    print(k)
                    print(pd.DataFrame(v),'\n\n')
                elif v == []:
                    continue
                else:
                    if type(v) is list:
                        print(k)
                        for i in v:
                            print(i)
                        print('\n')
                    else:
                        print(k," - ",v,'\n')
        else:
            self.instruments = inst_append(_data)

    def load_csv(self, filename, validate = True):
        """
        This method loads instruments from a file similar to how its done within DSS from csv files. Column position 1 is used for Instrument Type, column position 2 is used for the instrument id.
        filename: file and path to the file you are loading.
        """
        self.instruments = []
        def inst_append(df):
            _instruments = []
            for i,v in df.iterrows():
                if len(df.columns) == 2:
                    _instruments.append({"Identifier": v[1],"IdentifierType": v[0]})
                else:
                    if v[3] is not None:
                        _instruments.append({"Identifier": v[1],"IdentifierType": v[0], "Source":v[3]})
                    else:
                        _instruments.append({"Identifier": v[1],"IdentifierType": v[0]})
            return _instruments

        instrument_list = []
        with open(filename, 'r') as file:
            for line in file:
                row = line.split(',')
                row[-1] = row[-1].replace('\n','')
                instrument_list.append(row)

        _data = pd.DataFrame(instrument_list)

        _corrections = {"CSP":"Cusip","ISN":"Isin","RIC":"Ric","CHR":"ChainRic","SED":"Sedol","CIN":"Cin"}

        for k,v in _corrections.items():
            if sum(_data.iloc[:,0].isin([k])):
                _data.iloc[:,0] = _data.iloc[:,0].str.replace(k, v)

        self.odataIns = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList"

        _inst = inst_append(_data)

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
            _body["InputsForValidation"] = _inst
            _resp = requests.post(_url, headers = _header, json = _body)
            _resp = json.loads(_resp.content)

            try:
                self.valid_inst = pd.DataFrame(_resp["ValidatedInstruments"])
                _vi = self.valid_inst.copy()
                _vi['blank'] = ''
                self.instruments = inst_append(_vi[_vi['Status'] == 'Valid'][['IdentifierType','Identifier','blank','Source']])
            except:
                print(_resp)

            for k,v in _resp['ValidationResult'].items():
                if k == 'StandardSegments':
                    print(k)
                    print(pd.DataFrame(v),'\n')
                elif v == []:
                    continue
                else:
                    if type(v) is list:
                        print(k)
                        for i in v:
                            print(i)
                        print('\n')
                    else:
                        print(k," - ",v,'\n')
        else:
            self.instruments = inst_append(_data)

    def composite(self, fields):

        """This method provides access to the composite template. Use the options in the template setting to select from available settings.
        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        :today_only: an option that will return only todays prices for a report or a null value.
        NOTES: add source as an input for pricing.
        """

        _odata = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CompositeExtractionRequest"

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
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                }
            }
        }


        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = self.instruments
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def price(self, template, fields, today_only = False):

        """This method provides access to standard pricing templates. Use the options in the template setting to select from available settings.
        :template:
                    options shown below:
                            "eod":"EndOfDayPricingExtractionRequest",
                            "peod":"PremiumEndOfDayPricingExtractionRequest",
                            "prem":"PremiumPricingExtractionRequest"
        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        :today_only: an option that will return only todays prices for a report or a null value.
        NOTES: add source as an input for pricing.
        """

        _tmChoices ={
            "eod":"EndOfDayPricingExtractionRequest",
            "peod":"PremiumEndOfDayPricingExtractionRequest",
            "prem":"PremiumPricingExtractionRequest"
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
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": "null"
            }
        }

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

    def price_history(self, fields, rangeStart, rangeEnd):

        """This method provides access to standard pricing templates. Use the options in the template setting to select from available settings.

        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        """

        _header={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization": "Token " + self.token
        }

        _body={
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.PriceHistoryExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": []
                },
                "Condition": "null"
            }
        }

        _body["ExtractionRequest"]["Condition"] = {"QueryStartDate": rangeStart,"QueryEndDate": rangeEnd}

        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = self.instruments
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def price_intraday(self, fields):

        """This method provides access to the intraday pricing template. Use the options in the template setting to select from available settings.

        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        """

        _header={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization": "Token " + self.token
        }

        _body={
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.IntradayPricingExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": []
                }
            }
        }

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
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
            }
        }

        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = self.instruments
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def ref_bond_schedule(self, bond_schedule_type, fields):
        """This method provides access to standard reference templates. Use the options in the template setting to select from available settings.
        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        :bond_schedule_type: If you are leveraging the bond_schedule templates, you'll need to use the get_bond_sched_types method to get the bond types. Example is 'CALL' for Call schedule.
        """

        _odata = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.BondScheduleExtractionRequest"

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
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition":
                    {
                        "BondScheduleTypeCodes": [bond_schedule_type]
                    }
            }
        }

        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = self.instruments
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def corax_cap_change(self, rangeStart, rangeEnd, fields, CorporateActionsCapitalChangeType = "ann", IncludeNullDates = True, ExcludeDeletedEvents = True, IncludeInstrumentsWithNoEvents = False):
        """
        Two identifiers included in return object:
            * RIC
            * Issue Level Event ID
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        CorporateActionsCapitalChangeType: {"ann": "CapitalChangeAnnouncementDate",
                                       "dld": "CapitalChangeDealDate",
                                       "exd": "CapitalChangeExDate",
                                       "eff": "EffectiveDate",
                                       "rec": "RecordDate"}
                                    definition:
                                        Coverage includes Year to date and interim results, as well as As-reported and annualized figures. You can retrieve the data by Announcement Date and Period End Date.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _templates = {"ann": "CapitalChangeAnnouncementDate",
                      "dld": "CapitalChangeDealDate",
                      "exd": "CapitalChangeExDate",
                      "eff": "EffectiveDate",
                      "rec": "RecordDate"}

        # if not self.validate_template(CorporateActionsCapitalChangeType, _templates):
        #     return

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": ['RIC','Issue Level Event ID'],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeDividendEvents": "false",
                    "IncludeCapitalChangeEvents": "true",
                    "IncludeEarningsEvents": "false",
                    "IncludeMergersAndAcquisitionsEvents": "false",
                    "IncludeNominalValueEvents": "false",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "false",
                    "IncludeVotingRightsEvents": "false",
                    "CorporateActionsCapitalChangeType": _templates[CorporateActionsCapitalChangeType]
                }
            }
        }

        if type(fields) is str:
            fields = [fields]

        # if not self.validate_fields("CorporateActions",fields):
        #     return

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def corax_earnings(self, rangeStart, rangeEnd, fields, CorporateActionsEarningsType="ead", IncludeNullDates = True, ExcludeDeletedEvents = True, IncludeInstrumentsWithNoEvents = True):
        """
        The three identifiers:
            * Issue Level Event ID
            * Offer ID
            * Deal ID
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        CorporateActionsEarningsType: {"ead": "EarningsAnnouncementDate",
                                        "ped":"PeriodEndDate"}
                                    definition:
                                        Coverage includes Year to date and interim results, as well as As-reported and annualized figures. You can retrieve the data by Announcement Date and Period End Date.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _templates = {"ead": "EarningsAnnouncementDate",
                      "ped":"PeriodEndDate"}

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeDividendEvents": "false",
                    "IncludeCapitalChangeEvents": "false",
                    "IncludeEarningsEvents": "true",
                    "IncludeMergersAndAcquisitionsEvents": "false",
                    "IncludeNominalValueEvents": "false",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "false",
                    "IncludeVotingRightsEvents": "false",
                    "CorporateActionsEarningsType": _templates[CorporateActionsEarningsType]
                }
            }
        }

        if type(fields) is str:
            fields = [fields]

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def corax_nominal_value(self, rangeStart, rangeEnd, fields, IncludeNullDates = True, ExcludeDeletedEvents = True, IncludeInstrumentsWithNoEvents = True):
        """
        The three identifiers:
            * Issue Level Event ID
            * Offer ID
            * Deal ID
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        CorporateActionsEarningsType: {"ead": "EarningsAnnouncementDate",
                                        "ped":"PeriodEndDate"}
                                    definition:
                                        Coverage includes Year to date and interim results, as well as As-reported and annualized figures. You can retrieve the data by Announcement Date and Period End Date.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeDividendEvents": "false",
                    "IncludeCapitalChangeEvents": "false",
                    "IncludeEarningsEvents": "false",
                    "IncludeMergersAndAcquisitionsEvents": "false",
                    "IncludeNominalValueEvents": "true",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "false",
                    "IncludeVotingRightsEvents": "false"
                }
            }
        }

        if type(fields) is str:
            fields = [fields]

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers


    def corax_shares_outstanding(self, rangeStart, rangeEnd, fields, ShareAmountTypes='Issued', IncludeNullDates = True, ExcludeDeletedEvents = True, IncludeInstrumentsWithNoEvents = True):
        """
        The three identifiers:
            * Issue Level Event ID
            * Offer ID
            * Deal ID
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        ShareAmountTypes: either a character string value or a list.
                    Acceptable inputs:
                        ['Authorised','CloselyHeld','FreeFloat','Issued','Listed','Outstanding','Treasure','Unclassified']
        CorporateActionsSharesType: Coverage includes the default share amount type and/or multiple types, including Outstanding, Issued, Listed, Closely Held, Treasury, Authorized and Unclassified. You can retrieve the number of shares by Shares Amount Date. Only one option = SharesAmountDate.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """
        shareAmtTypes = ['Authorised','CloselyHeld','FreeFloat','Issued','Listed','Outstanding','Treasure','Unclassified']

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeDividendEvents": "false",
                    "IncludeCapitalChangeEvents": "false",
                    "IncludeEarningsEvents": "false",
                    "IncludeMergersAndAcquisitionsEvents": "false",
                    "IncludeNominalValueEvents": "false",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "true",
                    "IncludeVotingRightsEvents": "false",
                    "CorporateActionsSharesType": "SharesAmountDate",
                    "ShareAmountTypes": []
                }
            }
        }

        if type(ShareAmountTypes) is str:
            ShareAmountTypes = [ShareAmountTypes]

        _body['ExtractionRequest']['Condition']['ShareAmountTypes'] = ShareAmountTypes

        if type(fields) is str:
            fields = [fields]

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def corax_dividend(self, rangeStart, rangeEnd, fields, CorporateActionsDividendsType = "ann", IncludeNullDates = True, ExcludeDeletedEvents = True, IncludeInstrumentsWithNoEvents = False):
        """
        1. Exclude deleted events
        2. Let's make these only range queries for now. Dates are easily manipulated in python.
        3. query start  and end should have an if then statement for ':all:' that indicates all historical, future events.
        The three identifiers:
            * Issue Level Event ID
            * Offer ID
            * Deal ID
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        CorporateActionsDividendsType: {"ann": "DividendAnnouncementDate",
                                        "exd":"DividendExDate",
                                        "pay":"DividendPayDate",
                                        "rec":"DividendRecordDate",
                                        "end":"PeriodEndDate"}
                                    definition:
                                        Coverage comprises Regular, Special and Extraordinary Distributions (including cash dividends, dividend reinvestments, dividends with stock options, capital gains payments as well as stock dividends) with relevant tax details, such as withholding tax, QDI, franked rates.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _templates = {"ann": "DividendAnnouncementDate",
                      "exd":"DividendExDate",
                      "pay":"DividendPayDate",
                      "rec":"DividendRecordDate",
                      "end":"PeriodEndDate"}

        # if not self.validate_template(CorporateActionsDividendsType, _templates): return

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeDividendEvents": "true",
                    "CorporateActionsDividendsType": _templates[CorporateActionsDividendsType],
                    "IncludeCapitalChangeEvents": "false",
                    "IncludeEarningsEvents": "false",
                    "IncludeMergersAndAcquisitionsEvents": "false",
                    "IncludeNominalValueEvents": "false",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "false",
                    "IncludeVotingRightsEvents": "false"
                }
            }
        }

        # if type(fields) is str:
        #     fields = [fields]
        #
        # if not self.validate_fields("CorporateActions",fields):
        #     return

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def corax_mna(self,rangeStart,rangeEnd, fields, CorporateActionsMergersAcquisitionsType = "ann", IncludeNullDates=True, ExcludeDeletedEvents=True,IncludeInstrumentsWithNoEvents=False):
        """
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        CorporateActionsMergersAcquisitionsType:   {"ann": "DealAnnouncementDate",
                                                    "can":"DealCancelDate",
                                                    "cls":"DealCloseDate",
                                                    "eff":"DealEffectiveDate",
                                                    "rev":"DealRevisedProposalDate",
                                                    "exp":"TenderOfferExpirationDate"}
                                    definition:
                                        Coverage includes Year to date and interim results, as well as As-reported and annualized figures. You can retrieve the data by Announcement Date and Period End Date.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """

        _templates = {"ann": "DealAnnouncementDate",
                      "can":"DealCancelDate",
                      "cls":"DealCloseDate",
                      "eff":"DealEffectiveDate",
                      "rev":"DealRevisedProposalDate",
                      "exp":"TenderOfferExpirationDate"}
        #
        # if not self.validate_template(CorporateActionsMergersAcquisitionsType, _templates):
        #     return

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeDividendEvents": "false",
                    "IncludeCapitalChangeEvents": "false",
                    "IncludeEarningsEvents": "false",
                    "IncludeMergersAndAcquisitionsEvents": "true",
                    "IncludeNominalValueEvents": "false",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "false",
                    "IncludeVotingRightsEvents": "false",
                    "CorporateActionsMergersAcquisitionsType":_templates[CorporateActionsMergersAcquisitionsType]
                }
            }
        }

        if type(fields) is str:
            fields = [fields]

        # if not self.validate_fields("CorporateActions",fields):
        #     return

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def historical_reference(self, rangeStart, rangeEnd, fields):
        """
        On Demand Histo Reference extraction.
        """
        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.HistoricalReferenceExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                    "InstrumentIdentifiers": self.instruments,
                    "ValidationOptions": {"AllowHistoricalInstruments": "true"},
                    "UseUserPreferencesForValidationOptions": "false"
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd
                }
            }
        }

        if type(fields) is str:
            fields = [fields]

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def corax_peo(self, rangeStart, rangeEnd, fields, CorporateActionsEquityOfferingsType = "all", IncludeNullDates = True, ExcludeDeletedEvents = True, IncludeInstrumentsWithNoEvents = True):
        """
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        CorporateActionsEquityOfferingsType:   {"all": "AllPendingDeals",
                                                "1st": "FirstTradingDate"}
                                    definition:
                                        Coverage comprises IPO data. You can retrieve the data for All Pending Deals or by First Trading Date.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """

        _templates = {"all": "AllPendingDeals",
                      "1st": "FirstTradingDate"}

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeDividendEvents": "false",
                    "IncludeCapitalChangeEvents": "false",
                    "IncludeEarningsEvents": "false",
                    "IncludeMergersAndAcquisitionsEvents": "true",
                    "IncludeNominalValueEvents": "false",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "false",
                    "IncludeVotingRightsEvents": "false",
                    "CorporateActionsEquityOfferingsType":_templates[CorporateActionsEquityOfferingsType]
                }
            }
        }

        if type(fields) is str:
            fields = [fields]

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def corax_voting_rights(self, rangeStart, rangeEnd, fields, IncludeNullDates = True, ExcludeDeletedEvents = True, IncludeInstrumentsWithNoEvents = True):
        """
        rangeStart: character string input. This is the start date to query. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        rangeEnd: character string input. Range end date, can be a future date. Format is "YYYY-MM-DD" or in pythonic format: "%Y-%m-%d"
        fields: list of fields. character can be used for one value.
        IncludeNullDates: Defaults True. Takes boolean values. Only applies to 'All Historical Events' query.
        ExcludeDeletedEvents: Defaults True. Takes boolean values. With this option, only currently valid records are included in the extraction.
        IncludeInstrumentsWithNoEvents: Defaults True. Takes boolean values. Option to include instruments with no event data for all standard events.
        """

        def iftrue(obj):
            if obj:
                return "true"
            else:
                return "false"

        _headers={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + self.token
        }

        _body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": self.odataIns,
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": self.validation_options
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": rangeStart,
                    "QueryEndDate": rangeEnd,
                    "IncludeInstrumentsWithNoEvents": iftrue(IncludeInstrumentsWithNoEvents),
                    "IncludeNullDates": iftrue(IncludeNullDates),
                    "ExcludeDeletedEvents": iftrue(ExcludeDeletedEvents),
                    "IncludeDividendEvents": "false",
                    "IncludeCapitalChangeEvents": "false",
                    "IncludeEarningsEvents": "false",
                    "IncludeMergersAndAcquisitionsEvents": "true",
                    "IncludeNominalValueEvents": "false",
                    "IncludePublicEquityOfferingsEvents": "false",
                    "IncludeSharesOutstandingEvents": "false",
                    "IncludeVotingRightsEvents": "false",
                    "CorporateActionsVotingRightsType": "VotingRightsDate"
                }
            }
        }

        if type(fields) is str:
            fields = [fields]

        for i in fields:
            _body['ExtractionRequest']['ContentFieldNames'].append(i)

        _body['ExtractionRequest']['IdentifierList']['InstrumentIdentifiers'] = self.instruments

        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _headers

    def extract(self):
        """
        """
        try:
            self.instruments
        except:
            print('Error: missing instruments. You need to add instruments with one of the datascope.load_xxx methods')
            return

        try:
            self.requestBody
        except:
            print('Error: missing report template. You need to add reports with one of the datascope.pricing/corax/reference methods')
            return

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

                if self.status_code == 200:
                    self.content = pd.DataFrame(json.loads(_respJson.content)['Contents'])
                    self.notes = json.loads(_respJson.content)['Notes'][0]
                    self.ricmaintenance = json.loads(_respJson.content)['Notes'][1]
                    print('Completed: added to self.content')

        elif (self.status_code == 200):
            self.content = pd.DataFrame(json.loads(_resp.content)['Contents'])
            self.notes = json.loads(_resp.content)['Notes'][0]
            self.ricmaintenance = json.loads(_resp.content)['Notes'][1]
            print('Completed: added to self.content')

        else:
            print('Error, issue with the export file. HTTP Status: ',self.status_code)

    def set_validation_options(self, settings):
        """settings= the dictionary with json settings to be set in the "ValidationOptions portion for dss"
        """
        self.validation_options = json

    def validate_fields(self, template, fields):
        _chkFields = self.get_fields(template)
        if type(fields) is str:
            fields = [fields]
        for i in fields:
            if not i in _chkFields:
                print('ERROR: Check field selection, your fields are not available for ',template)
                return False
                break

    def validate_template(self, var, templates):
        if not var in templates:
            print("ERROR: Issue with the template selected, review and retry")
            return False

    def extract(self):
        """
        """
        try:
            self.instruments
        except:
            print('Error: missing instruments. You need to add instruments with one of the datascope.load_xxx methods')
            return

        try:
            self.requestBody
        except:
            print('Error: missing report template. You need to add reports with one of the datascope.pricing/corax/reference methods')
            return

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

                if self.status_code == 200:
                    self.content = pd.DataFrame(json.loads(_respJson.content)['Contents'])
                    self.notes = json.loads(_respJson.content)['Notes'][0]
                    self.ricmaintenance = json.loads(_respJson.content)['Notes'][1]
                    print('Completed: added to self.content')

        elif (self.status_code == 200):
            self.content = pd.DataFrame(json.loads(_resp.content)['Contents'])
            self.notes = json.loads(_resp.content)['Notes'][0]
            self.ricmaintenance = json.loads(_resp.content)['Notes'][1]
            print('Completed: added to self.content')

        else:
            print('Error, issue with the export file. HTTP Status: ',self.status_code)

    def get_bond_sched_types(self):
        _url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/BondScheduleReportTemplateGetBondScheduleTypes'
        _header = {'Authorization': 'Token '+ self.token, 'Prefer': 'respond-async'}
        return pd.DataFrame(json.loads(requests.get(_url, headers= _header).content)['value'])

    def write_files(self, filename, notefilename = '',ricmaintfile = ''):
        """
		filename - name and path where the content will be written.
		notefilename - name and path where the notes will be written.
		ricmaintfile - name and path where the ric maintenance file will be written.
		"""
        self.content.to_csv(filename, index=False)
        if notefilename != "":
            with open(notefilename, 'w') as f:
                for line in f:
                    print(line)
                    f.write(line)
        else:
            print("It is good practice to save your notes files with the argument notefilename")
        if ricmaintfile != "":
            with open(ricmaintfile, 'w') as f:
                for line in f:
                    print(line)
                    f.write(line)
        else:
            print("It is good practice to save your ric maintenance files with the argument ricmainfile")

    def print_notes(self):
        for i in self.notes.split('\r\n'):
            print(i)