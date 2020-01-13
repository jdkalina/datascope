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
        import requests
        import json
        import pandas as pd

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
        from requests import get
        from json import loads
        from pandas import DataFrame

        _url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Users/UserClaims"

        _header={
            "Prefer":"respond-async",
            "Authorization": "Token " + self.token
        }

        return DataFrame(loads(get(_url, headers = _header).content)['value'])

    def _usage(self, startDate = '', endDate = '', type = 'i'):
        """
        :startDate: the beginning period for usage evaluation
        :endDate: the ending period for usage evaluation
        :type: either instrument or entity:
            'i' == instrument
            'e' == entity

        DevNotes: Not working thus far, this will need to be a pet project as its not very well annotated in the RestAPI tree.
        """
        from requests import post

        print('WARNING: This feature is under development and not currently working, use the GUI to check for now')
        _body = {
            'ExtractionUsageCriteria':
                {
                     'EndDateTime' : endDate,
                     'StartDateTime': startDate
                }
            }

        if type == 'i':
            _post = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Usage/GetExtractionUsageInstrumentSummary"
        elif type == 'e':
            _post = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Usage/GetExtractionUsageLegalEntitySummary"

        _header={
            "Prefer":"respond-async",
            "Authorization": "Token " + self.token
        }

        return post(_post, headers = _header, data = _body)

    def all_fields(self,template):
        """
        template:
        options available below:
            'elektron_timeseries'
            'historical_reference'
            'intraday_summaries'
            'time_and_sales'
            'market_depth'
        """
        from time import sleep
        import requests

        def create_url(report):
            _url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss.Api.Extractions.ReportTemplates.ReportTemplateTypes"
            return _url + "'"+ report + "')"

        _header={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization": "Token " + self.token
        }

        if template == 'historical_reference':
            _url = create_url('HistoricalReference')
        elif template == 'elektron_timeseries':
            _url = create_url('ElektronTimeseries')
        elif template == 'intraday_summaries':
            _url = create_url('TickHistoryIntradaySummaries')
        elif template == 'time_and_sales':
            _url = create_url('TickHistoryTimeAndSales')
        elif template == 'market_depth':
            _url = create_url('TickHistoryMarketDepth')
        else:
            print("Template Name not found")

        fields = []
        resp = requests.get(_url, headers = _header)
        while resp.status_code != 200:
            sleep(1)
            resp = requests.get(_url, headers = _header)

        for i in json.loads(resp.content)['value']:
            fields.append(i['Name'])
            return fields

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

    def instruments(self, instrument, isTS = False, tsStart = None, tsEnd = None, isChain = False):
        """
        DevNotes: This is designed currently for Identifier lists only. Criteria lists will be soon to follow.
            instrument: Either a character string for a single RIC or Chain RIC or list of RICs.
            tsStart: if pricing a timeseries or intraday template, "YYYY-MM-DD" format.
            tsEnd: if pricing a timeseries or intraday template, "YYYY-MM-DD" format.
            isChain: True or False. is chain activates chain_expand if True.
        """
        self.timeseries = isTS
        self.start = tsStart
        self.end = tsEnd
        self.odataIns = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList"
        if isChain:
            self.expand_chain(instrument, self.start, self.end)
        else:
            if type(instrument) is str:
                self.rics = [instrument]
            else:
                self.rics = instrument

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
                _body["ExtractionRequest"]["Condition"] = {"LimitReportToTodaysData": "True"}
            else:
                _body["ExtractionRequest"]["Condition"] = {"LimitReportToTodaysData": "False"}

        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        for i in self.rics:
            _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
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
        for i in self.rics:
            _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def corporate_actions(self, fields, rangeType = "Range"):
        """This method provides access to standard events (Refinitiv term), corporate actions. Note,

        :fields: takes in a [list] of fields specific to the template you selected. Note, if you enter in non-existent fields or fields from the wrong template, you may throw a 400 error on the server.
        :rangeType: this is a preference specific to Corax. Possible values are "Delta", "Init", "Last", "NoRange", "Range".

        """

        _odata = "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest"

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
                "Condition": {
                    "ReportDateRangeType": rangeType,
                    "IncludeInstrumentsWithNoEvents": "true",
                    "ExcludeDeletedEvents": "true",
                    "IncludeCapitalChangeEvents": "true",
                    "IncludeDividendEvents": "true",
                    "IncludeEarningsEvents": "true",
                    "IncludeMergersAndAcquisitionsEvents": "true",
                    "IncludeNominalValueEvents": "true",
                    "IncludePublicEquityOfferingsEvents": "true",
                    "IncludeSharesOutstandingEvents": "true",
                    "IncludeVotingRightsEvents": "true",
                    "CorporateActionsCapitalChangeType": "CapitalChangeAnnouncementDate",
                    "CorporateActionsDividendsType": "DividendAnnouncementDate",
                    "CorporateActionsEarningsType": "EarningsAnnouncementDate",
                    "CorporateActionsEquityOfferingsType": "AllPendingDeals",
                    "CorporateActionsMergersAcquisitionsType": "DealAnnouncementDate",
                    "CorporateActionsNominalValueType": "NominalValueDate",
                    "CorporateActionsSharesType": "SharesAmountDate",
                    "CorporateActionsStandardEventsType": "CAP",
                    "CorporateActionsVotingRightsType": "VotingRightsDate",
                    "ShareAmountChoice": "All",
                    "ShareAmountTypes": [
                        "Authorised"
                    ]
            }
        }

        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        for i in self.rics:
            _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header

    def export(self, file, note_file = ''):
        """
        :file: this is the filename and path for output file. Note, if the text is written ':memory:', then this function will return a tuple PANDAS dataframe (file, note_file) instead writing files to disk. Do the same with note_file.
        :note_file: this is the filename and path for notes file.
        """
        import requests
        from time import sleep
        import pandas as pd

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
                sleep(30)
                _respJson = requests.get(_url,headers=_requestHeaders)
                self.status_code = _respJson.status_code

                if self.status_code != 200 :
                    print('ERROR: An error occurred. Try to run this cell again. If it fails, re-run the previous cell.')

                if self.status_code == 200:
                    if file == ':memory:':
                        return (pd.DataFrame(json.loads(_resp.content)['Contents']), pd.DataFrame(json.loads(_resp.content)['Notes']))
                    else:
                        pd.DataFrame(json.loads(_respJson.content)['Contents']).to_csv(file)
                        pd.DataFrame(json.loads(_respJson.content)['Notes']).to_csv(note_file)
                    print('Successfully downloaded file')
                else:
                    print(self.status_code, "- Issue raised")
        elif (self.status_code == 200):
            if file == ':memory:':
                return (pd.DataFrame(json.loads(_resp.content)['Contents']), pd.DataFrame(json.loads(_resp.content)['Notes']))
            else:
                pd.DataFrame(json.loads(_resp.content)['Contents']).to_csv(file)
                pd.DataFrame(json.loads(_resp.content)['Notes']).to_csv(note_file)
            print('Successfully downloaded file')
        else:
            print('Error, issue with the export file. HTTP Status: ',self.status_code)

    def expand_chain(self, chain, start, end):
        from json import loads
        from requests import post

        _header = {"Prefer":"respond-async",
                   "Content-Type":"application/json",
                   "Authorization":"Token " + self.token
                   }

        def bodyGenerator(chain, start, end):
            return {
                "Request": {
                    "ChainRics": [chain],
                    "Range": {
                        "Start": start,
                        "End": end
                    }
                }
            }

        _body = bodyGenerator(chain, start, end)
        _chain = post('https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/HistoricalChainResolution',
                               headers = _header,
                               json= _body)
        _data = loads(_chain.content)

        self.rics = []
        for i in _data['value']:
            for p in i['Constituents']:
                self.rics.append(p['Identifier'])

    def _serial_requests(self, template, concurrent_files, directory, file_name, fields, ifintervalsummary = ''):
        """
        This is the post, async call, and download from the TRTH servers.
        :directory: this is the local path where you will be downloading your files.
        :template: options available below:
            'elektron_timeseries'
            'historical_reference'
            'intraday_summaries'
            'time_and_sales'
            'market_depth'
        :concurrent_files: should be a number 1-50
            :directory: file path where the file where be saved.
            :file_name: name of the output file (extension should not include '.csv')
            :fields: list of fields for the template of choice
            :ifintervalsummary: If using the intraday template what is the interval.
            """
        from datetime import datetime
        from math import ceil

        ric_uni = self.rics.copy()
        beg = 0
        num_rics = ceil(len(self.rics)/concurrent_files)
        end = num_rics
        for i in range(concurrent_files):
            self.rics = ric_uni[beg:end]
            if template == 'historical_reference':
                self.historical_reference(fields)
                self.async_post(file = directory + file_name + str(i + 1) + '.csv')
            elif template == 'elektron_timeseries':
                self.elektron_timeseries(fields)
                self.async_post(file = directory + file_name + str(i + 1) + '.csv')
            elif template == 'intraday_summaries':
                self.intraday_summaries(fields, interval = ifintervalsummary)
                self.async_post(file = directory + file_name + str(i + 1) + '.csv')
            elif template == 'time_and_sales':
                self.time_and_sales(fields)
                self.async_post(file = directory + file_name + str(i + 1) + '.csv')
            elif template == 'market_depth':
                self.market_depth(fields)
                self.async_post(file = directory + file_name + str(i + 1) + '.csv')
            else:
                print("Template Name not found")
            beg += num_rics
            end += num_rics

    def historical_reference(self, fields):
        """
        This method should be called for Tick History Historical Reference Files.
        :fields: A list of field names to be used with this historical reference files.
        """
        _header={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization": "Token " + self.token
        }

        _body={
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.HistoricalReferenceExtractionRequest",
                "ContentFieldNames": [],
                "IdentifierList": {
                    "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                    "InstrumentIdentifiers": [],
                    "ValidationOptions": {"AllowHistoricalInstruments": "true"},
                    "UseUserPreferencesForValidationOptions": "false"
                },
                "Condition": {
                    "ReportDateRangeType": "Range",
                    "QueryStartDate": self.start,
                    "QueryEndDate": self.end
                }
            }
        }
        for i in fields:
            _body["ExtractionRequest"]["ContentFieldNames"].append(i)
        for i in self.rics:
            _body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
        self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
        self.requestBody = _body
        self.requestHeader = _header
