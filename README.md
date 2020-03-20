# datascope

This is an unofficial library for the datascope select api. Datascope is a licensed Pricing and Reference platform from Refinitiv. Each module and instrument count may require licensing in place with Refinitiv.

The module works in the following manner:

First step, intialize a Datascope instance like shown below:

import datascope
dss = datascope.session('id','pw')

This will authenticate your credentials and store a 24 hour Token that will be paired with all further requests. 
Second Step, add instruments to your datascope object. I have currently built two loading methods: load_pd and load_csv. load_pd() allows you to load form a Pandas dataframe where by the columns must be in this order: ['IdentifierType','Identifier',optional:'source']. The load_csv file takes a file with no headers in the same positional manner that Datascope Select reads in files. Notice, the IdentifierType field should be in the API syntax, for example 'Cusip' as opposed to 'CSP'. However, if you load a standard DSS instrument list in, this method will try to convert traditional DSS types into API syntax. These methods save variables to your dss object. There is also a validate option, that will validate your instruments, saving time during the extract phase. These methods are shown in examples below:

dss.load_pd(pd.DataFrame({'type':['Ric','Ric'],'id':['AAPL.O','DIS']}))
dss.load_csv('myfile.csv')

The next step is to add a template to DSS, note the instruments should be loaded first. As of the writing of this documentation, there are a variety of methods written for various templates. This step will complete the json body creation, taking the validated or un-validated instruments from the load phase. Note, if the fields are not correct or do not line up with those in DSS, the extract() phase will likely fail. The fields must line up with the templates you use, for more info on this mapping, look at the GUI(hosted.datascope.reuters.com) or look at the Data Conten Guide avaialble on our support website. (my.refinitiv.com) Examples are below:

dss.composite(['Asset Type'])
dss.price_history(['Trade Date','Bid Price'],'2019-01-01','2019-05-01')
dss.price_intraday(['Primary Activity','Secondary Activity'])
dss.reference('tnc',['Asset Type','Asset SubType'])
dss.corax_cap_change('2019-01-01','2019-05-01',[fields],'ann')
dss.corax_earnings('2019-01-01','2019-05-01',[fields],'ped')
dss.corax_nominal_value('2019-01-01','2019-05-01',[fields],'ead')
dss.corax_shares_outstanding('2019-01-01','2019-05-01',[fields],'Authorised')
dss.corax_dividend('2019-01-01','2019-05-01',[fields],'exd')
dss.corax_mna('2019-01-01','2019-05-01',[fields],'ann')

From here for diagnostic purposes, you could also call the variables dss.requestBody or dss.requestHeader or dss.requestUrl to remediate any issues with your extract. This is a good feature if you're going to ask for help on the developers community. The last step is to extract the report. Simply see the following:

dss.extract()

If successful, this will save three variables in your object: dss.content, dss.notes, dss.ric_maintenance. 

