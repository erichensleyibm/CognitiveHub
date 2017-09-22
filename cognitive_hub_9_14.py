
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 12 14:55:42 2017

@author: minah.kim@ibm.com
"""



def runcognitivehubrefresh():
    import requests
    from bs4 import BeautifulSoup
    import random
    mutual_funds = ['VHCAX', 'VWILX', 'FSELX', 'TRBCX', 'FOCPX',
                    'VFINX', 'VINIX', 'FSTMX', 'RIRCX', 'RPMGX',
                    'AEGFX', 'FCNTX', 'VMCPX', 'FLPSX', 'NEWFX']
    dct = {}
      
    #for ticker in mutual_funds:
    #    url = "https://finance.yahoo.com/quote/%s/profile?p=%s" %(ticker,ticker)
    #    r = requests.get(url) 
    #    soup = BeautifulSoup(r.content,"lxml")
    #    dct['lst_%s' % ticker] = []
    #    overview = soup.find_all("span", {"class":"Fl(end)"})
    #
    #    for item in overview:
    #        dct['lst_%s' % ticker].append(item.text)
    
        
    for ticker in mutual_funds:
        url = "https://finance.yahoo.com/quote/%s/profile?p=%s" %(ticker,ticker)
        r = requests.get(url) 
        soup = BeautifulSoup(r.content,"lxml")
        dct['lst_%s' % ticker] = []
        overview = soup.find_all("span", {"class":"Fl(end)"})
    
        for item in overview:
            dct['lst_%s' % ticker].append(item.text)
        
        #    
        #### Added current price and change
        #
        currentprice = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)"})
        for price in currentprice:
            dct['lst_%s' % ticker].append(price.text)
        changes = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(500) Pstart(10px) Fz(24px)"})
        for change in changes:
            dct['lst_%s' % ticker].append(change.text.split(" ")[0])
            dct['lst_%s' % ticker].append(change.text.split(" ")[1][1:-2])        
        changes = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(500) Pstart(10px) Fz(24px) C($dataGreen)"})
        for change in changes:
            dct['lst_%s' % ticker].append(change.text.split(" ")[0][1:])
            dct['lst_%s' % ticker].append(change.text.split(" ")[1][2:-2])   
        changes = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(500) Pstart(10px) Fz(24px) C($dataRed)"})
        for change in changes:
            dct['lst_%s' % ticker].append(change.text.split(" ")[0])
            dct['lst_%s' % ticker].append(change.text.split(" ")[1][1:-2])   
        #
        #### Done with addition
        #
        
        
    print dct
    
    '''
    for item in g_data:
        print item.contents[0].text
    '''
    
    datedict = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}
    import mysql.connector
    
    
    
    def push_data_to_db(dct, NewOrUpdate):
        cnx = mysql.connector.connect(user = 'b864ff502ee38e',
                                  password = 'cb8f7deb',
                                  host = 'us-cdbr-sl-dfw-01.cleardb.net',
                                  database = 'ibmx_40b976d51ccb51c')
        cursor = cnx.cursor()
        
        ### first time set up
        if NewOrUpdate == 'new':
            cursor.execute('call ibmx_40b976d51ccb51c.sp_reset_db();')
            cnx.commit()
        
        #sets basic info for each fund          
            for fund in dct:
                baseinsert = 'insert into `FundOverview` (`FundName`, `Category`,`FundFamily`, `Inception`) VALUES ("%s", "%s", "%s", "%s");'% (fund, dct[fund][1], dct[fund][2], str(str(dct[fund][7].split(' ')[2])+'/'+datedict[str(dct[fund][7].split(' ')[0])]+'/'+str(dct[fund][7].split(' ')[1]))[:10])
                cursor.execute(baseinsert)
                cnx.commit()
        
        #drop outdated data
        if NewOrUpdate == 'update':
            cursor.execute('truncate table `PriceAndPerformance`;')
        
        #replace with new data
        for fund in dct:
            # still need to add SEC Yield
            updateinsert = 'insert into `PriceAndPerformance` (`FundName`, `NetAssets`, `YTD_Return`, `Yield`, `Rating`, `Share_Price`, `Change_USD`, `Change_Perc`) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s");'% (fund, dct[fund][3][:-1], dct[fund][4][:-1], dct[fund][5][:-1], len(dct[fund][6]), dct[fund][12], dct[fund][13], dct[fund][14]) 
            cursor.execute(updateinsert)
            cnx.commit()
            
        cursor.close()
        cnx.close()
        
    
    
    
    push_data_to_db(dct, 'new')
    push_data_to_db(dct, 'update')
    
    
    
    
    
    
    
    
    
    
    
    #ETFs = ['IBB', 'VCLT','VTWO','SOXX','ONEQ','VXUS','SCZ','VNQI',\
    #        'IJT','VONE','CIU','SHY','VTIP','ACWV','VGSH']
    ETFs = ['VGSH', 'ONEQ','SOXX','CIU','VCLT','IBB','VONE','SCZ','VTWO','IJT','VNQI']
    def get_ETF(list_ETF):
        dct = {}
        list_ETF = ETFs
        for ticker in list_ETF:
            url = "https://finance.yahoo.com/quote/%s?p=%s" %(ticker,ticker) 
            r = requests.get(url) 
            soup = BeautifulSoup(r.content,"lxml")
            dct['lst_%s' % ticker] = []
            overview = soup.find_all("span", {"class":"Trsdu(0.3s) "})
    #        full_name = soup.find_all("div",{"class":"D(ib)"})
    #        dct['lst_%s' % ticker].append(full_name.text)
            for item in overview:
                dct['lst_%s' % ticker].append(item.text)
            
            currentprice = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)"})
            for price in currentprice:
                dct['lst_%s' % ticker].append(price.text)
            changes = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(500) Pstart(10px) Fz(24px)"})
            for change in changes:
                dct['lst_%s' % ticker].append(change.text.split(" ")[0])
                dct['lst_%s' % ticker].append(change.text.split(" ")[1][1:-2])        
            changes = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(500) Pstart(10px) Fz(24px) C($dataGreen)"})
            for change in changes:
                dct['lst_%s' % ticker].append(change.text.split(" ")[0][1:])
                dct['lst_%s' % ticker].append(change.text.split(" ")[1][2:-2])   
            changes = soup.find_all("span", {"class":"Trsdu(0.3s) Fw(500) Pstart(10px) Fz(24px) C($dataRed)"})
            for change in changes:
                dct['lst_%s' % ticker].append(change.text.split(" ")[0])
                dct['lst_%s' % ticker].append(change.text.split(" ")[1][1:-2])   
    
        return dct
    
    
    def push_ETF_data_to_db(dct, NewOrUpdate):
    #    dct = x
        NewOrUpdate = 'new'
        cnx = mysql.connector.connect(user = 'b864ff502ee38e',
                                  password = 'cb8f7deb',
                                  host = 'us-cdbr-sl-dfw-01.cleardb.net',
                                  database = 'ibmx_40b976d51ccb51c')
    
        cursor = cnx.cursor()
        ### first time set up
        if NewOrUpdate == 'new':
            cursor.execute('call ibmx_40b976d51ccb51c.sp_reset_ETF_();')
            cnx.commit()
        
        #sets basic info for each fund 
            for fund in dct:
                try:
                    baseinsert = 'insert into `etf_fundoverview` (`ETFName`,\
                     `Inception`) \
                    VALUES ("%s", "%s");'\
                    % (fund, dct[fund][13].replace("-","/"))
                    cursor.execute(baseinsert)
                    cnx.commit()
                except:
                    pass
        dct[fund]
        #drop outdated data
        if NewOrUpdate == 'update':
            cursor.execute('truncate table `etf_priceandperformance`;')
        
         
        #replace with new data
        for fund in dct:
            try:
                # still need to add SEC Yield
                updateinsert = 'insert into `etf_priceandperformance` (`ETFName`, `Bid`, `Ask`, `NetAssets`, `NAV`, `PE_Ratio`, `Yield`, `YTD_Return`, `Beta`, `ExpRatio`, `Share_Price`, `Change_USD`) VALUES ("%s", "%s", "%s", "%s", \
                "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s");' % (fund, dct[fund][0], dct[fund][1], dct[fund][2].split(' x')[0][0]+dct[fund][2].split(' x')[0][1], \
                dct[fund][3].split(' x')[0][0]+dct[fund][3].split(' x')[0][1], random.random(), random.random(), \
                random.random(), random.random(), random.random(), random.random(), \
                dct[fund][14])
                cursor.execute(updateinsert)
                cnx.commit()
            except:
                pass
            
        cursor.close()
        cnx.close()
        
        
        
    x = get_ETF(ETFs)
    
    push_ETF_data_to_db(x, 'new')
    push_ETF_data_to_db(x, 'update')

z = 5
while z < 10:
    runcognitivehubrefresh()