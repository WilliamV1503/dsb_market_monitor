import yfinance as yf
from yfinance import shared
import pandas as pd
import numpy as np
import datetime as dt
import time
import requests
from pygooglenews.__init__ import GoogleNews
from newsplease import NewsPlease
import base64
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.message import EmailMessage
import ssl
import smtplib
import os
import shutil


class TickerSummary:
    def __init__(self, summary: str = "", articles: str = "", big_mover: bool = False):
        self.summary = summary
        self.articles = articles
        self.big_mover = big_mover

def currentTime():
    now=dt.datetime.now()
    now_t=now.strftime("%d %m %y %H %M %S").split()
    return now_t

def cleanData(df):
    df.reset_index(inplace=True)
    df.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)

def yfPullToParquet(path,ticker,yf_pd):
    grab = yf.Ticker(ticker)
    hist = grab.history(period=yf_pd)
    df = hist
    cleanData(df)
    presetA(df,ticker)
    df.to_parquet(f'{path}{ticker}.parquet',index=False,engine='fastparquet')
    print(f'{ticker} data pulled from yfinance to {path}')
    
def openFromParquet(path,ticker):
    a=pd.read_parquet(f'{path}{ticker}.parquet',engine="fastparquet")
    return a
    
def tickerName(ticker):
    response = requests.get(f'https://finance.yahoo.com/quote/{ticker}?p={ticker}&.tsrc=fin-srch')
    soup = BeautifulSoup(response.text, 'html.parser')
    name = soup.select('h1')[0].text
    return name

def makeTodayFolder():
    path=r'commodities_images\\'
    hoy=currentTime()
    month=f'{hoy[1]}\\'
    year=f'{hoy[2]}\\'
    date=f'{hoy[1]}-{hoy[0]}-{hoy[2]}\\'
    if not os.path.exists(path):
        os.mkdir(path)
        os.mkdir(path+year)
        os.mkdir(path+year+month)
        os.mkdir(path+year+month+date)
    elif not os.path.exists(path+year):
        os.mkdir(path+year)
        os.mkdir(path+year+month)
        os.mkdir(path+year+month+date)
    elif not os.path.exists(path+year+month):
        os.mkdir(path+year+month)
        os.mkdir(path+year+month+date)
    elif not os.path.exists(path+year+month+date):
        os.mkdir(path+year+month+date)
    path_big=path+year+month+date
    print('Path made: ',path_big)
    return path_big

def makeDataFolder():
    path=r''
    data_folder='commodities_data_parquet\\'
    final=path+data_folder
    if not os.path.exists(final):
        os.mkdir(final)
    return final

def existsData(data_path,ticker):
    path=f'{data_path}{ticker}.parquet'
    if os.path.exists(path):
        return True
    return False

def downloadTickerData(d_path,ticker_list):
    for i in range(len(ticker_list)):
        ticker=ticker_list[i]
        yfPullToParquet(d_path,ticker,'5y')

def deleteImagesFolder():
    shutil.rmtree("commodities_images")
    print("Images folder has been deleted.")

def deleteDataFolder():
    shutil.rmtree("commodities_data_parquet")
    print("Images folder has been deleted.")
            
def validateTicker(ticker):
    
    tickerdata = yf.Ticker(ticker)

    try:
        history = tickerdata.history(period="5d")
        error_message = shared._ERRORS[ticker]
        if error_message!=None:
            return False
        else:
            return True
    except KeyError:
        return True
    
def latestTicker(t,t_suf,mo_k):
    #this list filters out tickers that don't change by month
    if t in ["^OVX","LIT","URA","GDX"]:
        return t
    else:
        now=currentTime()
        month=int(now[1])
        year=int(now[2])
        
        is_valid=None
        temp_ticker=""
        
        for i in range(1,13):
            temp_ticker = f'{t[:-2]}{mo_k[month]}{year}{t_suf[t]}'
            is_valid = validateTicker(temp_ticker)
            if is_valid:
                return temp_ticker
            else:
                if month==12:
                    year+=1
                    month=1
                else:
                    month+=1
        
        if is_valid==False:
            return None
        else:
            return temp_ticker
                
    
def tickerList(tick_list,t_suf,mo_k):
    today_tickers = []
    i = 0
    while i < len(tick_list):
        ticker = tick_list[i]
        latest_ticker = latestTicker(ticker,t_suf,mo_k)
        if latest_ticker != None:
            today_tickers.append(latest_ticker)
        else:
            print(f"Could not find latest ticker for {ticker}.")
        print(latest_ticker)
        i+=1
        
    return today_tickers

#news article scraping functions

def articlePreview(url):
    article = NewsPlease.from_url(url)
    preview = article.maintext[:250] + " ..."
    return preview

def normalizeURL2(url):
    while url[0:4]!="http":
        url=url[1:]
    for i in range(len(url)):
        if not is_ascii(url[i]):
            url=url[:i]
            break
    return url

def is_ascii(character):
    # Get the Unicode code point of the character
    code_point = ord(character)
    
    # Check if the code point is within the valid ASCII range
    return 0 <= code_point <= 127

def normalizeURL1(google_url):
    base64_url = google_url.replace("https://news.google.com/rss/articles/","").split("?")[0]
    try:
        actual_url = base64.b64decode(base64_url + "==")[4:-3].decode('utf-8', errors="replace")
        actual_url = normalizeURL2(actual_url)
    except base64.binascii.Error as e:
        actual_url = "Invalid URL: " + str(e)
    return actual_url

def getNews(ticker_keywords, lookback_period="12h", limit=3):
    gn = GoogleNews(lang = "en",country="US")
    articles = []
    for word in ticker_keywords:
        search = gn.search(word, when = lookback_period, )
        for i in range(len(search["entries"][:limit])):
            title = search["entries"][i]["title"]
            url = normalizeURL1(search["entries"][i]["link"])
            articles.append((title, url))
    
    unique_articles=list(set(articles))
    news_string = "Articles: \n"
    
    for i in range(len(unique_articles)):
        news_string+="\n"
        news_string+=unique_articles[i][0]
        news_string+="\n"
        news_string+=unique_articles[i][1]

    return news_string

#add new time series cols in df
def presetA(df,ticker):
    
    #MA = Moving Average
    #SD = Standard Deviation

    #below lines scale day close price of rough rice by 100 (yf issue)
    if ticker[:2]=='ZR':
        df.iloc[-1, df.columns.get_loc('Close')] = df.iloc[-1, df.columns.get_loc('Close')] *100
        df.iloc[-1, df.columns.get_loc('High')] = df.iloc[-1, df.columns.get_loc('High')] *100
        df.iloc[-1, df.columns.get_loc('Low')] = df.iloc[-1, df.columns.get_loc('Low')] *100
    
    deg=2
    
    df['Close Change'] = df['Close'].diff()
    df['Close Pct Change'] = df['Close'].pct_change()*100
    
    df['Close Change 50d SD'] = df['Close Pct Change'].abs().rolling(50, closed='left').std()
    df['Close Change 50d MA'] = df['Close Pct Change'].abs().rolling(50, closed='left').mean()
    
    df['50d MA'] = df['Close'].rolling(50, closed='left').mean()
    df['100d MA'] = df['Close'].rolling(100, closed='left').mean()
    df['200d MA'] = df['Close'].rolling(200, closed='left').mean()
    
    df['50d SD'] = df['Close'].rolling(50, closed='left').std()
    
    df['50d Lower BB'] = df['50d MA'] - deg*df['50d SD']
    df['50d Upper BB'] = df['50d MA'] + deg*df['50d SD']
    
    df['1mo Low'] = df['Low'].rolling(21, closed='left').min()
    df['3mo Low'] = df['Low'].rolling(63, closed='left').min()
    df['6mo Low'] = df['Low'].rolling(126, closed='left').min()
    df['1yr Low'] = df['Low'].rolling(252, closed='left').min()
    df['2yr Low'] = df['Low'].rolling(504, closed='left').min()
    df['3yr Low'] = df['Low'].rolling(756, closed='left').min()
    
    df['1mo High'] = df['High'].rolling(21, closed='left').max()
    df['3mo High'] = df['High'].rolling(63, closed='left').max()
    df['6mo High'] = df['High'].rolling(126, closed='left').max()
    df['1yr High'] = df['High'].rolling(252, closed='left').max()
    df['2yr High'] = df['High'].rolling(504, closed='left').max()
    df['3yr High'] = df['High'].rolling(756, closed='left').max()

def createPresetAChart(path, ticker, ticker_name, month, year, df):
    
    plt.plot(df["Date"].tail(63), df['Close'].tail(63),c='k')
    plt.plot(df["Date"].tail(63), df['50d MA'].tail(63),c='b',label="50d MA")
    plt.plot(df["Date"].tail(63), df['50d Lower BB'].tail(63),c='r',label='50d Lower BB')
    plt.plot(df["Date"].tail(63), df['50d Upper BB'].tail(63),c='g',label='50d Upper BB')
    
    leg = plt.legend(loc='lower center',fontsize="7")
    
    plt.xticks(fontsize=7)
    plt.yticks(fontsize=7)
    plt.title(month + " " + year + " " + ticker_name)
       
    final_path=path+f'{ticker}_Chart.png'
    plt.savefig(final_path)
    print(f"Chart for {ticker} created")
    plt.clf()

#create summaries based on conditional statements
def presetASummary(path, ticker, df,ticker_names,com_tickers,inv_mo_k,ticker_searchterms): 
    
    summary = ''
    
    deg=2
    
    close = df['Close'].iloc[-1]
    change = df['Close Change'].iloc[-1]
    pct_change = df['Close Pct Change'].iloc[-1]
    
    change_sd = df['Close Change 50d SD'].iloc[-1]
    change_ma = df['Close Change 50d MA'].iloc[-1]
    
    ma_50d = df['50d MA'].iloc[-1]
    ma_100d = df['100d MA'].iloc[-1]
    ma_200d = df['200d MA'].iloc[-1]
    
    sd_50d = df['50d SD'].iloc[-1]
    
    upbb_50d = df['50d Upper BB'].iloc[-1]
    lobb_50d = df['50d Lower BB'].iloc[-1]
    
    low = df['Low'].iloc[-1]
    low_1mo = df['1mo Low'].iloc[-1]
    low_3mo = df['3mo Low'].iloc[-1]
    low_6mo = df['6mo Low'].iloc[-1]
    low_1y = df['1yr Low'].iloc[-1]
    low_2y = df['2yr Low'].iloc[-1]
    low_3y = df['3yr Low'].iloc[-1]
    
    high=df['High'].iloc[-1]
    high_1mo = df['1mo High'].iloc[-1]
    high_3mo = df['3mo High'].iloc[-1]
    high_6mo = df['6mo High'].iloc[-1]
    high_1y = df['1yr High'].iloc[-1]
    high_2y = df['2yr High'].iloc[-1]
    high_3y = df['3yr High'].iloc[-1]

    big_move=False
    
    #Ticker name not used since loading resulted in "Will be right back..." names
    if ticker[-4:-3]=='.':
        mo_k=ticker[-7:-6]
        mo=inv_mo_k[mo_k]
        yr=ticker[-6:-4]
    else:
        mo=''
        yr=''
        
    if ticker in ["^OVX","LIT","URA","GDX"]:
        i=com_tickers.index(ticker)
    else:
        if ticker[:3]=="LBR":
            i=com_tickers.index(f'{ticker[:3]}=F')
        else:
            i=com_tickers.index(f'{ticker[:2]}=F')
                                
    ticker_name=ticker_names[com_tickers[i]]
    
    summary += f'{ticker} - Close: {close:.2f}; Change: {change:.2f}; (%) Change: {pct_change:.2f}%\n'
    summary += '\n'
    
    if abs(abs(pct_change)-change_ma) > deg*change_sd:
        summary+=f'Daily change of {pct_change:.2f}% is >{deg} SDs outside 50d moving average of {change_ma:.2f}%.\n'
        big_move=True
        
    if close>ma_50d:
        if close>upbb_50d:
            summary+=f'Close price of {close:.2f} is >{deg} SDs ({sd_50d:.2f}) above 50d moving average of {ma_50d:.2f}.\n'
        else:
            summary+=f'Close price of {close:.2f} is above 50d moving average of {ma_50d:.2f}.\n'  
    if close<ma_50d:
        if close<lobb_50d:
            summary+=f'Close price of {close:.2f} is >{deg} SDs ({sd_50d:.2f}) below 50d moving average of {ma_50d:.2f}.\n'
        else:
            summary+=f'Close price of {close:.2f} is below 50d moving average of {ma_50d:.2f}.\n'
            
    if close>ma_100d:
        summary+=f'Close price of {close:.2f} is above 100d moving average of {ma_100d:.2f}.\n'
    if close<ma_100d:
        summary+=f'Close price of {close:.2f} is below 100d moving average of {ma_100d:.2f}.\n' 
    if close>ma_200d:
        summary+=f'Close price of {close:.2f} is above 200d moving average of {ma_200d:.2f}.\n' 
    if close<ma_200d:
        summary+=f'Close price of {close:.2f} is below 200d moving average of {ma_200d:.2f}.\n'
        
    if low<low_3y:
        summary+=f'Low of {low:.2f} is below 3y low of {low_3y:.2f}.\n'
    elif low<low_2y:
        summary+=f'Low of {low:.2f} is below 2y low of {low_2y:.2f}.\n'
    elif low<low_1y:
        summary+=f'Low of {low:.2f} is below 1y low of {low_1y:.2f}.\n'
    elif low<low_6mo:
        summary+=f'Low of {low:.2f} is below 6mo low of {low_6mo:.2f}.\n'
    elif low<low_3mo:
        summary+=f'Low of {low:.2f} is below 3mo low of {low_3mo:.2f}.\n'
    elif low<low_1mo:
        summary+=f'Low of {low:.2f} is below 1mo low of {low_1mo:.2f}.\n'


    if high>high_3y:
        summary+=f'High of {high:.2f} is above 3y high of {high_3y:.2f}.\n'
    elif high>high_2y:
        summary+=f'High of {high:.2f} is above 2y high of {high_2y:.2f}.\n'
    elif high>high_1y:
        summary+=f'High of {high:.2f} is above 1y high of {high_1y:.2f}.\n'
    elif high>high_6mo:
        summary+=f'High of {high:.2f} is above 6mo high of {high_6mo:.2f}.\n'
    elif high>high_3mo:
        summary+=f'High of {high:.2f} is above 3mo high of {high_3mo:.2f}.\n'
    elif high>high_1mo:
        summary+=f'High of {high:.2f} is above 1mo high of {high_1mo:.2f}.\n'
        
    createPresetAChart(path, ticker, ticker_name, mo,yr,df)

    if big_move:
        news_string=getNews(ticker_searchterms[com_tickers[i]])
    else:
        news_string=""

    ticker_summary = TickerSummary(summary,news_string,big_move)
        
    return ticker_summary
    
    
#create email with preset A summary
def emailSummary(data_path,tickers,ticker_names,com_tickers,inv_mo_k,ticker_searchterms):
    
    today=currentTime()
    date=f'{today[1]}-{today[0]}-{today[2]}\\'
    path=makeTodayFolder()
    
    email_sender = 'wvillcomm247@gmail.com'
    email_p = 'awqqxwyupxmxkwea'
    email_receiver = 'wvillcomm247@gmail.com'

    em = MIMEMultipart()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = f'{date[:8]} 8:00PM EST - Commodities Market Update'
    em.preamble='This is a MIME Multipart message'

    body1=''
    body2=''
    
    for i in range(len(tickers)):
        
        ticker=tickers[i]
        
        df=openFromParquet(data_path,ticker)

        t_summary=presetASummary(path,ticker,df,ticker_names,com_tickers,inv_mo_k,ticker_searchterms)
        price_summary=t_summary.summary
        news_summary=t_summary.articles
        big_mover=t_summary.big_mover
        price_summary=price_summary.replace("\n", "<br />\n")
        news_summary=news_summary.replace("\n", "<br />\n")

        with open(f'{path}{ticker}_Chart.png', 'rb') as fp:
            img = MIMEImage(fp.read())
            img.add_header('Content-Disposition', 'attachment', filename=f'{ticker}_Chart.png')
            img.add_header('X-Attachment-Id', f'{i}')
            img.add_header('Content-ID', f'<{i}>')
            fp.close()
            em.attach(img)
            
        if big_mover:
            body1+=f'''<p>{price_summary}</p>
            <p>{news_summary}</p>
            <p><img src="cid:{i}"></p>'''
        else:
            body2+=f'''<p>{price_summary}</p>
            <p><img src="cid:{i}"></p>'''
            
        
    em.attach(MIMEText(
        f'''
        <html>
            <body>
                <h1 style="text-align: center;">Commodities Market Update</h1>
                <h2 style="text-align: left;">Big Movers:</h2>
                {body1}
                <h2 style="text-align: left;">Others:</h2>
                {body2}
            </body>
        </html>
        ''',
        'html', 'utf-8'))
    
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_p)
        smtp.sendmail(email_sender, email_receiver, em.as_string())
        
def main():
    
    print('COMMODITIES MONITOR STARTED')
    
    com_tickers = ['SB=F', 'KC=F', 'CC=F', 'HE=F', 'OJ=F', 'GF=F', 'LE=F', 'DC=F', 'ZC=F', 'ZS=F', 
                   'ZW=F', 'CT=F', 'ZO=F', 'ZR=F', '^OVX', 'LIT', 'URA', 'GDX', 'LBR=F', 'BZ=F', 
                   'HG=F', 'CL=F', 'RB=F', 'GC=F', 'NG=F', 'PA=F', 'PL=F', 'SI=F']
    
    ticker_names = {
        'SB=F':'Sugar #11',
        'KC=F':'Coffee',
        'CC=F':'Cocoa',
        'HE=F':'Lean Hogs',
        'OJ=F':'Orange Juice',
        'GF=F':'Feeder Cattle',
        'LE=F':'Live Cattle',
        'DC=F':'Class III Milk',
        'ZC=F':'Corn',
        'ZS=F':'Soybean',
        'ZW=F':'Chicago Wheat',
        'CT=F':'Cotton',
        'ZO=F':'Oat',
        'ZR=F':'Rough Rice',
        '^OVX':'CBOE Crude Volatility',
        'LIT':'Lithium ETF',
        'URA':'Uranium ETF',
        'GDX':'VanEck Gold Miners',
        'LBR=F':'Lumber',
        'BZ=F':'Brent Oil Last Day Finance',
        'HG=F':'Copper',
        'CL=F':'WTI Crude Oil',
        'RB=F':'RBOB Gasoline',
        'GC=F':'Gold',
        'NG=F':'Natural Gas',
        'PA=F':'Palladium',
        'PL=F':'Platinum',
        'SI=F':'Silver',
    }
    
    ticker_suffix = {
        'SB=F':'.NYB',
        'KC=F':'.NYB',
        'CC=F':'.NYB',
        'HE=F':'.CME',
        'OJ=F':'.NYB',
        'GF=F':'.CME',
        'LE=F':'.CME',
        'DC=F':'.CME',
        'ZC=F':'.CBT',
        'ZS=F':'.CBT',
        'ZW=F':'.CBT',
        'CT=F':'.NYB',
        'ZO=F':'.CBT',
        'ZR=F':'.CBT',
        '^OVX':'',
        'LIT':'',
        'URA':'',
        'GDX':'',
        'LBR=F':'.CME',
        'BZ=F':'.NYM',
        'HG=F':'.CMX',
        'CL=F':'.NYM',
        'RB=F':'.NYM',
        'GC=F':'.CMX',
        'NG=F':'.NYM',
        'PA=F':'.NYM',
        'PL=F':'.NYM',
        'SI=F':'.CMX'
    }
    
    month_key={
        1:'F',
        2:'G',
        3:'H',
        4:'J',
        5:'K',
        6:'M',
        7:'N',
        8:'Q',
        9:'U',
        10:'V',
        11:'X',
        12:'Z'
    }
    
    inv_month_key={
        'F':'Jan',
        'G':'Feb',
        'H':'Mar',
        'J':'Apr',
        'K':'May',
        'M':'Jun',
        'N':'Jul',
        'Q':'Aug',
        'U':'Sep',
        'V':'Oct',
        'X':'Nov',
        'Z':'Dec'
    }

    ticker_searchterms = {
        'SB=F':['Sugar #11', 'sugar futures'],
        'KC=F':['coffee futures'],
        'CC=F':['cocoa futures'],
        'HE=F':['lean hog futures', 'hog futures'],
        'OJ=F':['orange juice futures', 'orange prices'],
        'GF=F':['feeder cattle futures'],
        'LE=F':['live cattle futures'],
        'DC=F':['class iii milk futures', 'milk futures'],
        'ZC=F':['corn futures'],
        'ZS=F':['soybean futures'],
        'ZW=F':['wheat futures'],
        'CT=F':['cotton futures'],
        'ZO=F':['oat futures'],
        'ZR=F':['rough rice futures'],
        '^OVX':['crude oil volaility', 'crude oil'],
        'LIT':['lithium stocks','lithium prices','global x lithium etf'],
        'URA':['uranium stocks','uranium prices','global x uranium etf'],
        'GDX':['gold stocks', 'gold prices', 'vaneck gold miners etf'],
        'LBR=F':['lumber futures','lumber prices'],
        'BZ=F':['brent futures','brent crude oil'],
        'HG=F':['copper futures'],
        'CL=F':['wti crude futures','wti crude oil'],
        'RB=F':['gasoline futures', 'gasoline prices'],
        'GC=F':['gold futures'],
        'NG=F':['natural gas futures'],
        'PA=F':['palladium futures'],
        'PL=F':['platinum futures'],
        'SI=F':['silver futures']
    }
    
    d_path=makeDataFolder()
    today_tickers = tickerList(com_tickers,ticker_suffix,month_key)
    downloadTickerData(d_path,today_tickers)
    
    emailSummary(d_path,today_tickers,ticker_names,com_tickers,inv_month_key,ticker_searchterms)
    
    deleteDataFolder()
    deleteImagesFolder()

        