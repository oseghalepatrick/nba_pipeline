import uuid
import numpy as np
import pandas as pd
from datetime import datetime, date

def scrapeGames(year, dest_file):
    
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 
          'august','september', 'october', 'november', 'december']
    
    """
    I scrape the data for seasons 1996-97 to present. 
    These season have play-by-play data.
    """
    df = pd.DataFrame(columns=['Date', 'seasonStartYear', 'Visitor/Neutral', 'PTS', 
                               'Home/Neutral', 'PTS.1', 'Unnamed: 5', 'Unnamed: 6', 'Attend.', 'Notes'])
    current_yr = int(date.today().strftime('%Y'))
    months_not_played = months[months.index(date.today().strftime('%B').lower())+1:]
    for month in months: #Due to how urls are structured on the website I need to collect months separetly
        if year == current_yr and month in months_not_played: continue
        try:
            url = 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'.format(year, month)
            yearMonth_table = pd.io.html.read_html(url)[0]
            yearMonth_table['seasonStartYear'] = year - 1
            df = pd.concat([df, yearMonth_table]).reset_index(drop='index')
        except:
            continue

    """
    The covid season has two october months, which the code above does not find. 
    I add these months by iterating through this url_lst.
    """
    print(df.columns)
    if year in [2019, 2020]:
        url_lst = f'https://www.basketball-reference.com/leagues/NBA_2020_games-october-{year}.html'
        yearMonth_table = pd.io.html.read_html(url_lst)[0]
        yearMonth_table['seasonStartYear'] = 2019
        df = pd.concat([df, yearMonth_table]).reset_index(drop='index')
    else:
        df['Start (ET)'] = np.nan
    try:
        df.drop(columns=['Unnamed: 7'], inplace=True)
    except KeyError:
        pass

    #Fix date column
    print(df.columns)
    df_backup = df.copy()
    df = df_backup.copy()
    df = df[df.Date!='Playoffs'].copy()
    df['datetime'] = pd.to_datetime(df['Date'], infer_datetime_format=True)

    """
    Creates a variable for playoff and regular season games.
    """
    playOffStart = {1996:'1997-04-24', 1997:'1998-04-23', 1998:'1999-05-08', 1999:'2000-04-22', 2000:'2001-04-21',
                   2001:'2002-04-20',2002:'2003-04-19', 2003:'2004-04-17', 2004:'2005-04-23', 2005:'2006-04-22',
                   2006: '2007-04-21', 2007:'2008-04-19', 2008:'2009-04-18', 2009:'2010-04-17', 2010:'2011-04-16',
                   2011:'2012-04-28', 2012:'2013-04-20', 2013:'2014-04-19', 2014:'2015-04-18', 2015:'2016-04-16',
                   2016:'2017-04-15', 2017:'2018-04-14', 2018:'2019-04-13', 2019:'2020-08-15', 2020:'2021-05-18',
                   2021:'2022-04-16'}

    df['playoff'] = df['seasonStartYear'].apply(lambda x: playOffStart[x])
    df['isRegular'] = df['playoff'] > df['datetime']
    df['isRegular'] = df['isRegular'].apply(lambda x: 1 if x else 0)
    df.drop(columns=['playoff'], inplace=True)

    # Remove exhibition games in 2018-19
    df[(df.seasonStartYear==2018)&(df.isRegular==1)&(df['Visitor/Neutral']=='Boston Celtics')].sort_values(by=['datetime'])
    df['d'] = df['datetime']> '2018-10-01'
    df['y'] = df['seasonStartYear'] != 2018
    df = df[(df['d'])|(df['y'])].copy()
    df.drop(columns=['d','y'], inplace=True)

    # I drop Unnecessary columns
    print(df.columns)
    df.drop(columns=['Unnamed: 5','Unnamed: 6', 'Date'], inplace=True)
    print(df.columns)

    #Create game_id
    df.reset_index(drop='index', inplace=True)
    df = df.sort_values(by='datetime')
    df['game_id'] = [uuid.uuid4().hex for i in range(len(df))]

    #Set column names
    df.set_axis(['seasonStartYear', 'awayTeam', 'pointsAway', 'homeTeam', 'pointsHome','attendance',
                'notes', 'startET', 'datetime', 'isRegular', 'game_id'], axis=1, inplace=True)
    df.to_csv(dest_file, index=False)