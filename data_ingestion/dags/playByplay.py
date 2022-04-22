import uuid
import pandas as pd
from datetime import datetime, date

def fixHornets(row):
    if row.seasonStartYear < 2014 and row['homeTeam'] == 'Charlotte Hornets':
        return 'CHH'
    else:
        return row.abbreviation
    
def createTime(row):
    try:
        minute, sec = row.split(":")
        sec = sec.split('.')[0]
        return int(minute)*60 + int(sec)
    except:
        return row
    
def create_period(df):
    """
    Create period variable:
    - If game id changes --> new game starts
    - If periodTime == 720 --> new period starts
    """

    prevGame_id = 0
    lst = []
    for i, row in df.iterrows():
        if prevGame_id != row.game_id:
            lst.append(1)
            prevGame_id = row.game_id
            continue
        elif row.periodTime == 720:
            lst.append(lst[-1]+1)
            continue
        elif row.periodTime != 720:
            lst.append(lst[-1])
            continue
        else:
            lst.append("ERROR")
    return lst

def createScore(row):
    try:
        return [int(i) for i in row.score.split('-')]
    except:
        return None

def createHomeAwayPoints(df):
    pointsAway = []
    pointsHome = []

    for i, row in df.iterrows():
        point_lst = createScore(row)
        if point_lst != None:
            pointsAway.append(point_lst[0])
            pointsHome.append(point_lst[1])
        else:
            if row.period == 1 and row.periodTime == 720:
                pointsAway.append(0)
                pointsHome.append(0)
            else:
                pointsAway.append(pointsAway[-1])
                pointsHome.append(pointsHome[-1])
    return pointsAway, pointsHome

def cleanDuplicateValues(df, colName):
    return_lst = []
    for i, row in df.iterrows():
        if row.event_away == row.point_away:
            return_lst.append(None)
        elif row.point_away == None:
            return_lst.append(None)
        else:
            return_lst.append(row[colName])
    return return_lst

def scrapePlayByPlay(src_file, dest_file):
    
    """
    I add the abbreviation for the home team because I need it for creating the URLs.
    """

    teamAbbreviation = {'Dallas Mavericks':'DAL', 'Phoenix Suns':'PHO', 'Boston Celtics':'BOS',
           'Portland Trail Blazers':'POR', 'New Jersey Nets':'NJN', 'Toronto Raptors':'TOR',
           'Los Angeles Lakers':'LAL', 'Utah Jazz':'UTA', 'Philadelphia 76ers':'PHI',
           'New York Knicks':'NYK', 'Minnesota Timberwolves':'MIN', 'Orlando Magic':'ORL',
           'San Antonio Spurs':'SAS', 'Sacramento Kings':'SAC', 'Atlanta Hawks':'ATL',
           'Seattle SuperSonics':'SEA', 'Washington Bullets':'WSB', 'Indiana Pacers':'IND',
           'Los Angeles Clippers':'LAC', 'Miami Heat':'MIA', 'Milwaukee Bucks':'MIL',
           'Charlotte Hornets':'CHO', 'Cleveland Cavaliers':'CLE', 'Houston Rockets':'HOU',
           'Denver Nuggets':'DEN', 'Vancouver Grizzlies':'VAN', 'Golden State Warriors':'GSW',
           'Chicago Bulls':'CHI', 'Detroit Pistons':'DET', 'Washington Wizards':'WAS',
           'Memphis Grizzlies':'MEM', 'New Orleans Hornets':'NOH', 'Charlotte Bobcats':'CHA',
           'New Orleans/Oklahoma City Hornets':'NOK', 'Oklahoma City Thunder':'OKC',
           'Brooklyn Nets':'BRK', 'New Orleans Pelicans':'NOP'}
    games_df = pd.read_csv(src_file)
    games_df = games_df[~games_df.pointsAway.isnull()]
    games_df['abbreviation'] = games_df['homeTeam'].apply(lambda x: teamAbbreviation[x])
    # fix abbreviation for Charlotte Hornets
    games_df['abbreviation'] = games_df.apply(fixHornets, axis=1)
    yr = games_df.seasonStartYear[0]
    df = pd.DataFrame(columns=['time', 'event_away', 'point_away', 'score', 'point_home','event_home', 'game_id'])
    for i, row in games_df.iterrows():
        year, month, day = row.datetime.split('-')
        url = "https://www.basketball-reference.com/boxscores/pbp/{}{}{}0{}.html".format(year, month, day, row.abbreviation)
        tables = pd.io.html.read_html(url)[0]['1st Q']
        tables.set_axis(['time', 'event_away', 'point_away', 'score', 'point_home','event_home'], axis=1, inplace=True)
        tables['game_id'] = row.game_id
        df = pd.concat([df, tables]).reset_index(drop='index')

    # Create play_id
    df['play_id'] = [uuid.uuid4().hex for i in range(len(df))]

    df['periodTime'] = df.time.apply(createTime)
    # I cut values that do not have a period time becuase these are headings for the tables on basketball-reference
    df = df[df['periodTime'].apply(lambda x: isinstance(x, (int, float)))]
    df.drop(columns=['time'], inplace=True)

    # Create period variable:
    df['period'] = create_period(df)

    pointsAway, pointsHome = createHomeAwayPoints(df)
    df['awayPoints'] = pointsAway
    df['homePoints'] = pointsHome
    df.drop(columns=['score'], inplace=True)

    df['point_away'] = cleanDuplicateValues(df, 'point_away')
    df['point_home'] = cleanDuplicateValues(df, 'point_home')
    df['event_home'] = cleanDuplicateValues(df, 'event_home')

    df.to_csv(dest_file, index=False)