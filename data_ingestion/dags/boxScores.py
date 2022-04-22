import uuid
import pandas as pd
from datetime import datetime, date

def fixHornets(row):
        if row.seasonStartYear < 2014 and row.homeTeam == 'Charlotte Hornets':
            return 'CHH'
        else:
            return row.abbreviation
            
def scrapeBoxScores(src_file, dest_file):
    games = pd.read_csv(src_file)

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

    games['abbreviation'] = games['homeTeam'].apply(lambda x: teamAbbreviation[x])

    games['abbreviation'] = games.apply(fixHornets, axis=1)
    games = games[~games.pointsAway.isnull()]

    df = pd.DataFrame(columns=['game_id', 'teamName','playerName', 'MP', 
                               'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB',
                               'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', '+/-'])
    for i, row in games.iterrows():
        year, month, day = row.datetime.split('-')
        url = "https://www.basketball-reference.com/boxscores/{}{}{}0{}.html".format(year, month, day, row.abbreviation)
        tables = pd.io.html.read_html(url)
        away = True
        for table in tables:
            # It throws an error at the 2nd if statement without the 1st if statement
            if table.columns.get_level_values(0)[1] == 'Advanced Box Score Stats': continue
            if table['Basic Box Score Stats'][-1:]['MP'].isna().max(): continue
            if int(table['Basic Box Score Stats']['MP'][-1:].max()) >= 240:
                teamStats = table['Basic Box Score Stats']
                teamStats['playerName'] = table['Unnamed: 0_level_0']
                teamStats['game_id'] = row.game_id
                if away:
                    teamStats['teamName'] = row.awayTeam
                    away = False
                else:
                    teamStats['teamName'] = row.homeTeam
                df = pd.concat([df, teamStats])


    """
    Create a variable for starters
    """
    lst = []
    j= 0
    for i, row in df.iterrows():
        if row.playerName == 'Reserves':
            lst.append(0)
        elif row.playerName == 'Team Totals':
            lst.append(1)
        elif i == 0:
            lst.append(1)
        else:
            lst.append(lst[-1])

    df['isStarter'] = lst

    #Cut values that do not include data on players
    df = df[(df.playerName!='Reserves')&(df.playerName!='Team Totals')]
    df.drop(columns=['FG%', '3P%', 'FT%'], inplace=True)
    df.to_csv(dest_file, index=False)