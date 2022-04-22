import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, date

def scrapeCoaches(dest_file):
    """
    Gets all the coaches tags from basketball-reference.com. These tags include the URL for the coach's own page.
    """

    tag_lst = []
    current_yr = int(date.today().strftime('%Y'))
    for season in range(1997, current_yr):
        page = requests.get("https://www.basketball-reference.com/leagues/NBA_{}_coaches.html".format(season))
        soup = BeautifulSoup(page.content, 'html.parser')
        for tag in soup.find_all('a'):
            if re.search('<a href="/coaches/', str(tag)) and re.search('html', str(tag)):
                tag_lst.append(tag)

    # Remove duplicates            
    tag_lst = list(dict.fromkeys(tag_lst))

    """
    Creates a df with the coach's name and url to their page.
    """
    coach_lst = []
    i = 1
    for tag in tag_lst:
        temp_lst = []
        temp_lst.append(i)
        i+=1
        for name in tag:
            temp_lst.append(name)
        temp_lst.append("https://www.basketball-reference.com" + str(tag).split('"')[1])

        coach_lst.append(temp_lst)

    coach_df = pd.DataFrame(coach_lst, columns=['coach_id','coachName', 'url'])

    """
    Collects the on the coaches
    """

    df = pd.DataFrame(columns=['Season', 'Age', 'Tm', 'Lg', 'G', 'W', 'L', 'W/L%', 'W > .500',
           'Finish', 'G', 'W', 'L', 'W/L%', 'Notes', 'coachName', 'coach_id'])

    for i, row in coach_df.iterrows():
        table = pd.io.html.read_html(row['url'])
        indexNames = list(dict.fromkeys([i[0] for i in table[0].columns]))
        temp_df = pd.DataFrame()
        for ind in indexNames:
            temp_df = pd.concat([temp_df, table[0][ind]], axis=1)
        temp_df['coachName'] = row.coachName
        temp_df['coach_id'] = row.coach_id
        temp_df.drop(columns=['Unnamed: 10_level_1'], inplace=True)

        df = pd.concat([df, temp_df])

    """
    Cut values that do not include data on the season level and create seasonStartYear variable
    """ 

    def createMask(season):
        if len(str(season).split('-'))==2:
            return True
        else:
            return False

    mask = df.Season.apply(createMask)

    df = df[mask]

    #Create season start year

    df['seasonStartYear'] = df.Season.apply(lambda x: int(x.split('-')[0]))
    df.drop(columns=['Season'], inplace=True)

    df.set_axis(['Age', 'Tm', 'Lg', 'G_reg', 'W_reg', 'L_reg', 'W/L%_reg', 'W > .500', 'Finish', 'G_playoff',
           'W_playoff', 'L_playoff', 'W/L%_playoff', 'Notes', 'coachName', 'coach_id', 'seasonStartYear'], axis = 1, inplace=True)

    """
    Define coach type
    """

    def defineCoachType(x):
        try:
            int(x)
            return "Head Coach"
        except:
            if x=='Player Development':
                return x
            else:
                return x.split('Coach')[0] + 'Coach'

    df['coachType'] = df.G_reg.apply(defineCoachType)

    """
    Cut observations that are not on the season level. E.g. some rows had information about the coach's career.
    """

    def isNumber(x):
        try:
            int(x)
            return True
        except:
            return False

    mask = df.G_reg.apply(isNumber)
    df = df[mask].copy()
    df.to_csv(dest_file, index=False)