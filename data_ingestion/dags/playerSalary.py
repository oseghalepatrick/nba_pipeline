import pandas as pd
import string
from datetime import date

def scrapeSalary(dest_file):
    """
    I scrape the data for seasons 1996-97 to last year season. 
    """
    df = pd.DataFrame(columns=['playerName', 'seasonStartYear', 'salary', 'inflationAdjSalary'])
    current_yr = int(date.today().strftime('%Y'))
    for year in range(1997, current_yr):
        url = 'https://hoopshype.com/salaries/players/{}-{}/'.format(year-1, year)
        table = pd.io.html.read_html(url)[0]
        table.drop(columns=['Unnamed: 0'], inplace=True)
        table.set_axis(['playerName', 'salary', 'inflationAdjSalary'], axis=1, inplace=True)
        table['seasonStartYear'] = year - 1
        df = pd.concat([df, table])

    # Get the current season salary
    table = pd.io.html.read_html('https://hoopshype.com/salaries/players/')[0]
    table = table.iloc[:, [1,2]]
    table.set_axis(['playerName', 'salary'], axis=1, inplace=True)
    table['seasonStartYear'] = current_yr - 1
    df = pd.concat([df, table])
    df.to_csv(dest_file, index=False)