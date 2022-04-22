import pandas as pd
import string

def scrapePlayersInfo(dest_file):
    df = pd.DataFrame(columns=['Player', 'From', 'To', 'Pos', 'Ht', 'Wt', 'Birth Date', 'Colleges'])
    for letter in list(string.ascii_lowercase): #
        url = 'https://www.basketball-reference.com/players/{}/'.format(letter)
        table = pd.io.html.read_html(url)[0]
        df = pd.concat([df, table])

    df.rename(columns={'Player':'playerName', 'Birth Date': 'birthDate'}, inplace=True)
    df.to_csv(dest_file, index=False)