#import packages for scraping and cleaning the data
import pandas as pd
import requests
from splinter import Browser

#open browser that will find the correct pages to scrape.
#you will need to change the chromedriver location to match your computer.
executable_path = {'executable_path': '/bin/chromedriver.exe'}
browser = Browser('chrome', **executable_path, headless = True)

#open browser that will find the correct pages to scrape.
#you will need to change the chromedriver location to match your computer.
executable_path = {'executable_path': '/bin/chromedriver.exe'}
browser = Browser('chrome', **executable_path, headless = True)

#Creates the dataframe we will store the teams data in. this line isn't actually neccesary.
team_df = pd.DataFrame()

#loads in first url to visit with a list of links to all NBA teams past and present
browser.visit(f'https://www.basketball-reference.com/teams/')
#this is so we can have the full team name in the database name.
Full_Team = browser.links.find_by_partial_text(team).first.text
Team_Name = Full_Team.split(" ")[1]

#Clicks the link for the team you picked when asked for input
browser.links.find_by_partial_text(Full_Team).click()

#scrapes the website the browser is currently opened on for tables of data
team_table = pd.read_html(browser.html)
#selects which table we want to become the dataframe we created earlier
team_df = team_table[0]

#cleans the newly scraped table and creates an column that will be used as a foriegn key later 
team_df = team_df.drop(columns=["Lg",  "Unnamed: 8", "Unnamed: 15", "Top WS", "SRS", "Rel Pace", "ORtg", "Rel ORtg",
                               "DRtg", "Rel DRtg"])
team_df.drop(team_df[team_df['Season'] == 'Season'].index, inplace = True)
team_df = team_df.reset_index(drop=False)
team_df = team_df.rename(columns = {"index":"Season_Id", "W/L%":"Win_Pct"}

#resets the browser to the originaly site, this helps limit some errors down the line
browser.visit(f'https://www.basketball-reference.com/teams/')

#same as above just here to limit errors
browser.links.find_by_partial_text(Full_Team).click()

#creates dataframes for roster and season stat, these need to be created outside the for loop below
All_Players = pd.DataFrame() 
Season_totals = pd.DataFrame()

#finds the number of seasons the for loop will need to iterate through
seasons = team_df["Season"].count()

#find the first year of the franchises history so we can find the correct link
year1 = team_df.iloc[-2]['Season']
Syear = year1.split('-')[0]

#for loop that will scrape each season for which ever team you selected
for y in range(seasons):
    #again so we can find the correct link
    year = int(Syear) + y
    #helps us track where issues may have arisen if we get an error
    print(year)
    #selects the link that will take we will need to scrape data from for each individual season
    browser.links.find_by_partial_href(f'/{year}.html').click()
    #scrapes the table for the current year of the iteration
    roster_table = pd.read_html(browser.html)
    #selects the roster table 
    Roster = roster_table[0]
    #if else statement needed as the current year has on additional table on the website for currently injured players
    if year == 2021:
        totals = roster_table[6]
        totals['year'] = year
    else:
        totals = roster_table[5]
        totals['year'] = year
    #adds the current iterations data to the dataframes we created earlier
    All_Players = pd.concat([All_Players, Roster])
    Season_totals = pd.concat([Season_totals, totals])

#closes browser since we have scraped all of the data we will be using
browser.quit()

#drops any duplicate players so each player only has one instance on this table.
All_Players = All_Players.drop_duplicates(subset=["Player"])
#resets the index and creates an ID column we can use for a foreign key later
All_Players = All_Players.reset_index(drop=True)
All_Players = All_Players.reset_index(drop=False)

#cleans data so we have good column names and no unneccesary columns
All_Players = All_Players.rename(columns = {"Unnamed: 6":"Country", "index":"Player_Id", "Birth Date":"Birth_Date", "No.":"Num"})
All_Players = All_Players.drop(columns = ["Exp"])

#create dataframes that will allow us to add foreign keys to Season_Totals dataframe
Player_Ids = All_Players[{"Player_Id", "Player"}]
Season_Ids = team_df[{"Season","Season_Id"}]

#cleans data and resets index also removes summary rows
Season_totals = Season_totals.reset_index(drop =True)
Season_totals = Season_totals.rename(columns = {"Unnamed: 1":"Player", "3P": "Three", "3PA": "ThreeA", 
                                                "2P":"Two", "2PA":"TwoA"})
Season_totals.drop(Season_totals[Season_totals['Player'] == 'Team Totals'].index, inplace=True)
Season_totals = Season_totals.drop(columns = ["Rk", "FG%", "3P%", "2P%", "eFG%", "FT%", "TRB"])

#Splits up the season row so that it is no longer formatted as yyyy-yy
Season_Ids[['year', 'Season_e']] = Season_Ids.Season.str.split("-",expand = True)

#changes variable type to we can make it easier to merge with another dataframe
Season_Ids['year'] =Season_Ids['year'].astype('int64')
#Adds one to the year column so it will match with the column from another dataframe
Season_Ids['year'] +=1

#adds season id #s to Season_Totals column
Season_Totals = pd.merge(Season_totals, Season_Ids, on = ['year', 'year'])

#adds player id #s to Season_Total column
Season_Totals = pd.merge(Season_Totals, Player_Ids, on = ['Player', 'Player'])

#Drops any unneccesary columns so there will be no duplicate info in the entire data base
Season_Totals = Season_Totals.drop(columns=["Player",  "year", "Season", "Season_e"])

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float

base = declarative_base()


class Team_Stats(base):
    __tablename__ = "Team_Stats"
    Season_Id = Column(Integer, primary_key = True)
    Season = Column(String(7))
    Team = Column(String(50))
    W = Column(String(3))
    L = Column(String(3))
    Win_Pct = Column(String(8))
    Pace = Column(String(8))
    Finish = Column(String(15))
    Playoffs = Column(String(100))
    Coaches = Column(String(100))
    

class Player_Stats(base):
    __tablename__ = "Player_Stats"
    Id = Column(Integer, primary_key = True)
    Player_Id = Column(Integer)
    Season_Id = Column(Integer)
    Age = Column(Integer)
    G = Column(Integer)
    GS = Column(Integer)
    MP = Column(Integer)
    FG = Column(Integer)
    FGA = Column(Integer)
    Three = Column(Integer)
    ThreeA = Column(Integer)
    Two = Column(Integer)
    TwoA = Column(Integer)
    FT = Column(Integer)
    FTA = Column(Integer)
    ORB = Column(Integer)
    DRB = Column(Integer)
    AST = Column(Integer)
    STL = Column(Integer)
    BLK = Column(Integer)
    TOV = Column(Integer)
    PF = Column(Integer)
    PTS = Column(Integer)

class Roster(base):
    __tablename__ = "Roster"
    Player_Id = Column(Integer, primary_key = True)
    Num = Column(Integer)
    Player = Column(String(100))
    Pos = Column(String(25))
    Ht = Column(String(10))
    Wt = Column(Integer)
    Birth_Date = Column(String(20))
    Country = Column(String(50))
    College = Column(String(200))

engine =  create_engine(f'sqlite:///data/{Team_Name}.sqlite')
conn = engine.connect()

base.metadata.drop_all(engine)
base.metadata.create_all(engine)

from sqlalchemy.orm import Session

session = Session(bind = engine)

All_Players.to_sql(name = "Roster", con = engine, if_exists = "append", index = False)

team_df.to_sql(name = "Team_Stats", con = engine, if_exists = "append", index = False)

Season_Totals.to_sql(name = "Player_Stats", con = engine, if_exists = "append", index = False)