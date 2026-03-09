import nba_api
import requests
import pandas as pd
import time as t
from datetime import datetime
import random 
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.static import teams 
class MysteryBoxscore:
    
    def __init__(self):
        self.teams = teams.get_teams()
        print('MB Instance')

        
    @property
    def get_game_details_ordered(self):
        return self.game_details_ordered
    
    @property
    def get_boxscore(self):
        return self.bxscr
    
    @property
    def get_sample_game_stats(self):
        return self.sample_game
    

    def name_finder(team_id):
        for d in teams:
            if d['id'] == team_id:
                return d[['full_name','team_id']]
        print('No Teams Found')
    
    def nba_dict_to_df(dict_):
        df = pd.DataFrame(dict_['resultSets'][0]['rowSet'])
        df.columns = dict_['resultSets'][0]['headers'] 
        return df

    
    def randomize_game(self,min_season):
        team_id_of_day = teams.get_teams()[random.randint(0,29)]['id'] #Get random team
        games_dict = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id_of_day).get_dict() #Get Games
        games = MysteryBoxscore.nba_dict_to_df(games_dict)
        sample_game = games[games['GAME_DATE'] >= min_season + '-09-01'].sample()
        self.sample_game = sample_game[['SEASON_ID','GAME_ID','GAME_DATE','MATCHUP','PTS','PLUS_MINUS']].copy()

        return 'Sample Created' #self.sample_game
    
    def pull_boxscore(self):
        self.bxscr = MysteryBoxscore.nba_dict_to_df(boxscoretraditionalv2.BoxScoreTraditionalV2(game_id = self.sample_game['GAME_ID']).get_dict())
        self.bxscr.columns = ['game_id','team_id','team_abbreviation','team_city','player_id','player_name','nickname','start_position',
                              'comment','minutes','fg_made','fg_attempted','fg_percentage','3pt_made','3pt_attempted','3pt_percentage',
                              'ft_made','ft_attempted','ft_percentage','o_reb','d_reb','rebounds','assists','steals','blocks','turnovers','points',
                              'personal_fouls','plus_minus']
        return 'Boxscore Found' #self.bxscr
    
    def get_game_details(self):
        game_details = self.sample_game[['SEASON_ID','GAME_ID','GAME_DATE','MATCHUP','PTS','PLUS_MINUS']].copy()
        game_details['home_team'], game_details['away_team'] = self.bxscr['team_abbreviation'].unique()
        game_details['home_team_id'], game_details['away_team_id'] = self.bxscr['team_id'].unique()
        game_details['away_pts'] = int((game_details['PTS'] - game_details['PLUS_MINUS']).iloc[0])
        game_details.columns =  ['season_id','game_id','game_date','matchup','home_pts','plus_minus','home_team','away_team','home_team_id','away_team_id','away_pts']
        self.game_details_ordered = game_details[['season_id','game_id','game_date','matchup','home_team_id','home_team','home_pts','away_team_id','away_team','away_pts']].copy()
        return 'Game Details summarized'
    
    def get_game(self,min_season):
        self.randomize_game(min_season=min_season)
        self.pull_boxscore()
        self.get_game_details()
        return 'Game successfully pulled'
        
    
    def clean_game_details(self,max_id_int):
        df_gd = self.get_game_details_ordered.copy()
        df_gd['id'] = max_id_int + 1
        df_gd['valid_from'] = datetime.datetime.now()
        df_gd['valid_to'] = None
        self.df_gd_insert = df_gd[['id','valid_from','valid_to','game_id','game_date','home_team',
                                'home_team_id','home_pts','away_team','away_team_id','away_pts']]
        
    def clean_boxscore(self,max_id_int):
        df_bs = self.get_boxscore.copy()
        df_bs['id'] = max_id_int + 1
        df_bs['minutes'] = df_bs['minutes'].str[:2] + df_bs['minutes'].str[-3:]
        df_bs['instance_player_id'] = (df_bs['id'].astype(str) + df_bs['player_id'].astype(str)).astype(int)
        self.df_bs_insert = df_bs[['id','instance_player_id','game_id','team_id','player_id','player_name','start_position',
                                'minutes','fg_made','fg_attempted','fg_percentage','3pt_made','3pt_attempted',
                                '3pt_percentage','ft_made','ft_attempted','ft_percentage','o_reb','d_reb','rebounds','assists',
                                'steals','blocks','turnovers','points','personal_fouls']]
    
    def upload_to_db(self):
        # Fetch variables
        USER = 'postgres.zbxnnskeboyenigadawi'
        PASSWORD = 'HTIIWPDArKFp2fHv'
        HOST = 'aws-0-us-east-2.pooler.supabase.com'
        PORT = 6543
        DBNAME = 'postgres'

        # Construct the SQLAlchemy connection string
        try:
            DATABASE_URL = f"postgresql+psycopg2://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DBNAME}?sslmode=require"
        except:
            print('Connection Failed')  

        # Create the SQLAlchemy engine & session
        engine = create_engine(DATABASE_URL)

        # Get max existent id in game details
        connection = engine.connect()
        max_id = connection.execute(text(f'select max(id) as maxid from public.f2p_nba_game_details'))
        max_id_int = max(x for x in [max_id.fetchone()[0],0] if x is not None)

        # Create dataframe from game details
        self.clean_game_details(max_id_int)
        print('DF Game Details Created succesfully')

        # Upload Data frame Game details
        table_name = 'f2p_nba_game_details'
        try:
            self.df_gd_insert.to_sql(table_name,engine,if_exists = 'append',index=False)
        except Exception as e:
            print(f'Upload Game Details failed: {e}')

        print('DF Game Details successfully uploaded')

        # Update valid to column
        connection.execute(text(f"""
                        UPDATE public.f2p_nba_game_details
                        SET valid_to = (select DISTINCT valid_from from public.f2p_nba_game_details where id = {max_id_int})
                        WHERE valid_to is NULL and id != {max_id_int};
                        """))
        connection.commit()
        connection.close()
        # Create dataframe from box score
        self.clean_boxscore(max_id_int)
        print('DF Boxscore Created succesfully')
        
        

        # Upload Data frame Box score
        table_name = 'f2p_nba_boxscores'
        try:
            self.df_bs_insert.to_sql(table_name,engine,if_exists = 'append',index=False)
        except Exception as e:
            print(f'Upload Boxscore failed: {e}')

        print('DF Boxscore successfully uploaded')