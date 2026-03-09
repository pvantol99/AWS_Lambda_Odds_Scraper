"""
SofaScore stats fetcher and uploader to Postgres (public schema).
"""
import datetime
import json
import time
from bs4 import BeautifulSoup 
import unicodedata
import uuid
import requests
import pandas as pd
import numpy as np
from pandasql import sqldf
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
class SofaScore_Stats:

    # Rate limiting: min seconds between requests; backoff on 429
    DEFAULT_DELAY = 1.5
    DEFAULT_BACKOFF = 60  # seconds to wait on 429 before retry

    def __init__(self, request_delay=None, backoff_on_429=None):
        """
        request_delay: seconds to wait between API requests (default 1.5).
        backoff_on_429: seconds to wait when rate-limited before retry (default 60).
        """
        self._last_request_time = 0.0
        self._request_delay = request_delay if request_delay is not None else self.DEFAULT_DELAY
        self._backoff_on_429 = backoff_on_429 if backoff_on_429 is not None else self.DEFAULT_BACKOFF
        self._creds_loaded = False
        self._engine = None
        print('Initialize SofaScore Stats Instance')

    def get_creds(self):
        """Load DB credentials from env once and cache; build DATABASE_URL once."""
        if self._creds_loaded:
            return
        load_dotenv()
        self.USER = os.getenv('USER')
        self.PASSWORD = os.getenv('PASSWORD')
        self.HOST = os.getenv('HOST')
        self.PORT = os.getenv('PORT')
        self.DBNAME = os.getenv('DBNAME')
        if not all([self.USER, self.PASSWORD, self.HOST, self.PORT, self.DBNAME]):
            raise ValueError('Missing DB env vars: USER, PASSWORD, HOST, PORT, DBNAME')
        self._database_url = (
            f"postgresql+psycopg2://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DBNAME}?sslmode=require"
        )
        self._creds_loaded = True

    def _get_engine(self):
        """Return a single cached SQLAlchemy engine (reused for all DB operations)."""
        if self._engine is not None:
            return self._engine
        self.get_creds()
        self._engine = create_engine(self._database_url)
        return self._engine

    def _rate_limited_get(self, url, max_retries=3):
        """GET with delay between requests and retry with backoff on 429."""
        for attempt in range(max_retries):
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < self._request_delay:
                time.sleep(self._request_delay - elapsed)
            self._last_request_time = time.monotonic()
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 429:
                    if attempt < max_retries - 1:
                        time.sleep(self._backoff_on_429)
                        continue
                    r.raise_for_status()
                r.raise_for_status()
                return r
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(self._backoff_on_429)
        return None  # unreachable

    def query_season_id(self, season):
        self.get_creds()
        engine = self._get_engine()
        with engine.connect() as connection:
            self.season_id = connection.execute(
                text(f"""select DISTINCT id from public.tournaments
                        where name = '{season}' """)
            ).fetchone()[0]

    def get_games_in_round(self,season,rounds:list):
        self.query_season_id(season)
        round_info = []
        now = datetime.datetime.now()
        for round in rounds:
            print(round)
            response = self._rate_limited_get(f'https://www.sofascore.com/api/v1/unique-tournament/215/season/{self.season_id}/events/round/{round}')
            for game in response.json()['events']:
                game_id = game['id']
                round_info.append([game['id'],
                                game['season']['id'],
                                game['roundInfo']['round'],
                                game['homeTeam']['name'],
                                game['homeTeam']['id'],
                                game['awayTeam']['name'],
                                game['awayTeam']['id'],
                                datetime.datetime.fromtimestamp(game['startTimestamp']),
                                json.dumps(game['homeScore']),
                                json.dumps(game['awayScore']),
                                now
                                ]
                                )
                
            print(f'''Round {round} obtained''')

        self.round_df = pd.DataFrame(round_info)
        self.round_df.columns = ['game_id','tournament_id','round','home_team','home_team_id','away_team',
                                 'away_team_id','start_timestamp_et','home_team_statistics','away_team_statistics','updated_at']

    def upload_games(self, rounds: tuple, check_replace=True):
        engine = self._get_engine()
        if check_replace:
            with engine.connect() as connection:
                connection.execute(text(f"""
                    DELETE from public.game_instances
                    WHERE tournament_id = {self.season_id} and round in {rounds}
                """))
                connection.commit()
        table_name = 'game_instances'
        try:
            self.round_df.to_sql(table_name, engine, if_exists='append', index=False, schema='public')
        except Exception as e:
            print(f'Upload Game Details failed: {e}')
        print(f'Games successfully uploaded to {table_name}')
    
    def get_stats(self, game_id: list):
        now = datetime.datetime.now()
        player_stats = []
        for game in game_id:
            response_game = self._rate_limited_get(f'https://www.sofascore.com/api/v1/event/{game}/lineups')
            data = response_game.json()
            for side in ['home', 'away']:
                try:
                    for player in data[side]['players']:
                        player_stats.append([
                            int(str(game) + str(player['player']['id'])),
                            game,
                            player['teamId'],
                            side,
                            player['player']['name'],
                            player['player']['id'],
                            player['position'],
                            player['substitute'],
                            player['jerseyNumber'],
                            player['player']['country']['name'],
                            json.dumps(player['statistics']),
                            data[side]['formation'],
                            json.dumps(player['player']),
                            now,
                        ])
                except (KeyError, TypeError):
                    print('No data available for fixture: continue')
        self.game_df = pd.DataFrame(player_stats)
        self.game_df.columns = ['id', 'game_id', 'current_team_id', 'side', 'player_name', 'player_id', 'position', 'substitute', 'player_number',
                                'nationality', 'game_stats', 'formation', 'player_raw_data', 'updated_at']
        print(f'Game IDs {game_id} obtained')

    def upload_stats(self, game_id: tuple, check_replace=True):
        engine = self._get_engine()
        if check_replace:
            with engine.connect() as connection:
                connection.execute(text(f"""
                    DELETE from public.player_statistics
                    WHERE game_id in {game_id}
                """))
                connection.commit()
        table_name = 'player_statistics'
        try:
            self.game_df.to_sql(table_name, engine, if_exists='append', index=False, schema='public')
        except Exception as e:
            print(f'Upload Game Details failed: {e}')
        print(f'Stats successfully uploaded to {table_name}')

    def get_rounds_in_previous(self, days=7, tournament='Super League Switzerland 2024-2025'):
        self.query_season_id(season=tournament)
        engine = self._get_engine()
        with engine.connect() as connection:
            info = connection.execute(text(f"""
                SELECT DISTINCT round, game_id
                from public.game_instances
                WHERE tournament_id = {self.season_id}
                    and start_timestamp_et >= NOW() - INTERVAL '{days} Day' and start_timestamp_et < NOW()
            """)).fetchall()
        rounds = list(set(a[0] for a in info))
        game_ids = list(set(a[1] for a in info))
        return rounds, game_ids

    def get_sl_teams_fbt(self):
        """Super League team slugs -> SofaScore team ids (cached once)."""
        if not hasattr(SofaScore_Stats, '_SL_TEAMS_FBT'):
            SofaScore_Stats._SL_TEAMS_FBT = {
                'bsc-young-boys': 2445, 'fc-basel': 2501, 'fc-lugano': 2443,
                'fc-luzern': 2453, 'servette-fc': 2448, 'fc-lausanne-sport': 2446,
                'fc-sankt-gallen-1879': 2442, 'fc-zuerich': 2450, 'fc-sion': 2452,
                'yverdon-sport-fc': 2460, 'grasshopper-club-zuerich': 2449,
                'fc-winterthur': 2458,
            }
        return SofaScore_Stats._SL_TEAMS_FBT
    def clean_name(self,input_str, pattern, no_removal=False):
        if no_removal == False:
            ### only last name
            last_name = input_str[input_str.rfind(pattern) + 1:]
        else:
            last_name = input_str
        ### Remove accents
        last_name = last_name.replace('Đ','Dj') # Special Case

        nfkd_form = unicodedata.normalize('NFKD', last_name)
        name = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
        return name
         
    def get_roster_updates(self,tournament = 'Super League Switzerland 2024-2025'):
        if 'super league' in tournament.lower():
            teams_fbt = self.get_sl_teams_fbt()
        else:
            return ('Invalid Tournament: No Teams list found')
        
        ###### First: Find current roster (to account for transfers and other changes)
        self.rosters_and_injuries = pd.DataFrame()
        for key,val in teams_fbt.items():
            lineups = self._rate_limited_get(f'''https://www.sofascore.com/api/v1/team/{val}/players''')
            rosters = []
            for player in lineups.json()['players']:
                rosters.append([
                    player['player']['name'],
                    self.clean_name(player['player']['name'],' ',no_removal=True),
                    player['player']['shortName'],
                    self.clean_name(player['player']['shortName'],' '),
                    player['player']['shortName'][0],
                    player['player']['position'],
                    player['player']['id'],
                    val
                ])
            rosters = pd.DataFrame(rosters)
            rosters.columns = ['player_name','player_name_clean','player_shortname','player_shortname_clean','first_initial','position','player_id','team_id']

            ###### Second: Get injury updates from fussballtransfers.com

            injuries = self._rate_limited_get(f'''https://www.fussballtransfers.com/verein/{key}/kader/''')
            soup = BeautifulSoup(injuries.text,'html.parser')
            players = soup.find_all(attrs={'class': 'personCardCell__infos'})
            roster_injuries = []
            for i in range(0,len(players)):
                roster_injuries.append(
                [self.clean_name(players[i].find(attrs ={'class':'personCardCell__name'}).get_text().strip(),' '),
                players[i].find(attrs ={'class':'personCardCell__name'}).get_text().strip()[0],
                len(players[i].find(attrs ={'class':'personCardCell__description'}).find_all(attrs={'class':'personCardCell__icon personCardCell__icon--redCard'})) > 0,
                len(players[i].find(attrs ={'class':'personCardCell__description'}).find_all(attrs={'class':'personCardCell__icon personCardCell__icon--injury'})) > 0
                ])
            roster_injuries = pd.DataFrame(roster_injuries)
            roster_injuries.columns = ['player_shortname_clean','first_initial','suspension_flag','injury_flag']

            ### Merge everything
            output = sqldf(
                """
                SELECT DISTINCT
                    r.*,
                    coalesce(ri.suspension_flag, ri2.suspension_flag,ri3.suspension_flag) as suspension_flag,
                    coalesce(ri.injury_flag, ri2.injury_flag, ri3.injury_flag) as injury_flag
                FROM rosters r 
                LEFT JOIN roster_injuries ri
                    on (r.player_shortname_clean = ri.player_shortname_clean and r.first_initial = ri.first_initial)
                LEFT JOIN roster_injuries ri2
                    on (lower(r.player_name_clean) like '%' || lower(ri2.player_shortname_clean) || '%') and
                        (lower(r.player_name_clean) like '% ' || ri2.first_initial || '%' or
                            lower(substring(r.player_name_clean,1,1)) = lower(ri2.first_initial))    
                LEFT JOIN roster_injuries ri3 
                    on r.player_shortname_clean = ri3.player_shortname_clean
                """)
            output['suspension_flag'] = output['suspension_flag'].map({0.0:False, 1.0:True, np.nan:np.nan})
            output['injury_flag'] = output['injury_flag'].map({0.0:False, 1.0:True, np.nan:np.nan})
            
            self.rosters_and_injuries = pd.concat([self.rosters_and_injuries,output[['player_name','player_id','team_id','suspension_flag','injury_flag','position']]], ignore_index = True)
        print(f'''Rosters and Injuries succesfully pulled for {tournament}''')

    def upload_rosters_and_injuries(self, tournament='Super League Switzerland 2024-2025'):
        now = datetime.datetime.now()
        self.rosters_and_injuries['valid_from'] = now
        self.rosters_and_injuries['valid_to'] = None
        self.rosters_and_injuries['id'] = self.rosters_and_injuries.apply(
            lambda x: str(int(now.timestamp())) + str(x['player_id']), axis=1
        )
        self.rosters_and_injuries = self.rosters_and_injuries[
            ['id', 'team_id', 'player_id', 'player_name', 'injury_flag', 'suspension_flag', 'position', 'valid_from', 'valid_to']
        ]
        engine = self._get_engine()
        table_name = 'roster_availability'
        try:
            with engine.connect() as connection:
                connection.execute(text(f"""
                    UPDATE public.{table_name}
                    SET valid_to = '{now}'
                    WHERE valid_to is NULL
                """))
                connection.commit()
            self.rosters_and_injuries.to_sql(table_name, engine, if_exists='append', index=False, schema='public')
            print(f'Rosters successfully uploaded to {table_name}')
        except Exception as e:
            print(f'Upload Game Details failed: {e}')

    def get_pbp_subinfo(self,incident):
        json_dict = dict()
        if incident['incidentType'] == 'card':
            json_dict.update({
                'player_id' : incident.get('player',{}).get('id'),
                'player_name' : incident.get('player',{}).get('name'),
                'type_card' : incident.get('incidentClass'),
                'reason' : incident.get('reason'),
                'rescinded' : incident.get('rescinded')
            })
        elif incident['incidentType'] == 'substitution':
            json_dict.update({
                'player_id_in' : incident.get('playerIn',{}).get('id'),
                'player_name_in' : incident.get('playerIn',{}).get('name'),
                'player_id_out' : incident.get('playerOut',{}).get('id'),
                'player_name_out' : incident.get('playerOut',{}).get('name'),
                'injury' : incident.get('injury')
            })
        elif incident['incidentType'] == 'injuryTime':
            json_dict.update({
                'addedTime' : incident.get('addedTime')
            })
        elif incident['incidentType'] == 'goal':
            json_dict.update({
                'player_id_goal' : incident.get('player',{}).get('id'),
                'player_name_goal' : incident.get('player',{}).get('name'),
                'player_id_assist' : incident.get('assist1',{}).get('id'),
                'player_name_assist' : incident.get('assist1',{}).get('name'),
                'home_score' : incident.get('homeScore'),
                'away_score' : incident.get('awayScore'),
                'action_breakdown' : incident.get('footballPassingNetworkAction')
            })
        elif incident['incidentType'] == 'varDecision':
            json_dict.update({
                'player_id_check' : incident.get('player',{}).get('id'),
                'player_name_check' : incident.get('player',{}).get('name'),
                'type_check' : incident.get('incidentClass'),
                'confirmed' : incident.get('confirmed')
            })
        return json_dict
    
    def get_event_pbps(self,game_id:list):
        game_output = []
        now = datetime.datetime.now()
        for game in game_id:
            num = 0
            pbp = self._rate_limited_get(f'https://www.sofascore.com/api/v1/event/{game}/incidents')
            for incident in pbp.json()['incidents']:
                num += 1
                if incident['incidentType'] == 'period':
                    continue
                game_output.append([
                        str(game) + str(num),
                        game,
                        incident.get('incidentType'),
                        incident.get('time'),
                        incident.get('isHome'),
                        json.dumps(self.get_pbp_subinfo(incident)),
                        now])
                
        self.game_pbp = pd.DataFrame(game_output)
        self.game_pbp.columns = ['id','game_id','incident_type','time_mins','isHome','incident_sub_info','updated_at']
        print('Game PBPs successfully pulled')
            
        
    def upload_event_pbps(self, game_id: tuple, check_replace=True):
        engine = self._get_engine()
        if check_replace:
            with engine.connect() as connection:
                connection.execute(text(f"""
                    DELETE from public.game_events
                    WHERE game_id in {game_id}
                """))
                connection.commit()
        table_name = 'game_events'
        try:
            self.game_pbp.to_sql(table_name, engine, if_exists='append', index=False, schema='public')
        except Exception as e:
            print(f'Upload Game Details failed: {e}')
        print(f'PBPs successfully uploaded to {table_name}')

    def get_shotmaps(self, game_id: list):
        """Fetch shotmap data per game from SofaScore API. One request per game."""
        now = datetime.datetime.now()
        rows = []
        for game in game_id:
            try:
                r = self._rate_limited_get(f'https://www.sofascore.com/api/v1/event/{game}/shotmap')
                data = r.json()
                rows.append([game, json.dumps(data), now])
            except Exception as e:
                print(f'Shotmap not available for game {game}: {e}')
        self.shotmap_df = pd.DataFrame(rows)
        self.shotmap_df.columns = ['game_id', 'payload', 'updated_at']
        print(f'Shotmaps obtained for {len(rows)} games')

    def upload_shotmaps(self, game_id: tuple, check_replace=True):
        engine = self._get_engine()
        if check_replace:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    DELETE FROM public.event_shotmap
                    WHERE game_id IN {game_id}
                """))
                conn.commit()
        table_name = 'event_shotmap'
        try:
            self.shotmap_df.to_sql(table_name, engine, if_exists='append', index=False, schema='public')
        except Exception as e:
            print(f'Upload shotmaps failed: {e}')
            return
        print(f'Shotmaps successfully uploaded to {table_name}')

    def get_rating_breakdowns(self, game_id: list):
        """Fetch player rating-breakdown per game. Uses lineups to get player ids, then one request per player."""
        now = datetime.datetime.now()
        rows = []
        for game in game_id:
            try:
                r_lineups = self._rate_limited_get(f'https://www.sofascore.com/api/v1/event/{game}/lineups')
                lineups = r_lineups.json()
                for side in ('home', 'away'):
                    if side not in lineups or not lineups[side]:
                        continue
                    team_id = lineups[side].get('teamId')
                    for player in lineups[side].get('players', []):
                        pid = player.get('player', {}).get('id')
                        if not pid:
                            continue
                        try:
                            r_rating = self._rate_limited_get(
                                f'https://www.sofascore.com/api/v1/event/{game}/player/{pid}/rating-breakdown'
                            )
                            data = r_rating.json()
                            rows.append([game, pid, team_id, side, json.dumps(data), now])
                        except Exception as e:
                            print(f'Rating breakdown not available for game {game} player {pid}: {e}')
            except Exception as e:
                print(f'Lineups or rating breakdown failed for game {game}: {e}')
        self.rating_breakdown_df = pd.DataFrame(rows)
        self.rating_breakdown_df.columns = ['game_id', 'player_id', 'team_id', 'side', 'payload', 'updated_at']
        print(f'Rating breakdowns obtained for {len(rows)} player-games')

    def upload_rating_breakdowns(self, game_id: tuple, check_replace=True):
        engine = self._get_engine()
        if check_replace:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    DELETE FROM public.player_rating_breakdown
                    WHERE game_id IN {game_id}
                """))
                conn.commit()
        table_name = 'player_rating_breakdown'
        try:
            self.rating_breakdown_df.to_sql(table_name, engine, if_exists='append', index=False, schema='public')
        except Exception as e:
            print(f'Upload rating breakdowns failed: {e}')
            return
        print(f'Rating breakdowns successfully uploaded to {table_name}')

    def refresh_game_and_stats(self, tournament, days=7, include_shotmaps_and_ratings=False):
        #Get rounds and game ids to refresh
        rounds, game_ids = self.get_rounds_in_previous(days=days, tournament=tournament)

        #Refresh rounds
        self.get_games_in_round(season=tournament, rounds=rounds)
        self.upload_games(tuple(rounds + [-1]), check_replace=True)

        #Refresh games
        self.get_stats(game_id=game_ids)
        self.upload_stats(tuple(game_ids + [-1]), check_replace=True)

        #Refresh play by plays
        self.get_event_pbps(game_id=game_ids)
        self.upload_event_pbps(tuple(game_ids + [-1]), check_replace=True)

        if include_shotmaps_and_ratings:
            self.get_shotmaps(game_id=game_ids)
            self.upload_shotmaps(tuple(game_ids + [-1]), check_replace=True)
            self.get_rating_breakdowns(game_id=game_ids)
            self.upload_rating_breakdowns(tuple(game_ids + [-1]), check_replace=True)
    
    @property
    def get_season_id(self):
        return self.season_id
    
    @property
    def get_games(self):
        return self.round_df

    @property
    def get_game_stats(self):
        return self.game_df
    
    @property
    def get_rosters_and_injuries(self):
        return self.rosters_and_injuries
    
    @property
    def get_game_pbp(self):
        return self.game_pbp

    @property
    def get_shotmap_df(self):
        return getattr(self, 'shotmap_df', None)

    @property
    def get_rating_breakdown_df(self):
        return getattr(self, 'rating_breakdown_df', None)