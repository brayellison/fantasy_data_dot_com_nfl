from copy import deepcopy

import requests
import pandas as pd
from joblib import Parallel, delayed

BASE_URL = "https://fantasydata.com/NFL_FantasyStats/FantasyStats_Read"


class Options:

    def __init__(self, pageSize=None, group=None, filter=None, filters_position=None, filters_team=None,
                 filters_teamkey=None, filters_season=None, filters_seasontype=None, filters_cheatsheettype=None,
                 filters_scope=None, filters_subscope=None, filters_redzonescope=None, filters_scoringsystem=None,
                 filters_leaguetype=None, filters_searchtext=None, filters_week=None, filters_startweek=None,
                 filters_endweek=None, filters_minimumsnaps=None, filters_teamaspect=None, filters_stattype=None,
                 filters_exportType=None, filters_desktop=None, filters_dfsoperator=None, filters_dfsslateid=None,
                 filters_dfsslategameid=None, filters_dfsrosterslot=None, filters_page=None, filters_showfavs=None,
                 filters_posgroup=None, filters_oddsstate=None, filters_showall=None, filters_aggregatescope=None,
                 filters_rangescope=None, filters_range=None):
        self.sort = "FantasyPoints-desc"
        self.pageSize = pageSize
        self.group = group
        self.filter = filter
        self.filters_position = filters_position
        self.filters_team = filters_team
        self.filters_teamkey = filters_teamkey
        self.filters_season = filters_season
        self.filters_seasontype = filters_seasontype
        self.filters_cheatsheettype = filters_cheatsheettype
        self.filters_scope = filters_scope
        self.filters_subscope = filters_subscope
        self.filters_redzonescope = filters_redzonescope
        self.filters_scoringsystem = filters_scoringsystem
        self.filters_leaguetype = filters_leaguetype
        self.filters_searchtext = filters_searchtext
        self.filters_week = filters_week
        self.filters_startweek = filters_startweek
        self.filters_endweek = filters_endweek
        self.filters_minimumsnaps = filters_minimumsnaps
        self.filters_teamaspect = filters_teamaspect
        self.filters_stattype = filters_stattype
        self.filters_exportType = filters_exportType
        self.filters_desktop = filters_desktop
        self.filters_dfsoperator = filters_dfsoperator
        self.filters_dfsslateid = filters_dfsslateid
        self.filters_dfsslategameid = filters_dfsslategameid
        self.filters_dfsrosterslot = filters_dfsrosterslot
        self.filters_page = filters_page
        self.filters_showfavs = filters_showfavs
        self.filters_posgroup = filters_posgroup
        self.filters_oddsstate = filters_oddsstate
        self.filters_showall = filters_showall
        self.filters_aggregatescope = filters_aggregatescope
        self.filters_rangescope = filters_rangescope
        self.filters_range = filters_range

    def __str__(self):
        return "&".join(["sort=FantasyPoints-desc"] + [f'{x.replace("_", ".")}={y}' for x, y in self.tuples()])

    def __repr__(self):
        inner_str = ", ".join([f"{x}={y}" for x, y in self.tuples()])
        return f"Options({inner_str})"

    def tuples(self):
        return [(x, y) for x, y in self.__dict__.items() if y is not None and x != "sort"]


def generate_with_options(options):
    resp = requests.get(f'{BASE_URL}?{options}').json()
    if resp['Total'] == 0:
        return None
    return pd.DataFrame(resp['Data'])


def generate_data(*options):
    frames = [generate_with_options(opt) for opt in options]
    frames = [x for x in frames if x is not None]
    return pd.concat(frames) if frames else None


base_options = dict(
    pageSize="3000",
    filters_season="2019",
    filters_seasontype="1",
    filters_scope="2",
    filters_subscope="1",
    filters_aggregatescope="1",
    filters_range="1",
    filters_scoringsystem="7"
)


def season_stats(*seasons, **other_options):
    seasons_data = []
    for season in seasons:
        other_options.update({"filters_season": season})
        week_num = 1
        while True:
            week_opts = Options(filters_startweek=week_num, filters_endweek=week_num, **other_options)
            week_data = generate_data(week_opts)
            if week_data is None:
                break
            week_num += 1
            seasons_data.append(week_data)
    return pd.concat(seasons_data)


if __name__ == "__main__":
    seasons = range(2002, 2020)
    other_options = deepcopy(base_options)
    get_it = Parallel(n_jobs=-1, verbose=15)(
        delayed(season_stats)(season, **other_options) for season in seasons
    )
