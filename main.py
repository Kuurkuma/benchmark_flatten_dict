import copy
import time
import tracemalloc
from functools import wraps
import json
import gc

import dlt
import pandas as pd
from flatdict import FlatDict


def generate_sample_data(num_players):
    """
    Generates a match_details dictionary with rugby player individual stats.
    This imitate a real-life API endpoint.
    """
    players = []
    for i in range(1, num_players + 1):
        if 1 <= i <= 8:
            position = "Forward"
        elif 9 <= i <= 15:
            position = "Back"
        else:  # Substitutes doesnt matter for the example
            position = "Substitute"
        # logic to define if player is a substitute
        is_substitute = i > 15

        player = {
            "player_id": i,
            "name": f"Player {i}",
            "position": position,
            "substitute": is_substitute,
            "match_stats": {
                "points": (i % 3) * 5,
                "tries": i % 3,
                "turnovers_conceded": i % 4,
                "offload": i % 5,
                "dominant_tackles": i % 10,
                "missed_tackles": i % 5,
                "tackle_success": round(0.85 + (i % 15) / 100, 2),
                "tackle_try_saver": i % 2,
                "tackle_turnover": i % 3,
                "penalty_goals": i % 2,
                "missed_penalty_goals": i % 2,
                "conversion_goals": i % 4,
                "missed_conversion_goals": i % 4,
                "drop_goals_converted": i % 1,
                "drop_goal_missed": i % 2,
                "runs": i % 20 + 5,
                "metres": (i % 20 + 5) * 8,
                "clean_breaks": i % 4,
                "defenders_beaten": i % 6,
                "try_assists": i % 2,
                "passes": i % 30 + 10,
                "bad_passes": i % 5,
                "rucks_won": i % 15,
                "rucks_lost": i % 3,
                "lineouts_won": i % 4,
                "penalties_conceded": i % 3,
            },
        }
        players.append(player)

    return {
        "match_id": 12345,
        "date": "2025-07-27",
        "venue": "Small Mem Stadium",
        "home": {
            "team_id": 101,
            "team_name": "The Bloody Ingestors",
            "teamsheet": players,
        },
        "away": {},
    }


# --- Benchmarking Setup ---
results_list = []


def benchmark(func):
    """Decorator to measure and store performance."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        tracemalloc.start()
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        results_list.append(
            {
                "function": func.__name__,
                "time_in_s": end_time - start_time,
                "memory_in_mb": peak / 10**6,
            }
        )
        return result

    return wrapper


# --- Flattening Functions ---
## Native python


@benchmark
def manual_flatten(match_details):
    """
    A pure nonesense manual function that flattens the player stats using a list comprehension.
    Used as reference for very static way !
    """
    
    player_list = match_details.get('home', {}).get('teamsheet', [])

    player_stats = [
        {
            'player_id': player.get('player_id'),
            'name': player.get('name'),
            'position': player.get('position'),
            'substitute': player.get('substitute'),
            'points': player.get('match_stats', {}).get('points', None),
            'tries': player.get('match_stats', {}).get('tries', None),
            'turnovers_conceded': player.get('match_stats', {}).get('turnovers_conceded', None),
            'offload': player.get('match_stats', {}).get('offload', None),
            'dominant_tackles': player.get('match_stats', {}).get('dominant_tackles', None),
            'missed_tackles': player.get('match_stats', {}).get('missed_tackles', None),
            'tackle_success': player.get('match_stats', {}).get('tackle_success', None),
            'tackle_try_saver': player.get('match_stats', {}).get('tackle_try_saver', None),
            'tackle_turnover': player.get('match_stats', {}).get('tackle_turnover', None),
            'penalty_goals': player.get('match_stats', {}).get('penalty_goals', None),
            'missed_penalty_goals': player.get('match_stats', {}).get('missed_penalty_goals', None),
            'conversion_goals': player.get('match_stats', {}).get('conversion_goals', None),
            'missed_conversion_goals': player.get('match_stats', {}).get('missed_conversion_goals', None),
            'drop_goals_converted': player.get('match_stats', {}).get('drop_goals_converted', None),
            'drop_goal_missed': player.get('match_stats', {}).get('drop_goal_missed', None),
            'runs': player.get('match_stats', {}).get('runs', None),
            'metres': player.get('match_stats', {}).get('metres', None),
            'clean_breaks': player.get('match_stats', {}).get('clean_breaks', None),
            'defenders_beaten': player.get('match_stats', {}).get('defenders_beaten', None),
            'try_assists': player.get('match_stats', {}).get('try_assists', None),
            'passes': player.get('match_stats', {}).get('passes', None),
            'bad_passes': player.get('match_stats', {}).get('bad_passes', None),
            'rucks_won': player.get('match_stats', {}).get('rucks_won', None),
            'rucks_lost': player.get('match_stats', {}).get('rucks_lost', None),
            'lineouts_won': player.get('match_stats', {}).get('lineouts_won', None),
            'penalties_conceded': player.get('match_stats', {}).get('penalties_conceded', None)
        }
        for player in player_list
    ]

    return player_stats

# this time, we are reasonable and use the unpack operator '**'. Less lines of code, indeed !
@benchmark
def unpack_operator_flatten(match_details):
    """
    A manual function that flattens the player stats using 
    a pythonic list comprehension method and the unpack operator
    """
    player_list = match_details.get('home', {}).get('teamsheet', [])

    player_stats = [
        {
            'player_id': player.get('player_id'),
            'name': player.get('name'),
            'position': player.get('position'),
            'substitute': player.get('substitute'),
            **player.get('match_stats', {}),         # nice! unpacking operator here for unesting the dictionary
        }
        for player in player_list
    ]

    return player_stats

# generator function
@benchmark
def generator_flatten(match_details):
    """
    An application-specific function that uses the python generator
    to process the player data.
    """
    from collections.abc import MutableMapping

    def flatten_gen(d, parent_key, sep):
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                yield from flatten_gen(v, new_key, sep)
            else:
                yield new_key, v

    # This is the user-facing utility function that starts the process.
    def flatten_dict(d: MutableMapping, parent_key: str = "", sep: str = "_"):
        """A generic utility to flatten a single dictionary."""
        return dict(flatten_gen(d, parent_key, sep))

    player_list = match_details.get('home', {}).get('teamsheet', [])
   # Process each player with a list comprehension
    player_stats = [
        {**{k: v for k, v in player.items() if k != 'match_stats'}, 
         **flatten_dict(player['match_stats'], sep='_')} 
        for player in player_list 
        if 'match_stats' in player and isinstance(player['match_stats'], MutableMapping)
    ]
   
    return player_stats


## Library-based functions

# pandas !
@benchmark
def pandas_flatten(match_details):
    """Flattens player stats using pandas."""

    player_list = match_details.get('home', {}).get('teamsheet', [])
    df = pd.json_normalize(player_list, sep='_')
    df = df.rename(columns=lambda x: x.replace('match_stats_', ''))

    return df.to_dict(orient='records')

# FlatDict
@benchmark
def flatdict_flatten(match_details):
    """Flattens player stats using the flatdict library."""

    player_list = match_details.get('home', {}).get('teamsheet', [])
    player_stats = [
        player.update(FlatDict(player.pop('match_stats', {}), delimiter='_')) or player
        for player in player_list
    ]

    return player_stats


## dlt Pipeline Function - note: dlt is more a ingestion tool for building pipeline
@benchmark
def dlt_flatten(match_details):
    """
    Flattens player stats using dlt pipeline structure.
    """

    @dlt.source(name="match_details_endpoint")
    def match_source(data):
        @dlt.resource(name="players_teamsheet", write_disposition="replace")
        def players_resource():
            yield data.get("home", {}).get(
                "teamsheet", []
            )  # use yield instead yield from, will be faster

        @dlt.transformer
        def player_stats(players):
            """Takes a player dict, unnests 'match_stats', and yields the result."""

            @dlt.defer  # when removed then 100k rows is processd faster
            def _get_player_stats(_player):
                if "match_stats" in _player:
                    stats = _player.pop(
                        "match_stats", {}
                    )  # seems that if yield from used then it's a str and .pop does not exist
                    _player.update(stats)
                return _player

            for _player in players:
                yield _get_player_stats(_player)

        return (
            players_resource | player_stats
        )  # pass players directly to the transformer

    pipeline = match_source(match_details)

    return list(pipeline)


# --- Main Comparison Function ---
def run_and_compare(data):
    """
    Runs all benchmarked functions and returns the results in a DataFrame.
    The global results_list is cleared for each run.
    """
    global results_list
    results_list = []  # Reset results for a clean run for this specific data size

    gc.collect()
    # Execute each function with its own copy of the data to populate the results list
    pandas_data = cp.deepcopy(data)
    pandas_flatten(pandas_data)
    gc.collect()
    
    manual_data = cp.deepcopy(data)
    manual_flatten(manual_data)
    gc.collect()

    generator_data = cp.deepcopy(data)
    generator_flatten(generator_data)
    gc.collect()

    unpack_data = cp.deepcopy(data)
    unpack_operator_flatten(unpack_data)
    gc.collect
    
    flatdict_data = cp.deepcopy(data)
    flatdict_flatten(flatdict_data)
    gc.collect()
    
    dlt_data = cp.deepcopy(data)
    dlt_flatten(dlt_data)

    # Create a DataFrame to perform further analysis
    df_results = pd.DataFrame(results_list)
    return df_results


# --- Main Execution Block ---
if __name__ == "__main__":
    player_counts = [23, 100, 1000, 10000, 100000]

    all_results_dfs = []

    print("=" * 50)
    print("  Starting Benchmark Suite for Multiple Data Sizes")
    print("=" * 50)

    # Loop over each defined player count
    for num in player_counts:
        print(f"\n--- Running benchmarks for {num} players ---")
        match_data = generate_sample_data(num_players=num)
        results_df_for_num = run_and_compare(match_data)
        # Add a column to identify the data size for this batch of results
        results_df_for_num["num_players"] = num

        all_results_dfs.append(results_df_for_num)
        print(results_df_for_num.sort_values(by="memory_in_mb").to_string())

    print("\n" + "=" * 50)
    print("AGGREGATED BENCHMARK RESULTS - by Memory usage")
    print("=" * 50 + "\n")

    final_comparison_df = pd.concat(all_results_dfs, ignore_index=True)

    final_comparison_df = final_comparison_df[['num_players', 'function', 'time_in_s', 'memory_in_mb']]
    print(final_comparison_df.sort_values(by=['num_players', 'memory_in_mb']).to_string())
    
    # save to json for further analyis
    final_comparison_df.to_json("data/benchmark_results.json", orient='records')
