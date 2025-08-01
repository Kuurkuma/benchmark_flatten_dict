import time
import tracemalloc
from functools import wraps

import dlt
import pandas as pd
from flatdict import FlatDict


def generate_sample_data(num_players):
    """
    Generates a larger match_details dictionary with rugby player individual stats.
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
    A specific manual function that flattens the player stats.
    """
    player_stats = []
    player_list = match_details.get("home", {}).get("teamsheet", [])

    for player in player_list:
        flat_stats = player.get("match_stats", {})
        player_row = {
            "player_id": player.get("player_id"),
            "name": player.get("name"),
            "position": player.get("position"),
            "substitute": player.get("substitute"),
            **flat_stats,
        }
        player_stats.append(player_row)
    return player_stats


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

    player_list = match_details.get("home", {}).get("teamsheet", [])
    player_stats = []

    for player in player_list:
        if "match_stats" in player and isinstance(
            player["match_stats"], MutableMapping
        ):
            nested_stats = player.pop("match_stats")
            flat_stats = flatten_dict(nested_stats, sep="_")
            player.update(flat_stats)

        player_stats.append(player)

    return player_stats


## Library-based functions


@benchmark
def pandas_flatten(match_details):
    """Flattens player stats using pandas."""

    player_list = match_details.get("home", {}).get("teamsheet", [])
    df = pd.json_normalize(player_list, sep="_")
    df = df.rename(columns=lambda x: x.replace("match_stats_", ""))
    return df.to_dict(orient="records")


@benchmark
def flatdict_flatten(match_details):
    """Flattens player stats using the flatdict library."""

    player_stats = []
    player_list = match_details.get("home", {}).get("teamsheet", [])
    for player in player_list:
        stats = player.pop("match_stats", {})
        flat_stats = FlatDict(stats, delimiter="_")
        player.update(flat_stats)
        player_stats.append(player)

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

            @dlt.defer
            def _get_player_stats(_player):
                if "match_stats" in _player:
                    stats = _player.pop("match_stats", {})
                    _player.update(stats)
                return _player

            for _player in players:
                yield _get_player_stats(_player)
                print(_player)

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

    # Execute each function to populate the results list
    pandas_flatten(data)
    manual_flatten(data)
    generator_flatten(data)
    flatdict_flatten(data)
    dlt_flatten(data)

    # Create a DataFrame from the results for this run
    df_results = pd.DataFrame(results_list)

    # Return the dataframe so it can be collected
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
    # Reorder columns for better readability
    final_comparison_df = final_comparison_df[
        ["num_players", "function", "time_in_s", "memory_in_mb"]
    ]

    print(
        final_comparison_df.sort_values(by=["num_players", "memory_in_mb"]).to_string()
    )

    final_comparison_df.to_json("data/benchmark_results.json", orient="records")
