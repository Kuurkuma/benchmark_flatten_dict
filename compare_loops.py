import time
import tracemalloc
from functools import wraps
import json
import gc
from flatdict import FlatDict
import pandas as pd

from main import generate_sample_data, benchmark


# --- Benchmarking Setup ---
results_list = []

# --- FlatDict Functions loop comparison---
# using a for loop
@benchmark
def flatdict_flatten(match_details):
    """Flattens player stats using the flatdict library."""

    player_stats = []
    player_list = match_details.get('home', {}).get('teamsheet', [])
    for player in player_list:
        stats = player.pop('match_stats', {})
        flat_stats = FlatDict(stats, delimiter='_')
        player.update(flat_stats)
        player_stats.append(player)

    return player_stats

# using a list comprehension instead of for loop
@benchmark
def flatdict_flatten_list_comprehension(match_details):
    """Flattens player stats using the flatdict library."""

    player_list = match_details.get('home', {}).get('teamsheet', [])
    
    # Create a new list where each player has their stats flattened
    player_stats = [dict(player, **FlatDict(player.pop('match_stats', {}), delimiter='_')) 
                    for player in player_list]

    return player_stats

@benchmark
def flatdict_flatten_gen_comprehension(match_details):
    """Flattens player stats using the flatdict library."""

    player_list = match_details.get('home', {}).get('teamsheet', [])
    
    # Create a new list where each player has their stats flattened
    player_stats = (dict(player, **FlatDict(player.pop('match_stats', {}), delimiter='_')) 
                    for player in player_list)

    return list(player_stats)

    
# --- Main Comparison Function ---
def run_and_compare(data):
    """
    Runs all benchmarked functions and returns the results in a DataFrame.
    The global results_list is cleared for each run.
    """
    global results_list
    results_list = [] # Reset results for a clean run for this specific data size

    # Execute each function to populate the results list

    flatdict_flatten(data)
    gc.collect()
    flatdict_flatten_list_comprehension(data) 
    gc.collect()
    flatdict_flatten_gen_comprehension(data) 

    # Create a DataFrame from the results for this run
    df_results = pd.DataFrame(results_list)

    # Return the dataframe so it can be collected
    return df_results


# --- Main Execution Block ---
if __name__ == "__main__":
    player_counts = [23, 100, 1000, 10000, 100000]

    all_results_dfs = []

    print("="*60)
    print("  Starting Benchmark Suite for Multiple Data Sizes")
    print("="*60)

    # Loop over each defined player count
    for num in player_counts:
        print(f"\n--- Running benchmarks for {num} players ---")
        match_data = generate_sample_data(num_players=num)
        results_df_for_num = run_and_compare(match_data)
        # Add a column to identify the data size for this batch of results
        results_df_for_num['num_players'] = num

        all_results_dfs.append(results_df_for_num)
        print(results_df_for_num.sort_values(by='memory_in_mb').to_string())

    print("\n" + "="*50)
    print("AGGREGATED BENCHMARK RESULTS - by Memory usage")
    print("="*50 + "\n")

    final_comparison_df = pd.concat(all_results_dfs, ignore_index=True)
    # Reorder columns for better readability
    final_comparison_df = final_comparison_df[['num_players', 'function', 'time_in_s', 'memory_in_mb']]

    print(final_comparison_df.sort_values(by=['num_players', 'memory_in_mb']).to_string())

    final_comparison_df.to_json("data/compare_loops_results.json", orient='records')
