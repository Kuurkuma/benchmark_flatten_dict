# 🥥 Benchmark: Flattening Nested JSON Data 

This repository contains a benchmark comparing different methods for ingesting and flattening nested JSON data. 
For testiong purposes, we use sports statistics data — specifically, rugby data.

## Context

I have worked on a project to build a clean, queryable database of rugby statistics. The goal is to power a new kind of sports visualization experience — built by and for fans.

To achieve this, I was ingesting data from a RapidAPI that returns deeply nested JSON responses. This includes complex structures such as:

- Lists of dictionaries
- Nested dictionaries
- Inconsistent schemas depending on the data type (players, matches, events, etc.)

The ingestion pipeline is designed to run on **low-cost workers**, where **memory usage** and **execution efficiency** are critical.


## Problem Statement

> How can we ingest and flatten deeply nested JSON from an API efficiently — while keeping memory usage low and code maintainable?

There are many ways to handle JSON ingestion in Python. This benchmark explores the trade-offs between:

- **Performance (speed & memory)**
- **Ease of implementation**
- **Maintainability & flexibility**

---

## 🧪 Benchmark Process

The benchmark compares **two main categories** of ingestion methods:

### 1. Native Python Approaches

#### a. Manual `.get()` Extraction
A straightforward method that manually walks through JSON keys using `.get()`, building records field by field.

#### b. Recursive Generator Function
A more advanced approach using a recursive generator to yield all values from a nested JSON object.

### 2. Library-Oriented Approaches

#### a. [`pandas.json_normalize`](https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html)
Used to flatten records into a table-like format.

#### b. [`flatdict`](https://pypi.org/project/flatdict/)
Recursively flattens dictionaries into key paths, e.g., `"match.kicks"`.

#### c. [`dlt`](https://dlthub.com/docs/intro)
A modern ingestion framework that automates schema normalization and handles loading into destinations like DuckDB or cloud warehouses.

## 🧾 Metrics Evaluated

Each method was evaluated based on:

- ✅ **Execution time**
- 🧠 **Memory usage** 
- 🛠️ **Code complexity**
- 🔧 **Ease of adapting to schema changes**
- 🔄 **Maintainability**

---

## 🚀 How to Run

1. Clone this repo  
   ```bash
   git clone https://github.com/your-username/benchmark_flatten_dict.git
   cd benchmark_flatten_dict
   ```

2. Install dependencies  

This project uses the `uv` blazing fast package manager that everyone should use ;)
```
uv venv                      # Create a virtual environment (like `python -m venv .venv`)
source .venv/bin/activate    # or `.venv\Scripts\activate` on Windows
uv sync                      # Install dependencies from the lock file
```

