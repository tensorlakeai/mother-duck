# SEC Filings AI Risk Analysis Example

This example demonstrates how to use Tensorlake Applications to extract and analyze AI-related risk mentions from SEC filings.

## Overview

This example contains two Tensorlake applications:

1. **`process-sec.py`** - Extracts AI risk data from SEC filings using Tensorlake DocAI and stores it in MotherDuck
2. **`query-sec.py`** - Queries the extracted data with 6 pre-defined analysis options

The example processes 10 SEC filings across 4 companies to identify and categorize AI-related risks.

You can try this example out using [this Colab Notebook](https://tlake.link/notebooks/motherduck-applications) as well, just make sure to add the two files (`process-sec.py` and `query-sec.py`) to your Notebook environment before starting.

## Available Queries

- `0` - Risk category distribution
- `1` - Operational AI risks
- `2` - Emerging risks in 2025
- `3` - Risk timeline analysis
- `4` - Company risk profiles
- `5` - Company summary statistics

## Getting Started

### Prerequisites

- Tensorlake API key
- MotherDuck token
- Python 3.11+

### MotherDuck Setup

This example requires a MotherDuck account with:

1. A MotherDuck token for authentication
2. The application will automatically create a table called `ai_risk_filings` in your MotherDuck database

If you don't have a MotherDuck account, you can:
- Sign up at [MotherDuck](https://motherduck.com/)
- Generate a token in your account settings

### Local Testing

#### 1. Install Dependencies

```bash
pip install --upgrade tensorlake duckdb pandas pyarrow
```

#### 2. Set Environment Variables

```bash
export TENSORLAKE_API_KEY=YOUR_TENSORLAKE_API_KEY
export MOTHERDUCK_TOKEN=YOUR_MOTHERDUCK_TOKEN
```

#### 3. Process a Test Filing

Run the processing script to extract data from a single test SEC filing:

```bash
python process-sec.py
```

#### 4. Query the Data

Query the extracted data (replace `5` with any query number 0-5):

```bash
python query-sec.py 5
```

### Deploying to Tensorlake Cloud

#### 1. Verify Tensorlake Connection

```bash
tensorlake whoami
```

#### 2. Set Secrets

```bash
tensorlake secrets set MOTHERDUCK_TOKEN='YOUR_MOTHERDUCK_TOKEN'
```

#### 3. Verify Secrets

```bash
tensorlake secrets list
```

#### 4. Deploy Applications

Deploy the processing application:

```bash
tensorlake deploy process-sec.py
```

Deploy the query application:

```bash
tensorlake deploy query-sec.py
```

Once your applications have been deployed, you should be able to see them in your Applications on [cloud.tensorlake.ai](https://cloud.tensorlake.ai).

![A screenshot of the Tensorlake dashboard showing the two deployed applications `document_ingestion` and `query_sec`](./deployed-applications.png)

#### 5. Run the Full Pipeline

Process all SEC filings using the deployed application:

```bash
python process-sec-remote.py
```

To run a specific query using the deployed application:

*Note: specify a command line argument 0-5*
```bash
python query-sec-filings.py 2
```

## Files

- `process-sec.py` - Document processing application
- `query-sec.py` - Data query application  
- `process-sec-remote.py` - Script to run the deployed process-sec application
- `query-sec-remote.py` - Script to run the deployed query-sec application
- `README.md` - This file
