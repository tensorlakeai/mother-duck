import os
import json
from typing import List, Optional, Tuple, Any

from pydantic import BaseModel, Field
import duckdb

from tensorlake.applications import Image, application, function, cls, RequestError, map

image = (
    Image(base_image="python:3.11-slim", name="snowflake-sec")
    .run("pip install duckdb=1.3.2 pandas pyarrow")
)

@application()
@function(
    image=image
)
def query_sec(query_choice):
    # Risk category distribution - Default Query
  query = """
      SELECT 
          RISK_CATEGORY,
          COUNT(*) as TOTAL_MENTIONS,
          COUNT(DISTINCT COMPANY_NAME) as COMPANIES_MENTIONING
      FROM AI_RISK_MENTIONS
      WHERE RISK_CATEGORY IS NOT NULL
      GROUP BY RISK_CATEGORY
      ORDER BY TOTAL_MENTIONS DESC
  """
  match query_choice:
    case "operational-risks":
      query = """
            WITH ranked_risks AS (
                SELECT 
                    COMPANY_NAME,
                    TICKER,
                    RISK_DESCRIPTION,
                    CITATION,
                    LENGTH(RISK_DESCRIPTION) as DESCRIPTION_LENGTH,
                    ROW_NUMBER() OVER (PARTITION BY COMPANY_NAME ORDER BY LENGTH(RISK_DESCRIPTION) DESC) as RN
                FROM AI_RISK_MENTIONS
                WHERE RISK_CATEGORY = 'Operational'
            )
            SELECT 
                COMPANY_NAME,
                TICKER,
                RISK_DESCRIPTION,
                CITATION,
                DESCRIPTION_LENGTH
            FROM ranked_risks
            WHERE RN = 1
            ORDER BY COMPANY_NAME
        """
    case "risk-evolution":
      query = """
            SELECT 
                COMPANY_NAME,
                TICKER,
                FISCAL_YEAR,
                FISCAL_QUARTER,
                RISK_CATEGORY,
                RISK_DESCRIPTION,
                CITATION
            FROM AI_RISK_MENTIONS
            WHERE FISCAL_YEAR = '2025'
            ORDER BY COMPANY_NAME, FISCAL_QUARTER
        """
    case "risk-timeline":
      query = """
            SELECT 
                FISCAL_YEAR,
                FISCAL_QUARTER,
                COUNT(DISTINCT SOURCE_FILE) as NUM_FILINGS,
                SUM(NUM_AI_RISK_MENTIONS) as TOTAL_RISK_MENTIONS,
                AVG(NUM_AI_RISK_MENTIONS) as AVG_RISK_MENTIONS_PER_FILING,
                SUM(CASE WHEN REGULATORY_AI_RISK THEN 1 ELSE 0 END) as FILINGS_WITH_REGULATORY_RISK
            FROM AI_RISK_FILINGS
            GROUP BY FISCAL_YEAR, FISCAL_QUARTER
            ORDER BY FISCAL_YEAR, FISCAL_QUARTER
        """
    case "risk-profiles":
      query = """
            SELECT 
                COMPANY_NAME,
                TICKER,
                RISK_CATEGORY,
                COUNT(*) as FREQUENCY
            FROM AI_RISK_MENTIONS
            WHERE RISK_CATEGORY IS NOT NULL
            GROUP BY COMPANY_NAME, TICKER, RISK_CATEGORY
            ORDER BY COMPANY_NAME, FREQUENCY DESC
        """
    case "company-summary":
      query = """
          SELECT 
              COMPANY_NAME,
              TICKER,
              COUNT(*) as TOTAL_FILINGS,
              AVG(NUM_AI_RISK_MENTIONS) as AVG_RISK_MENTIONS,
              SUM(CASE WHEN REGULATORY_AI_RISK THEN 1 ELSE 0 END) as FILINGS_WITH_REGULATORY_RISK,
              SUM(CASE WHEN AI_COMPETITION_MENTIONED THEN 1 ELSE 0 END) as FILINGS_MENTIONING_COMPETITION,
              SUM(CASE WHEN AI_INVESTMENT_MENTIONED THEN 1 ELSE 0 END) as FILINGS_MENTIONING_INVESTMENT
          FROM AI_RISK_FILINGS
          GROUP BY COMPANY_NAME, TICKER
          ORDER BY AVG_RISK_MENTIONS DESC
      """
  
  return make_query(query)


@function(
    image=image, 
    secrets=[
        "MOTHERDUCK_TOKEN"
    ]
)
def make_query(query: str) -> str:
    import duckdb
    con = duckdb.connect('md:ai_risk_factors')

    # Query: Risk category distribution (SQL approach)
    risk_categories = con.execute(query).fetchdf()

    print(risk_categories)

    return risk_categories.to_json(orient='records')

if __name__ == "__main__":
    from tensorlake.applications import run_local_application
    import sys

    queries = ["risk-distribution", "operational-risks", "risk-evolution", "risk-timeline", "risk-profiles", "company-summary"]
    query = queries[0]

    if len(sys.argv) > 1:
      query = queries[int(sys.argv[1])]

    response = run_local_application(
        query_sec,
        query
    )
    pretty_json = json.loads(response.output())
    print(json.dumps(pretty_json,indent=4))