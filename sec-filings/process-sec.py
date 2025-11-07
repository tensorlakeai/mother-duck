import os
import json
from typing import List, Optional, Tuple, Any

from pydantic import BaseModel, Field
import duckdb

from tensorlake.applications import Image, application, function, cls
from tensorlake.documentai import (
    DocumentAI, PageClassConfig, StructuredExtractionOptions, ParseResult
)

image = (
    Image(base_image="python:3.11-slim", name="motherduck-sec")
    .run("pip install duckdb==1.3.2 pandas pyarrow")
)

class AIRiskMention(BaseModel):
    """Individual AI-related risk mention"""
    risk_category: str = Field(
        description="Category: Operational, Regulatory, Competitive, Ethical, Security, Liability"
    )
    risk_description: str = Field(description="Description of the AI risk")
    severity_indicator: Optional[str] = Field(None, description="Severity level if mentioned")
    citation: str = Field(description="Page reference")

class AIRiskExtraction(BaseModel):
    """Complete AI risk data from a filing"""
    company_name: str
    ticker: str
    filing_type: str
    filing_date: str
    fiscal_year: str
    fiscal_quarter: Optional[str] = None
    ai_risk_mentioned: bool
    ai_risk_mentions: List[AIRiskMention] = []
    num_ai_risk_mentions: int = 0
    ai_strategy_mentioned: bool = False
    ai_investment_mentioned: bool = False
    ai_competition_mentioned: bool = False
    regulatory_ai_risk: bool = False

@application()
@function(
    secrets=[
        "TENSORLAKE_API_KEY"
    ], 
    image=image
)
def document_ingestion(document_urls: List[str]) -> None:
    """Main entry point for document processing pipeline"""
    doc_ai = DocumentAI(api_key=os.getenv("TENSORLAKE_API_KEY"))
    
    # Initialize MotherDuck table
    initialize_motherduck_table()
    
    page_classifications = [
        PageClassConfig(
            name="risk_factors",
            description="Pages that contain risk factors related to AI."
        ),
    ]
    parse_ids = {}

    for file_url in document_urls:
        try:
            parse_id = doc_ai.classify(
                file_url=file_url,
                page_classifications=page_classifications
            )
            parse_ids[file_url] = parse_id
            print(f"Successfully classified {file_url}: {parse_id}")
        except Exception as e:
            print(f"Failed to classify document {file_url}: {e}")
    results = extract_structured_data.map(parse_ids.items())
    print("Processing complete.")
    return results

@function(
    image=image, 
    secrets=[
        "TENSORLAKE_API_KEY"
    ]
)
def extract_structured_data(url_parse_id_pair: Tuple[str, str]) -> None:
    """Extract structured data from classified pages"""
    print(f"Processing: {url_parse_id_pair}")
    
    doc_ai = DocumentAI(api_key=os.getenv("TENSORLAKE_API_KEY"))
    result = doc_ai.wait_for_completion(parse_id=url_parse_id_pair[1])
    
    page_numbers = []
    for page_class in result.page_classes:
        if page_class.page_class == "risk_factors":
            page_numbers.extend(page_class.page_numbers)
    
    if not page_numbers:
        print(f"No risk factor pages found for {url_parse_id_pair[0]}")
        return None
    
    page_number_str_list = ",".join(str(i) for i in page_numbers)
    print(f"Extracting from pages: {page_number_str_list}")
    
    result = doc_ai.extract(
        file_url=url_parse_id_pair[0],
        page_range=page_number_str_list,
        structured_extraction_options=[
            StructuredExtractionOptions(
                schema_name="AIRiskExtraction", 
                json_schema=AIRiskExtraction
            )
        ]
    )

    # Write the structured data to MotherDuck
    return write_to_motherduck(result, url_parse_id_pair[0])

@function(
    image=image, 
    secrets=[
        "MOTHERDUCK_TOKEN"
    ]
)
def initialize_motherduck_table() -> None:
    """Initialize the MotherDuck table with the required schema"""
    import duckdb

    con = duckdb.connect('md:ai_risk_factors')
    
    create_ai_risk_factors_sql = """
    CREATE TABLE IF NOT EXISTS ai_risk_filings (
        company_name VARCHAR,
        ticker VARCHAR,
        filing_type VARCHAR,
        filing_date VARCHAR,
        fiscal_year VARCHAR,
        fiscal_quarter VARCHAR,
        ai_risk_mentioned BOOLEAN,
        ai_risk_mentions JSON,
        num_ai_risk_mentions INTEGER,
        ai_strategy_mentioned BOOLEAN,
        ai_investment_mentioned BOOLEAN,
        ai_competition_mentioned BOOLEAN,
        regulatory_ai_risk BOOLEAN
    )
    """
    con.execute(create_ai_risk_factors_sql)

    print("Creating Risk Mentions table")
    # Create Risk Mentions table
    create_ai_risk_mentions_sql = """
        CREATE TABLE IF NOT EXISTS ai_risks (
            COMPANY_NAME VARCHAR(100),
            TICKER VARCHAR(10),
            FISCAL_YEAR VARCHAR(4),
            FISCAL_QUARTER VARCHAR(10),
            SOURCE_FILE VARCHAR(500),
            RISK_CATEGORY VARCHAR(50),
            RISK_DESCRIPTION VARCHAR(16777216),
            SEVERITY_INDICATOR VARCHAR(20),
            CITATION VARCHAR(100)
        )
    """
    con.execute(create_ai_risk_mentions_sql)

@function(
    image=image, 
    secrets=[
        "TENSORLAKE_API_KEY",
        "MOTHERDUCK_TOKEN"
    ]
)
def write_to_motherduck(parse_id: str, file_url: str) -> None:
    """Write structured data to MotherDuck tables"""
    print("Writing data to MotherDuck")
    doc_ai = DocumentAI(api_key=os.getenv("TENSORLAKE_API_KEY"))
    result: ParseResult = doc_ai.wait_for_completion(parse_id)
    
    if not result.structured_data:
        print(f"No structured data found for {file_url}")
        return
    
    # Prepare data
    raw = result.structured_data[0].data
    record = raw if isinstance(raw, dict) else (raw[0] if isinstance(raw, list) and raw else {})
    
    data = dict(record)
    mentions = data.pop("ai_risk_mentions", []) or []
    
    # Add source file reference
    source_file = os.path.basename(file_url)
    data['source_file'] = source_file
    
    # Serialize mentions for JSON column
    data['ai_risk_mentions_json'] = json.dumps(mentions)

    con = duckdb.connect('md:ai_risk_factors')

    # Insert the single record into ai_risk_filings
    insert_sql = """
    INSERT INTO ai_risk_filings (
        company_name,
        ticker,
        filing_type,
        filing_date,
        fiscal_year,
        fiscal_quarter,
        ai_risk_mentioned,
        ai_risk_mentions,
        num_ai_risk_mentions,
        ai_strategy_mentioned,
        ai_investment_mentioned,
        ai_competition_mentioned,
        regulatory_ai_risk
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Execute the insert with positional parameters
    con.execute(insert_sql, (
        data.get('company_name'),
        data.get('ticker'),
        data.get('filing_type'),
        data.get('filing_date'),
        data.get('fiscal_year'),
        data.get('fiscal_quarter'),
        data.get('ai_risk_mentioned'),
        data.get('ai_risk_mentions'),
        data.get('num_ai_risk_mentions'),
        data.get('ai_strategy_mentioned'),
        data.get('ai_investment_mentioned'),
        data.get('ai_competition_mentioned'),
        data.get('regulatory_ai_risk')
    ))

    # Insert into ai_risk_mentions table
    if 'ai_risk_mentions' in data:
        for mention in ai_risk_mentions:  # Load JSON string back to list
            print("Inserting mention:", mention)
            insert_mentions_sql = """
            INSERT INTO ai_risks (
                COMPANY_NAME,
                TICKER,
                FISCAL_YEAR,
                FISCAL_QUARTER,
                SOURCE_FILE,
                RISK_CATEGORY,
                RISK_DESCRIPTION,
                SEVERITY_INDICATOR,
                CITATION
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            con.execute(insert_mentions_sql, (
                data.get('company_name'),
                data.get('ticker'),
                data.get('fiscal_year'),
                data.get('fiscal_quarter'),
                data.get('source_file', ''),  # Assuming source_file is part of data
                data['risk_category'],
                data['risk_description'],
                data['severity_indicator'],
                data['citation']
            ))
            print("Inserted mention:", mention)
        print(f"Inserted {len(ai_risk_mentions)} risk mentions for {data.get('company_name')}")

    con.close()  # Close the connection after operations

if __name__ == "__main__":
    from tensorlake.applications import run_local_application
    
    # Example usage with a single document
    test_urls = [
        "https://investors.confluent.io/static-files/95299e90-a988-42c5-b9b5-7da387691f6a"
    ]
    
    response = run_local_application(
        document_ingestion,
        test_urls
    )

    print(response.output())
