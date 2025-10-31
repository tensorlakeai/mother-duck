import os
import json
from typing import List, Optional, Tuple, Any

from pydantic import BaseModel, Field
import duckdb

from tensorlake.applications import Image, application, function, cls, map
from tensorlake.documentai import (
    DocumentAI, PageClassConfig, StructuredExtractionOptions
)

image = (
    Image(base_image="python:3.11-slim", name="snowflake-sec")
    .run("pip install duckdb=1.3.2 pandas pyarrow")
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
    results = synchronize(map(extract_structured_data, parse_ids.items()))

    print(type(results))

    return results


@function(image=image)
def synchronize(futures: List[Any]) -> List[Any]:
    """Synchronize parallel processing results"""
    pass


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
    
    # Write each extracted record individually
    for record in result:
        write_to_motherduck(record)
    
    return result


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
    
    create_table_sql = """
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
    con.execute(create_table_sql)

@function(
    image=image, 
    secrets=[
        "MOTHERDUCK_TOKEN"
    ]
)
def write_to_motherduck(data: dict) -> None:
    """Write a single record to MotherDuck"""
    import duckdb
    import json
    
    # Convert ai_risk_mentions to JSON string if it exists
    if 'ai_risk_mentions' in data:
        data['ai_risk_mentions'] = json.dumps(data.get('ai_risk_mentions', []))
    
    con = duckdb.connect('md:ai_risk_factors')
    
    # Insert the single record
    insert_sql = """
    INSERT INTO ai_risk_filings 
    SELECT * FROM (
        SELECT 
            %(company_name)s as company_name,
            %(ticker)s as ticker,
            %(filing_type)s as filing_type,
            %(filing_date)s as filing_date,
            %(fiscal_year)s as fiscal_year,
            %(fiscal_quarter)s as fiscal_quarter,
            %(ai_risk_mentioned)s as ai_risk_mentioned,
            %(ai_risk_mentions)s as ai_risk_mentions,
            %(num_ai_risk_mentions)s as num_ai_risk_mentions,
            %(ai_strategy_mentioned)s as ai_strategy_mentioned,
            %(ai_investment_mentioned)s as ai_investment_mentioned,
            %(ai_competition_mentioned)s as ai_competition_mentioned,
            %(regulatory_ai_risk)s as regulatory_ai_risk
    )
    """
    con.execute(insert_sql, data)

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
