# MotherDuck + Tensorlake Integration Examples

<p align="center">
  <img src="https://img.shields.io/badge/MotherDuck-FF6B6B?style=for-the-badge&logo=duck&logoColor=white" />
  <img src="https://img.shields.io/badge/RAG-8B5CF6?style=for-the-badge&logo=openai&logoColor=white" />
</p>
<p align="center">
  <a href="https://docs.tensorlake.ai"><img src="https://img.shields.io/badge/docs-tensorlake.ai-blue?style=flat-square" /></a>
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" />
</p>

## Transform Unstructured Data into Queryable and AI Ready Data on MotherDuck

Tensorlake is a serverless platform for building data applications and agents in Python that can ingest and transform unstructured data before landing them in MotherDuck's DuckDB database. This is an alternative to perform ETL orchestration with SQL expressions and UDF functions. 

Tensorlake's applications automatically behave like durable queues so you wouldn't need to setup Kafka or other queues to manage ingestion. The clusters automatically scales up as data is ingested to process them.

## Table of Contents
- [Example Use Cases](#use-cases)
  - [Document Ingestion Pipeline](#blueprint-document-ingestion-pipeline)
  - [Document Indexing](#blueprint-document-indexing) (*coming soon*)
- [Quick Overview: Tensorlake Applications](#quick-overview-tensorlake-applications)
- [Why This Integration Matters](#why-this-integration-matters)
- [Resources](#resources)

## Use Cases

We present some blueprints for production ready patterns to integrate with MotherDuck and code that you can deploy under 2 minutes and experience the integration.

### Blueprint: Document Ingestion Pipeline

The Tensorlake Application receives Document URLs over HTTP, uses an OCR API to parse the document, calls an LLM for structured extraction, and then uses MotherDuck's DuckDB connector to write structured data into your MotherDuck Database. Once it's inside MotherDuck you can do all sorts of analytics on the data. 

The Application is written in Python, without any external orchestration engines, so you can build and test it like any other normal application. You can use any OCR API in the Application, or even run open source OCR models on GPUs by annotating the OCR function with a GPU enabled hardware resource. 

Tensorlake automatically queues requests and scales out the cluster, there is no extra configuration required for handling spiky ingestion.

Try out the [code here](https://github.com/tensorlakeai/mother-duck/tree/main/sec-filings).

### Blueprint: Structured Data Extraction

This solution combines a Tensorlake serverless extraction application with a Streamlit interactive query interface to create an intelligent Wikipedia knowledge base. The Tensorlake application (`extract-wikipedia`) accepts a page type (like "actors"), uses BeautifulSoup and Requests to crawl Wikipedia pages, then leverages LangChain with OpenAI to intelligently parse HTML, chunk content, and extract structured information (birth dates, career highlights, key events). Everything is stored in MotherDuck with both structured data tables and text embeddings.

## Quick Overview: Tensorlake Applications

Tensorlake Applications are Python programs that:
1. Run as serverless applications
2. Can be triggered by HTTP requests, message queues, or scheduled events
3. Can use any Python package or model
4. Can run on CPU or GPU
5. Automatically scale out based on load
6. Have built-in queuing and fault tolerance

## Why This Integration Matters

The integration between Tensorlake and MotherDuck provides several key benefits:

1. **Simplified ETL for Unstructured Data**: Convert documents, images, and other unstructured data into structured formats without complex orchestration.
2. **Serverless Architecture**: No infrastructure management required - just write Python code.
3. **Automatic Scaling**: Handle varying loads without manual intervention.
4. **GPU Support**: Run ML models efficiently when needed.
5. **DuckDB Integration**: Leverage MotherDuck's powerful analytics capabilities with properly structured data.

## Resources

- [Tensorlake Documentation](https://docs.tensorlake.ai)
- [MotherDuck Documentation](https://motherduck.com/docs)
- [Example Applications](https://github.com/tensorlakeai/mother-duck/tree/main/examples)
- [Community Support](https:/tlake.link/slack)
