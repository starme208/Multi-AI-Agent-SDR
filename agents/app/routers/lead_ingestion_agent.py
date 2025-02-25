"""
Lead Ingestion Agent - Preliminary Lead Analysis

This agent automates the preliminary analysis of incoming leads by gathering 
and enriching data from various sources. It extracts relevant insights 
and prepare leads for further engagement.

Key Functionalities:
- Fetches and processes lead details submitted via web forms or other sources.
- Scrapes company website content to extract key business information.
- Retrieves CRM data from Salesforce to understand past interactions. This is currently mocked via an LLM call.
- Enriches lead data using external services like Clearbit. This is currently mocked via an LLM call.
- Conducts AI-driven research to assess lead quality, industry trends, and potential fit.
- Publishes the analyzed data to a messaging topic for downstream processing.

Tech Stack:
- FastAPI for API handling and request processing.
- LangChain + Claude 3.5 Haiku for AI-driven research and enrichment.
- BeautifulSoup for web scraping.
- Kafka (via `produce`) for publishing enriched leads.
- Async execution for efficient concurrent processing.

API Endpoint:
- `POST /lead-ingestion-agent`: Processes new lead data and triggers research workflows.

"""

from fastapi import APIRouter, Response, Request
from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import requests
import asyncio
import logging
from ..utils.publish_to_topic import produce
from ..utils.constants import LEAD_INGESTION_AGENT_OUTPUT_TOPIC

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
model = ChatAnthropic(model='claude-3-5-haiku-20241022', temperature=0.7)

def remove_empty_lines(text: str) -> str:
    """Removes empty lines from the given text."""
    return "\n".join(line for line in text.splitlines() if line.strip())

@tool
def get_company_website_information(company_website_url):
    """
    Fetches and extracts readable text content from a company's website.

    This function:
    - Sends an HTTP GET request to the specified company website URL.
    - Parses the HTML response while removing non-visible elements like 
      <style>, <script>, <head>, and <title> tags.
    - Extracts and cleans visible text content.
    - Removes empty lines for better readability.

    Args:
        company_website_url (str): The URL of the company's website.

    Returns:
        str: The cleaned visible text from the website if successful.
        None: If the request fails or the website is inaccessible.

    Raises:
        requests.RequestException: If an error occurs during the HTTP request.
    """
    logger.info(f"Fetching company website information for: {company_website_url}")

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
        }
        response = requests.get(company_website_url, headers=headers, timeout=10)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
            visible_text = soup.getText()

            response = remove_empty_lines(visible_text)

            return response
        else:
            logger.info(f"Failed to fetch the website. Status code: {response.status_code}")
        
            return None
    except requests.RequestException as e:
        logger.info(f"Error fetching website: {e}")
        return None

@tool
def get_salesforce_data(lead_details):
    """
    Generates synthetic Salesforce data for a given lead.

    This function:
    - Takes the provided lead details as input.
    - Constructs a prompt to generate realistic Salesforce data, including:
      - Contact information
      - Company details
      - Lead status and attributes
      - Historical interactions
    - Invokes an AI model to generate the synthetic Salesforce response.
    - Returns the generated data in JSON format.

    Args:
        lead_details (str): A string containing relevant lead information.

    Returns:
        dict: A JSON object representing the synthetic Salesforce data.

    Note:
        - This function does not query a real Salesforce instance; it generates 
          plausible Salesforce-like data using an AI model.
    """

    logger.info(f"Fetching Salesforce data for: {lead_details}")

    prompt = f"""
      Take the lead details and generate realistic Salesforce data to represent the contact,
      company, lead information, and any historical interactions we've had with the lead.

      Return only the fake Salesforce data as JSON. Do not wrap the message in any additional text.

      Lead details:
      {lead_details}
    """

    response = model.invoke([{ "role": "user", "content": prompt }])

    logger.info(response)

    return response

@tool
def get_enriched_lead_data(lead_details):
    """
    Generates synthetic enriched lead data, including both person and company details.

    This function:
    - Takes raw lead details as input.
    - Constructs a prompt to generate realistic enriched data using an AI model.
    - Simulates Clearbit-like enrichment, including:
      - Personal details (name, job title, email, social profiles, etc.).
      - Employment history.
      - Company details (industry, size, funding, technologies used, etc.).
      - Key decision-makers and hiring trends.
    - Returns the generated enrichment data in JSON format.

    Args:
        lead_details (str): A string containing relevant lead information.

    Returns:
        dict: A JSON object representing the enriched lead data.

    Note:
        - This function does not query a real enrichment service like Clearbit.
        - The output is AI-generated and structured based on a predefined example.
    """

    logger.info(f"Fetching Clearbit data for: {lead_details}")

    clear_bit_sample_payload = {
        "person": {
            "full_name": "Jane Doe",
            "job_title": "Director of Data Engineering",
            "company_name": "Acme Analytics",
            "company_domain": "acmeanalytics.com",
            "work_email": "jane.doe@acmeanalytics.com",
            "linkedin_url": "https://www.linkedin.com/in/janedoe",
            "twitter_handle": "@janedoe",
            "location": {
                "city": "San Francisco",
                "state": "California",
                "country": "United States"
            },
            "work_phone": "+1 415-555-1234",
            "employment_history": [
                {
                    "company": "DataCorp",
                    "job_title": "Senior Data Engineer",
                    "years": "2018-2022"
                },
                {
                    "company": "Tech Solutions",
                    "job_title": "Data Analyst",
                    "years": "2015-2018"
                }
            ]
        },
        "company": {
            "name": "Acme Analytics",
            "domain": "acmeanalytics.com",
            "industry": "Data & Analytics",
            "sector": "Software & IT Services",
            "employee_count": 500,
            "annual_revenue": "$50M-$100M",
            "company_type": "Private",
            "headquarters": {
                "city": "San Francisco",
                "state": "California",
                "country": "United States"
            },
            "linkedin_url": "https://www.linkedin.com/company/acme-analytics",
            "twitter_handle": "@acmeanalytics",
            "facebook_url": "https://www.facebook.com/acmeanalytics",
            "technologies_used": [
                "AWS",
                "Snowflake",
                "Apache Kafka",
                "Flink",
                "Looker",
                "Salesforce"
            ],
            "funding_info": {
                "total_funding": "$75M",
                "last_round": "Series B",
                "last_round_date": "2023-08-15",
                "investors": ["Sequoia Capital", "Andreessen Horowitz"]
            },
            "key_decision_makers": [
                {
                    "name": "John Smith",
                    "title": "CEO",
                    "linkedin_url": "https://www.linkedin.com/in/johnsmith"
                },
                {
                    "name": "Emily Johnson",
                    "title": "VP of Engineering",
                    "linkedin_url": "https://www.linkedin.com/in/emilyjohnson"
                }
            ],
            "hiring_trends": {
                "open_positions": 12,
                "growth_rate": "15% YoY",
                "top_hiring_departments": ["Engineering", "Data Science", "Sales"]
            }
        }
    }

    # Convert JSON to a properly escaped string
    clearbit_sample_as_string = json.dumps(clear_bit_sample_payload, indent=4) 

    prompt = f"""
      Take the lead details and generate realistic Clearbit data to represent the enriched lead.
      Return only the fake Clearbit data as JSON. Do not wrap the message in any additional text.

      Lead details:
      {lead_details}

      The fake output should look like this:
      {clearbit_sample_as_string}
    """

    response = model.invoke([{ "role": "user", "content": prompt }])

    logger.info(response)

    return response

# Define tools to be used by the agent
tools = [get_company_website_information, get_salesforce_data, get_enriched_lead_data]

SYSTEM_PROMPT = """
    You're an Industry Research Specialist at StratusDB, a cloud-native, AI-powered data warehouse built for B2B
    enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data
    pipelines, enabling companies to store, query, and operationalize their data in real time.

    Your role is to conduct research on potential leads to assess their fit for StratusAI Warehouse and provide key
    insights for scoring and outreach planning. Your research will focus on industry trends, company background,
    and AI adoption potential to ensure a tailored and strategic approach.
    """

graph = create_react_agent(model, tools=tools, state_modifier=SYSTEM_PROMPT)

def print_stream(stream):
    for s in stream:
        message = s["messages"][-1]
        if isinstance(message, tuple):
            print(message)
        else:
            message.pretty_print()

async def start_agent_flow(lead_details):
    inputs = {"messages": [("user", f"""
      Using the lead input data, conduct preliminary research on the lead. Focus on finding relevant data
      that can aid in scoring the lead and planning a strategy to pitch them.

      Key Responsibilities:
        - Analyze the lead's industry to identify relevant trends, market challenges, and AI adoption patterns.
        - Gather company-specific insights, including size, market position, recent news, and strategic initiatives.
        - Determine potential use cases for StratusAI Warehouse, focusing on how the company could benefit from real-time analytics, multi-cloud data management, and AI-driven optimization.
        - Assess lead quality based on data completeness and engagement signals. Leads with short or vague form responses should be flagged for review but not immediately discarded.
        - Use dedicated tools to enhance research and minimize manual work:
          - Company Website Lookup Tool - Fetches key details from the company's official website.
          - Salesforce Data Access - Retrieves CRM data about the lead's past interactions, status, and engagement history.
          - Clearbit Enrichment API - Provides firmographic and contact-level data, including company size, funding, tech stack, and key decision-makers.
        - Filter out weak leads, ensuring minimal time is spent on companies unlikely to be a fit for StratusDB's offering.

      Lead Form Responses:
        {lead_details}

      Product Overview - StratusAI Warehouse:
      StratusAI Warehouse is a next-generation AI-powered data warehouse designed for data-driven enterprises. Key capabilities include:

      Real-time analytics & AI readiness - Built-in support for streaming data ingestion, vector search, and ML model hosting.
      Seamless data sharing - Securely share and monetize data across organizations via our Data Exchange.
      Multi-cloud & hybrid flexibility - Deploy across AWS, Azure, and GCP with intelligent cost optimization.
      Built-in compliance & governance - Native support for GDPR, HIPAA, and SOC 2 without performance trade-offs.
      AI-driven query optimization - Our engine auto-tunes performance and cost based on query patterns.
      Expected Output - Research Report:
      The research report should be concise and actionable, containing:

      Industry Overview - Key trends, challenges, and AI adoption patterns in the lead's industry.
      Company Insights - Size, market position, strategic direction, and recent news.
      Potential Use Cases - How StratusAI Warehouse could provide value to the lead's company.
      Lead Quality Assessment - Based on available data, engagement signals, and fit for StratusDB's ideal customer profile.
      Additional Insights - Any relevant information that can aid in outreach planning or lead prioritization.""")]}
    
    response = await graph.ainvoke(inputs)

    last_message_content = response["messages"][-1]
    content = last_message_content.pretty_repr()

    logger.info(f"Response from agent: {content}")

    produce(LEAD_INGESTION_AGENT_OUTPUT_TOPIC, { "content": content, "lead_data": lead_details })

@router.api_route("/lead-ingestion-agent", methods=["GET", "POST"])
async def lead_ingestion_agent(request: Request):
    print("lead-ingestion-agent")
    if request.method == "POST":
        data = await request.json()

        print(data)

        for item in data:
            oid_raw = item.get('fullDocument', {}).get('_id', '{}')
            
            # TODO: need to pull out lead data and start agent

        return Response(content="Lead Ingestion Agent Started", media_type="text/plain", status_code=200)
    else: # For local testing
        data = {
          "lead": {
            "name": "Jane Doe",
            "email": "jane.doe@acmeanalytics.com",
            "company_name": "Tiger Analytics",
            "company_website": "https://www.tigeranalytics.com/",
            "lead_source": "Webinar - AI for Real-Time Data",
            "job_title": "Director of Data Engineering",
            "project_description": "Looking for a scalable data warehouse solution to support real-time analytics and AI-driven insights. Currently using Snowflake but exploring alternatives that better integrate with streaming data."
          }
        }

        asyncio.create_task(start_agent_flow(data))

        return Response(content="Lead Ingestion Agent Started", media_type="text/plain", status_code=200)

