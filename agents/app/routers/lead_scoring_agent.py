"""
Lead Scoring Agent

This FastAPI-based microservice automates lead scoring by analyzing lead data and lead research report.
It evaluates lead quality, assigns a score, determines the next steps, and identifies relevant talking points.

Key Features:
- Parses incoming lead data from form submissions, CRM records, and AI-generated research reports.
- Uses Claude 3.5 Haiku via LangChain to score leads and determine engagement strategy.
- Implements a structured scoring system based on industry relevance, company size, and readiness to buy.
- Publishes structured lead evaluation data to a Kafka topic for downstream processing.
- Exposes an API endpoint (`/lead-scoring-agent`) to handle lead evaluation requests.

API Endpoint:
- `POST /lead-scoring-agent`: Accepts lead data, processes it asynchronously, and publishes the scored lead.
"""
from fastapi import APIRouter, Response, Request
from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv
import logging
import json
import re
import asyncio
from ..utils.publish_to_topic import produce
from ..utils.constants import LEAD_SCORING_AGENT_OUTPUT_TOPIC

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
model = ChatAnthropic(model='claude-3-5-haiku-20241022', temperature=0.7)

# Define tools to be used by the agent
tools = []

SYSTEM_PROMPT = """
    You're the Lead Scoring and Strategic Planner at StratusDB, a cloud-native, AI-powered data warehouse built for B2B
    enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data
    pipelines, enabling companies to store, query, and operationalize their data in real time.
    
    You combine insights from lead analysis and research to score leads accurately and align them with the
    optimal offering. Your strategic vision and scoring expertise ensure that
    potential leads are matched with solutions that meet their specific needs.

    You role is to utilize analyzed data and research findings to score leads, suggest next steps, and identify talking points.
    """

graph = create_react_agent(model, tools=tools, state_modifier=SYSTEM_PROMPT)

async def start_agent_flow(lead_details, content):
    example_output = {
             "score": "80",
             "next_step": "Nurture | Actively Engage",
             "talking_points": "Here are the talking points to engage the lead"
         }
    
    inputs = {"messages": [("user", f"""
      Utilize the provided context and the lead's form response to score the lead.

      - Consider factors such as industry relevance, company size, StratusAI Warehouse use case potential, and buying readiness.
      - Evaluate the wording and length of the response—short answers are a yellow flag.
      - Be pessimistic: focus high scores on leads with clear potential to close.
      - Smaller companies typically have lower budgets.
      - Avoid spending too much time on leads that are not a good fit.
      
      Lead Data
      - Lead Form Responses: {lead_details}
      - Additional Context: {content}
      
      Output Format
      - The output must be strictly formatted as JSON, with no additional text, commentary, or explanation.
      - The JSON should exactly match the following structure:
         {json.dumps(example_output)}

      Formatting Rules
        1. score: An integer between 0 and 100.
        2. next_step: Either "Nurture" or "Actively Engage" (no variations).
        3. talking_points: A list of at least three specific talking points, personalized for the lead.
        4. No extra text, no explanations, no additional formatting—output must be pure JSON.
        
        Failure to strictly follow this format will result in incorrect output.
      """)]}
    
    response = await graph.ainvoke(inputs)

    last_message_content = response["messages"][-1]
    content = last_message_content.pretty_repr()

    json_match = re.search(r"\{.*\}", content, re.DOTALL)

    if json_match:
        json_str = json_match.group()  # Extract JSON part
        lead_evaluation = json.loads(json_str)  # Convert to Python dictionary
        
        produce(LEAD_SCORING_AGENT_OUTPUT_TOPIC, { "lead_evaluation": lead_evaluation, "lead_data": lead_details })
    else:
        logger.info("No JSON found in the string.")

@router.api_route("/lead-scoring-agent", methods=["GET", "POST"])
async def lead_scoring_agent(request: Request):
    logger.info("lead-scoring-agent")
    if request.method == "POST":
        data = await request.json()

        logger.info(data)

        for item in data:
            logger.info(item)

            lead_details = item.get('lead_data', {}).get('lead', {})
            content = item.get('content', "")

            logger.info(lead_details)
            logger.info(content)

            asyncio.create_task(start_agent_flow(lead_details, content))

        return Response(content="Lead Scoring Agent Started", media_type="text/plain", status_code=200)
    else: # For local testing
        item = {'lead_data': {'lead': {'project_description': '111 Looking for a scalable data warehouse solution to support real-time analytics and AI-driven insights. Currently using Snowflake but exploring alternatives that better integrate with streaming data.', 'company_name': 'Tiger Analytics', 'company_website': 'https://www.tigeranalytics.com/', 'lead_source': 'Webinar - AI for Real-Time Data', 'name': 'Jane Doe', 'job_title': 'Director of Data Engineering', 'email': 'jane.doe@acmeanalytics.com'}}, 'content': '================================== Ai Message ==================================\n\nComprehensive Research Report for Tiger Analytics Lead\n\n1. Industry Overview:\n- Industry: Data & Analytics, Enterprise Software\n- Market Trends:\n  - Increasing demand for real-time analytics and AI-driven insights\n  - Growing emphasis on multi-cloud and flexible data infrastructure\n  - Rising importance of AI/ML integration in data platforms\n\n2. Company Insights:\n- Company Size: 350 employees\n- Annual Revenue: $25M-$50M\n- Funding: $45M (Series A in 2022)\n- Key Technologies: Snowflake, AWS, Apache Spark, Databricks, Tableau, BigQuery\n- Strategic Focus:\n  - AI and analytics consulting\n  - Multi-industry solutions (CPG, Retail, BFS, Insurance, etc.)\n  - Strong partnerships with cloud and data platform providers\n\n3. Potential Use Cases for StratusAI Warehouse:\n- Real-time Data Ingestion: Support for streaming data crucial for their analytics services\n- Multi-Cloud Deployment: Aligns with their existing multi-cloud technology stack\n- AI/ML Integration: Native support for ML model hosting matches their AI engineering capabilities\n- Data Sharing: Potential for monetizing client data through secure data exchange\n- Compliance: Built-in governance features for various industry regulations\n\n4. Lead Quality Assessment:\n- Engagement Signals: \n  - Attended AI-focused webinar\n  - Actively exploring data warehouse alternatives\n  - Current pain point: scalability of existing Snowflake setup\n- Fit Score: High\n  - Director-level technical decision-maker\n  - Company focused on data and AI solutions\n  - Demonstrated interest in advanced data infrastructure\n\n5. Additional Insights:\n- Current Tech Stack Compatibility: StratusAI Warehouse can seamlessly integrate with existing tools\n- Growth Potential: Company experiencing 20% YoY growth, indicating investment in technology\n- Key Decision-Makers to Engage:\n  - Michael Rodriguez (CEO)\n  - Sarah Chen (CTO)\n  - Jane Doe (Primary Contact)\n\n6. Recommended Outreach Strategy:\n- Personalized demo highlighting real-time analytics and AI integration\n- Case studies showcasing multi-cloud flexibility\n- Technical deep dive on query optimization and cost management\n- Emphasize seamless migration from Snowflake\n\n7. Potential Challenges:\n- Existing strong relationships with cloud providers\n- Potential resistance to changing current data infrastructure\n- Need to demonstrate clear ROI and performance improvements\n\nRecommendation: Priority Lead - Pursue with tailored, technical engagement strategy focusing on AI capabilities and multi-cloud flexibility.'}

        lead_details = item.get('lead_data', {}).get('lead', {})
        content = item.get('content', "")

        asyncio.create_task(start_agent_flow(lead_details, content))

        return Response(content="Lead Scoring Agent Started", media_type="text/plain", status_code=200)

