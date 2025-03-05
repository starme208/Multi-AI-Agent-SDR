# Multi-Agent AI SDR for Lead Processing
This application uses LangChain, Anthropic's Claude, and Confluent to create an AI-based SDR.

The multi-agent system automates the SDR workflow by orchestrating a series of AI agents, each responsible for a specific task in the lead management and outreach process.

The system is event-driven, leveraging [Confluent Cloud's](https://www.confluent.io/) as the backbone for real-time communication and data flow between agents. 

At a high level, the initial system consists of the following key agents:

* Lead Ingestion Agent: Captures incoming leads from web forms, enriches them with external data (e.g., company website, Salesforce), and generates a research report that can be used for scoring
* Lead Scoring Agent: Uses the enriched lead information to score the lead and generate a short summary for how to best engage the lead. Determines the appropriate next step based on lead quality, either triggering the nurture agent sequence designer or triggering an active outreach campaign.
* Active Outreach Agent: Creates personalized outreach emails using AI-driven content generation, incorporating insights from the lead’s online presence, trying to book a meeting
* Nurture Campaign Agent: Dynamically creates a sequence of emails based on where the lead originated and what their interest was.

Each agent is designed to run as a microservice with a brain that communicates via event streams in Confluent Cloud, allowing for real-time processing and asynchronous handoffs between agents.

The diagram below illustrates how these agents interact through event-driven messaging.

<p align="center">
  <img src="/images/ai-sdr-architecture-diagram.png" />
</p>

## How it works
As a user, you can enter a lead into a web form. Once you submit the lead form, it's saved to MongoDB. A source connector
in Confluent takes data from MongoDB and adds it into a Kafka topic. An HTTP sink connector takes new messages in the
topic and calls the lead ingestion agent. 

This starts the multi-agent process.

# Project overview

The project is split into two applications. The `web-application` is a NextJS application that uses a standard three tier stack consisting of a frontend written in React, a backend in Node, and a MongoDB application database.

For the purposes of this demo, I'm using MongoDB to store the leads, but in a real world scenario, these would likely go
into a marketing automation platform or CRM.

Kafka and Flink, running on Confluent Cloud, are used to move data around between services. The web application doesn't know anything about LLMs, Kafka, or Flink.

The `agents` application is a Python app that includes routes to the different agents and API endpoints called by Confluent to consume messages from Kafka topics. These API endpoints take care of all the AI magic to generate a lead engagement plan.

# What you'll need
In order to set up and run the application, you need the following:

* [Node v22.5.1](https://nodejs.org/en) or above
* [Python 3.10](https://www.python.org/downloads/) or above
* A [Confluent Cloud](https://www.confluent.io/) account
* A [Claude](https://www.anthropic.com/claude) API key
* A [LangChain](https://www.langchain.com/) API key
* A [MongoDB](https://www.mongodb.com/) account

## Getting set up

### Get the starter code
In a terminal, clone the sample code to your project's working directory with the following command:

```shell
git clone https://github.com/thefalc/multi-agent-ai-sdr.git
```

### Setting up MongoDB

In MongoDB create a database called `stratusdb` with the following collections:

* `leads` - Stores leads generated from the web application

### Configure and run the lead capture web application

Go into your `web-application` folder and create a `.env` file with your MongoDB connection details.

```bash
MONGODB_URI='mongodb+srv://USER:PASSWORD@CLUSTER_URI/?retryWrites=true&w=majority&appName=REPLACE_ME'
```

Navigate into the `web-application` folder and run the application.

```bash
npm install
npm run dev
```

Go to `http://localhost:3000` and try creating a lead. If everything looks good, then continue with the setup.

### Setting up Confluent Cloud

The AI SDR uses Confluent Cloud to move and operate on data in real-time and handle the heavy lifting for communication between the agents.

### Create the MongoDB request source connector

In order to kick start the agentic workflow, data from MongoDB needs to be published to Kafka. This can be done by creating a MongoDB source connector.

In Confluent Cloud, create a new connector.

<p align="center">
  <img src="/images/confluent-cloud-overview.png" />
</p>

* Search for "mongodb" and select the **MongoDB Atlas Source**
* Enter a topic prefix as `incoming-leads`
* In **Kafka credentials**, select **Service account** and use an existing or create a new one
* In **Authentication,** enter your MongoDB connection details, the database name **stratusdb** and a collection name of **leads**
* Under **Configuration**, select **JSON**
* For **Sizing**, leave the defaults and click **Continue**
* Name the connector `inbound-leads-source-connector` and click **Continue**

### Create the HTTP sink connector to start the lead ingestion agent

Now that data is flowing from the application database into the `incoming-leadds.stratusdb.leads` topic (note, this topic will only be created once you've saved a lead to MongoDB), you now need to setup an HTTP sink connectors to start the lead ingestion agent.

* Under **Connectors**, click **+ Add Connector**
* Search for "http" and select the **HTTP Sink** connector
* Select the **incoming-leadds.stratusdb.leads** topic
* In **Kafka credentials**, select **Service account** and use you existing service account and click **Continue**
* Enter the URL for where the `lead-ingestion-agent` endpoint is running under the `agents` folder. This will be
similar to `https://YOUR-PUBLIC-DOMAIN/api/lead-ingestion-agent`. If running locally, you can use [ngrok](https://ngrok.com/)
to create a publicly accessible URL. Click **Continue**
* Under **Configuration**, select **JSON** and click **Continue**
* For **Sizing**, leave the defaults and click **Continue**
* Name the connector `lead-ingestion-agent-sink` and click **Continue**

Once the connector is created, under the **Settings** > **Advanced configuration** make sure the **Request Body Format** is set to **json**.

### Create the lead ingestion output topic

The `lead-ingestion-agent` endpoint publishes a report on the lead to a Kafka topic called `lead_ingestion_agent_output`.

In your Confluent Cloud account.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `lead_ingestion_agent_output`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **lead_ingestion_agent_output** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "properties": {
    "content": {
      "connect.index": 1,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "lead_data": {
      "connect.index": 0,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "properties": {
            "company_name": {
              "type": "string"
            },
            "company_website": {
              "format": "uri",
              "type": "string"
            },
            "email": {
              "format": "email",
              "type": "string"
            },
            "job_title": {
              "type": "string"
            },
            "lead_source": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "project_description": {
              "type": "string"
            }
          },
          "required": [
            "name",
            "email",
            "company_name",
            "lead_source",
            "job_title"
          ],
          "type": "object"
        }
      ]
    }
  },
  "title": "Record",
  "type": "object"
}
```

* Save the schema

### Create the HTTP sink connector to start the lead scoring agent

The lead scoring agent takes the output from the lead ingestion agent and calculates a score for the lead
and a recommendation for whether to nurture the lead or actively engage it. 

To do this, we need to create an HTTP sink connector that is triggered when new messages are written to the 
**lead_ingestion_agent_output** topic.

* Under **Connectors**, click **+ Add Connector**
* Search for "http" and select the **HTTP Sink** connector
* Select the **lead_ingestion_agent_output** topic
* In **Kafka credentials**, select **Service account** and use you existing service account and click **Continue**
* Enter the URL for where the `lead-scoring-agent` endpoint is running under the `agents` folder. This will be
similar to `https://YOUR-PUBLIC-DOMAIN/api/lead-scoring-agent`. If running locally, you can use [ngrok](https://ngrok.com/)
to create a publicly accessible URL. Click **Continue**
* Under **Configuration**, select **JSON** and click **Continue**
* For **Sizing**, leave the defaults and click **Continue**
* Name the connector `lead-scoring-agent-sink` and click **Continue**

Once the connector is created, under the **Settings** > **Advanced configuration** make sure the **Request Body Format** is set to **json**.

### Create the lead scoring agent topic

The `lead-scoring-agent` endpoint publishes lead score information to a Kafka topic called `lead_scoring_agent_output`.

In your Confluent Cloud account.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `lead_scoring_agent_output`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **lead_scoring_agent_output** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "properties": {
    "lead_data": {
      "connect.index": 0,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "properties": {
            "company_name": {
              "type": "string"
            },
            "company_website": {
              "format": "uri",
              "type": "string"
            },
            "email": {
              "format": "email",
              "type": "string"
            },
            "job_title": {
              "type": "string"
            },
            "lead_source": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "project_description": {
              "type": "string"
            }
          },
          "required": [
            "name",
            "email",
            "company_name",
            "lead_source",
            "job_title"
          ],
          "type": "object"
        }
      ]
    },
    "lead_evaluation": {
      "connect.index": 1,
      "properties": {
        "next_step": {
          "description": "Recommended next action based on lead score.",
          "enum": [
            "Actively Engage",
            "Nurture",
            "Disqualify"
          ],
          "type": "string"
        },
        "score": {
          "description": "Evaluation score of the lead (0-100).",
          "type": "string"
        },
        "talking_points": {
          "description": "Key discussion points for engaging the lead.",
          "items": {
            "type": "string"
          },
          "type": "array"
        }
      },
      "required": [
        "score",
        "next_step",
        "talking_points"
      ],
      "type": "object"
    }
  },
  "title": "Record",
  "type": "object"
}
```

* Save the schema

### Flink SQL setup to split the leads

Flink SQL is used to select the subset of leads that should be nurtured and add them into a `leads_to_nurture` topic and
select the leads that should be acively engaged and add them into a `leads_to_activate` topic.

First, let's create the topics.

In your Confluent Cloud account.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `leads_to_nurture`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **leads_to_nurture** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "properties": {
    "lead_data": {
      "connect.index": 0,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "properties": {
            "company_name": {
              "type": "string"
            },
            "company_website": {
              "format": "uri",
              "type": "string"
            },
            "email": {
              "format": "email",
              "type": "string"
            },
            "job_title": {
              "type": "string"
            },
            "lead_source": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "project_description": {
              "type": "string"
            }
          },
          "required": [
            "name",
            "email",
            "company_name",
            "lead_source",
            "job_title"
          ],
          "type": "object"
        }
      ]
    },
    "lead_evaluation": {
      "connect.index": 1,
      "properties": {
        "next_step": {
          "description": "Recommended next action based on lead score.",
          "enum": [
            "Actively Engage",
            "Nurture",
            "Disqualify"
          ],
          "type": "string"
        },
        "score": {
          "description": "Evaluation score of the lead (0-100).",
          "type": "string"
        },
        "talking_points": {
          "description": "Key discussion points for engaging the lead.",
          "items": {
            "type": "string"
          },
          "type": "array"
        }
      },
      "required": [
        "score",
        "next_step",
        "talking_points"
      ],
      "type": "object"
    }
  },
  "title": "Record",
  "type": "object"
}
```

Repeat the same process to create the topic `leads_to_activate`.

Next, let's write the Flink jobs to split up the leads.

* In your Kafka cluster, go to the **Stream processing** tab
* Click **Create workspace**
* Enter the following SQL to populate the `leads_to_activate` topic.

```sql
INSERT INTO `leads_to_activate`
SELECT
    *
FROM `lead_scoring_agent_output`
WHERE lead_evaluation.next_step = 'Actively Engage';
```
* Click **Run**

* Next, enter the following SQL to populate the `leads_to_nurture` topic.

```sql
INSERT INTO `leads_to_activate`
SELECT
    *
FROM `lead_scoring_agent_output`
WHERE lead_evaluation.next_step = 'Nurture';
```
* Click **Run**

The next step to configuring Confluent Cloud is to create two more HTTP sink connectors that will call the `active-outreach-agent` and `nurture-campaign-agent` endpoints.

#### Created the active outreach agent HTTP sink

In your Confluent Cloud account.

* Under **Connectors**, click **+ Add Connector**
* Search for "http" and select the **HTTP Sink** connector
* Select the **leads_to_activate** topic
* In **Kafka credentials**, select **Service account** and use you existing service account and click **Continue**
* Enter the URL for where the `active-outreach-agent` endpoint is running under the `agents` folder. This will be
similar to `https://YOUR-PUBLIC-DOMAIN/api/active-outreach-agent`. If running locally, you can use [ngrok](https://ngrok.com/)
to create a publicly accessible URL. Click **Continue**
* Under **Configuration**, select **JSON_SR** and click **Continue**
* For **Sizing**, leave the defaults and click **Continue**
* Name the connector `active-outreach-agent-sink` and click **Continue**

Once the connector is created, under the **Settings** > **Advanced configuration** make sure the **Request Body Format** is set to **json**.

#### Created the nurture campaign agent HTTP sink

In your Confluent Cloud account.

* Under **Connectors**, click **+ Add Connector**
* Search for "http" and select the **HTTP Sink** connector
* Select the **leads_to_nurture** topic
* In **Kafka credentials**, select **Service account** and use you existing service account and click **Continue**
* Enter the URL for where the `nurture-campaign-agent` endpoint is running under the `agents` folder. This will be
similar to `https://YOUR-PUBLIC-DOMAIN/api/nurture-campaign-agent`. If running locally, you can use [ngrok](https://ngrok.com/)
to create a publicly accessible URL. Click **Continue**
* Under **Configuration**, select **JSON_SR** and click **Continue**
* For **Sizing**, leave the defaults and click **Continue**
* Name the connector `nuture-campaign-agent-sink` and click **Continue**

Once the connector is created, under the **Settings** > **Advanced configuration** make sure the **Request Body Format** is set to **json**.

### Create the email campaigns topic

Both the `activate-outreach-agent` and `nurture-campaign-agent` publish their campaigns to a Kafka topic called `email_campaigns`.

In your Confluent Cloud account.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `email_campaigns`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **email_campaigns** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "campaign_type": {
      "description": "Defines the type of campaign: 'Nurture' for long-term relationship-building or 'Actively Engage' for immediate sales conversion.",
      "enum": [
        "Nurture",
        "Actively Engage"
      ],
      "type": "string"
    },
    "emails": {
      "items": {
        "properties": {
          "body": {
            "description": "Email content",
            "type": "string"
          },
          "subject": {
            "description": "Email subject line",
            "type": "string"
          },
          "to": {
            "description": "Recipient email address",
            "format": "email",
            "type": "string"
          }
        },
        "required": [
          "to",
          "subject",
          "body"
        ],
        "type": "object"
      },
      "type": "array"
    }
  },
  "title": "EmailCampaign",
  "type": "object"
}
```

* Save the schema

### Run the application

1. In a terminal, navigate to your project directory. Run the app with the following command:

```shell
python -m venv env
source env/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```
2. From your browser, navigate to http://localhost:3000 and you should see a lead capture form.
3. Enter some fake lead information.
4. Click **Submit**.
4. Wait for the agent flow to complete. If everything goes well, after a few minutes you'll have an email campaign in the `email_campaigns` topic.
