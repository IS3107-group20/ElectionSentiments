# ElectionSentiments

## Airflow Setup

### Prerequisites

Before starting, ensure the following requirements are met:

- Docker is installed and running on your machine.
- Docker Compose is available for orchestrating the Airflow containers.
- You have basic familiarity with Docker, Docker Compose, and Apache Airflow concepts.

### Setup Instructions

Follow these steps to get your Airflow environment up and running:

#### Step 1: Initialize Environment Variables

Start by setting up the necessary environment variables in an `.env` file. This step ensures that the Docker containers run with the appropriate permissions.

Open a terminal in your project's root directory and execute:

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env

```

#### Step 2: Create secrets.json

Airflow needs access to certain secrets, like API keys or database credentials, to execute your workflows. Store these secrets in a secrets.json file.

Create a new file named secrets.json in the root of your project directory.

Populate secrets.json with your secrets, formatted as key-value pairs in JSON. For example:

```json
{
  "API_KEY": "your_api_key_here",
  "DATABASE_PASSWORD": "your_database_password_here"
}
```

#### Step 3: Launch Airflow

With the .env file configured and your secrets safely stored in secrets.json, you are now ready to launch the Airflow environment. Use Docker Compose to manage the setup and ensure all services are started correctly.

Navigate to your project's root directory in a terminal and execute:

```bash
docker-compose down && docker-compose up --build -d
```
