# Natural Language to SQL Query Generator

A powerful web application that converts natural language queries into SQL queries, allowing users to interact with databases using plain English. The application features a modern React frontend and a FastAPI backend with natural language processing capabilities.

## Features

- **Natural Language Processing**: Convert plain English questions into SQL queries
- **Interactive UI**: Modern, responsive interface for easy querying
- **Database Integration**: Connect to various SQL databases
- **Real-time Results**: Get instant query results
- **Query History**: View and manage your previous queries
- **Export Capabilities**: Export query results to multiple formats

## Tech Stack

### Backend
- **Python 3.8+**
- **FastAPI** - Modern, fast web framework
- **SQLAlchemy** - SQL toolkit and ORM
- **spaCy** - Natural Language Processing
- **Uvicorn** - ASGI server

### Frontend
- **React** - Frontend library
- **TypeScript** - Type-safe JavaScript
- **Material-UI** - UI components
- **Axios** - HTTP client
- **React Query** - Data fetching and state management

## Project Structure

```
nlp-dashboard-demo/
├── backend/                 # Backend application
│   ├── .env                # Environment variables
│   ├── requirements.txt    # Python dependencies
│   ├── simple_api.py      # FastAPI application
│   ├── simple_query.py    # Core query processing logic
│   ├── db.py             # Database connection and utilities
│   └── check_columns.py  # Database schema utilities
├── frontend-react/        # Frontend React application
│   ├── public/           # Static files
│   └── src/              # React source code
├── assets/               # Images and other static assets
└── README.md            # This file
```

## Prerequisites

- Python 3.8 or higher
- Node.js 14.x or higher
- npm or yarn
- PostgreSQL/MySQL/SQLite (or any SQL database)

## Installation

### Backend Setup

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

2. Install backend dependencies:
   ```bash
   cd backend
   pip install -r requirements.txt
   ```

3. Set up environment variables in `.env`:
   ```
   DATABASE_URL=your_database_connection_string
   SECRET_KEY=your_secret_key
   ```

### Frontend Setup

1. Navigate to the frontend directory:
   ```bash
   cd frontend-react
   ```

2. Install dependencies:
   ```bash
   npm install
   # or
   yarn install
   ```

## Running the Application

### Start the Backend

```bash
cd backend
uvicorn simple_api:app --reload
```
The API will be available at `http://localhost:8000`

### Start the Frontend

```bash
cd frontend-react
npm start
# or
yarn start
```
The application will be available at `http://localhost:3000`

## API Endpoints

- `POST /query` - Process natural language query
  - Request body: `{"query": "your natural language query"}`
  - Response: SQL query and results

- `GET /tables` - List all available tables
- `GET /schema` - Get database schema information

## Usage

1. Open the application in your browser
2. Enter your natural language query (e.g., "Show me all customers from New York")
3. View the generated SQL query and results
4. Optionally save or export the results

## Example Output
Here is a screenshot of the SQL Generator in action:

![SQL Generator Output](assets/output.png)
## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Acknowledgments

- Built with ❤️ using FastAPI and React
- Inspired by the need for more accessible database querying tools
- Special thanks to all contributors who have helped improve this project

