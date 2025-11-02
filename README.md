# Cornhole Player Statistics

A Fly.io application that fetches, stores, and displays cornhole player statistics from the ACL (American Cornhole League) API.

## Features

- 📊 Fetch player data from ACL standings API
- 🔍 Search and filter players by name, state, skill level, and conference
- 📈 Sort by rank, PPR, DPR, CPI, win percentage, and more
- 📅 Support for multiple seasons (bucket IDs)
- 📉 Player comparison charts across seasons
- 🎨 Modern, responsive UI

## Project Structure

```
fly-cornhole/
├── main.py              # FastAPI application
├── database.py          # Database models and setup
├── models.py            # Pydantic models for API responses
├── fetcher.py           # Functions to fetch data from ACL APIs
├── requirements.txt     # Python dependencies
├── Dockerfile          # Docker configuration
├── fly.toml            # Fly.io configuration
├── templates/          # HTML templates
│   └── index.html
└── static/             # Static assets
    ├── styles.css
    └── app.js
```

## Setup

### Prerequisites

- Python 3.11+
- [Fly.io CLI](https://fly.io/docs/getting-started/installing-flyctl/) installed
- Fly.io account

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
uvicorn main:app --reload
```

3. Visit `http://localhost:8000`

### Deploy to Fly.io

1. Login to Fly.io:
```bash
fly auth login
```

2. Create a new app (if not already created):
```bash
fly apps create fly-cornhole
```

3. Deploy:
```bash
fly deploy
```

4. Open your app:
```bash
fly open
```

## Usage

### Fetching Data

1. Click the "Fetch Latest Data" button to start fetching player data
2. The data fetch runs in the background and may take several minutes (there are ~10,000 players)
3. You can continue using the app while data is being fetched

### Filtering and Sorting

- **Search**: Type a player's name to search
- **State**: Filter by US state
- **Skill Level**: Filter by skill level (P, A, B, C, S, T)
- **Sort By**: Choose the field to sort by (Rank, PPR, DPR, CPI, etc.)
- **Order**: Choose ascending or descending order

### Player Comparison

Click "Compare Seasons" on any player row to see their stats across multiple seasons in a chart.

### Season Selection

Use the season dropdown in the header to switch between different seasons (bucket IDs). Default is season 11 (2025-2026).

## API Endpoints

- `GET /` - Main web interface
- `POST /api/fetch-data/{bucket_id}` - Trigger data fetch for a season
- `GET /api/players` - Get players with filtering, sorting, and pagination
- `GET /api/players/{player_id}` - Get a specific player's data
- `GET /api/players/{player_id}/comparison` - Get player stats across multiple seasons
- `GET /api/stats/filters` - Get available filter options

## Database

The application uses SQLite by default (for simplicity). For production, you may want to use PostgreSQL:

1. Create a Postgres database on Fly.io:
```bash
fly postgres create --name cornhole-db
```

2. Attach it to your app:
```bash
fly postgres attach --app fly-cornhole cornhole-db
```

3. Update `database.py` to use the `DATABASE_URL` environment variable.

## Notes

- The ACL API may have rate limits. The fetcher includes small delays between requests.
- Initial data fetch may take 10-20 minutes due to the number of players.
- Player stats may not be available for all players (some may return null/empty data).

## License

MIT

