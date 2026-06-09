# Real-Time Voting Data Engineering Project

## 📌 Project Summary

A real-time data engineering pipeline that simulates an election voting system. The system generates vote data, streams it through Apache Kafka, processes it using Apache Spark Streaming, stores aggregated results in PostgreSQL, and displays live election results on a Streamlit dashboard.

This project demonstrates a complete streaming analytics pipeline — from data generation to real-time visualization.


## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                         SYSTEM ARCHITECTURE                            │
│                                                                        │
│  ┌───────────────┐                                                     │
│  │  Vote         │   Generates simulated voter & candidate data        │
│  │  Simulator    │                                                     │
│  │  (main.py)    │                                                     │
│  └──────┬────────┘                                                     │
│         │                                                              │
│         ▼                                                              │
│  ┌──────────────┐      Kafka Topics        ┌───────────────────────┐  │
│  │    Kafka     │ ────────────────────────► │   Spark Streaming     │  │
│  │   Broker     │  • voters_topic           │   (spark-streaming.py)│  │
│  │              │  • candidates_topic       │                       │  │
│  └──────────────┘  • votes_topic            └──────────┬────────────┘  │
│                                                        │               │
│                                                        ▼               │
│                                               ┌─────────────────┐     │
│                                               │   PostgreSQL    │     │
│                                               │   Database      │     │
│                                               │                 │     │
│                                               │ • candidates    │     │
│                                               │ • votes         │     │
│                                               │ • vote_counts   │     │
│                                               └────────┬────────┘     │
│                                                        │               │
│                                                        ▼               │
│                                               ┌─────────────────┐     │
│                                               │    Streamlit    │     │
│                                               │    Dashboard    │     │
│                                               │ (Live Results)  │     │
│                                               └─────────────────┘     │
└────────────────────────────────────────────────────────────────────────┘
```


## 🛠️ Tech Stack

| Layer            | Technology                        |
|------------------|-----------------------------------|
| Language         | Python 3.10                       |
| Message Broker   | Apache Kafka                      |
| Stream Processing| Apache Spark Streaming            |
| Database         | PostgreSQL                        |
| Dashboard        | Streamlit                         |
| Containerization | Docker & Docker Compose           |
| DB Connector     | JDBC (PostgreSQL driver)          |


## ✨ Key Features

- **Realistic vote simulation** — Generates voters, candidates, and votes with randomized data
- **Multi-topic Kafka streaming** — Separate topics for voters, candidates, and votes
- **Real-time aggregation** — Spark Streaming processes and aggregates vote counts live
- **Persistent storage** — All data stored in PostgreSQL for reliability
- **Live election dashboard** — Streamlit displays real-time vote counts, leading candidates, and charts
- **Fully containerized** — One command to start the entire infrastructure


## 📁 Project Structure

```
realtime-voting-data-engineering/
│
├── main.py                    # Vote simulator — generates voters, candidates, votes
├── voting.py                  # Core voting logic and data models
├── spark-streaming.py         # Spark Streaming job — consumes Kafka, writes to PostgreSQL
├── streamlit-app.py           # Live dashboard — reads from PostgreSQL and visualizes
├── python_aggregator.py       # Alternative Python-based vote aggregator
├── test_streamlit.py          # Test file for dashboard components
├── docker-compose.yml         # Kafka, Zookeeper, PostgreSQL container setup
├── requirements.txt           # Python dependencies
└── README.md
```


## ⚙️ Setup & Installation

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Java 8+ (required for Apache Spark)

### Step 1 — Clone the Repository
```bash
git clone https://github.com/Farihakk67/Realtime-Voting-Data-Engineering.git
cd Realtime-Voting-Data-Engineering
```

### Step 2 — Start All Services with Docker
```bash
docker-compose up -d
```
This starts:
- Apache Kafka
- Zookeeper
- PostgreSQL

Wait ~20 seconds for all services to be ready.

### Step 3 — Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Step 4 — Run the Vote Simulator
```bash
python main.py
```
This generates candidates, voters, and votes, then pushes them to Kafka topics.

### Step 5 — Run Spark Streaming Job (separate terminal)
```bash
python spark-streaming.py
```
This consumes from Kafka and writes aggregated results to PostgreSQL.

### Step 6 — Launch the Dashboard (separate terminal)
```bash
streamlit run streamlit-app.py
```
Open `http://localhost:8501` to see the live election results.


## 🗃️ Database Schema

```sql
-- Candidates table
CREATE TABLE candidates (
    candidate_id    VARCHAR PRIMARY KEY,
    candidate_name  VARCHAR,
    party_affiliation VARCHAR,
    photo_url       VARCHAR
);

-- Votes table
CREATE TABLE votes (
    voter_id        VARCHAR,
    candidate_id    VARCHAR,
    voting_time     TIMESTAMP,
    vote            INTEGER DEFAULT 1
);

-- Aggregated vote counts
CREATE TABLE vote_counts (
    candidate_id    VARCHAR PRIMARY KEY,
    total_votes     INTEGER
);
```


## 📊 Dashboard Features

The Streamlit dashboard shows:
- 🥇 Leading candidate with vote count
- 📊 Bar chart of votes per candidate
- 🗺️ Vote distribution by party
- 🔄 Auto-refreshes every few seconds


## 🤝 Contributing

Contributions are welcome.

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "Add your feature"`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request


## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.


## 👩‍💻 Author

**Fariha**
BS Computer Science — Muhammad Ali Jinnah University, Karachi
[GitHub](https://github.com/Farihakk67) • [LinkedIn](https://linkedin.com/in/fariha-kk)

<img width="1024" height="1536" alt="Realtime_Voting" src="https://github.com/user-attachments/assets/99aadfc4-a81f-4924-81bc-a37d5f737301" />
