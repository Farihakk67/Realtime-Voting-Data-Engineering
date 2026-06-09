import time  # Time tracking ke liye (refresh timing aur timestamps)
import matplotlib.pyplot as plt  # Charts aur graphs banane ke liye (bar chart, pie chart)
import pandas as pd  # Data process karne ke liye (tables, filtering, calculations)
import simplejson as json  # JSON data parse karne ke liye (Kafka se aane wala data)
import streamlit as st  # Web dashboard banane ke liye (UI components, display)
from kafka import KafkaConsumer  # Kafka se data lene ke liye (real-time data fetch)
from streamlit_autorefresh import st_autorefresh  # Auto-refresh ke liye (page automatically update)
import psycopg2  # Database se connect karne ke liye (voter counts, candidate info)

# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000)
        return consumer
    except:
        return None

# Function to fetch voting statistics from PostgreSQL
@st.cache_data
def fetch_voting_stats():
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM voters")
        voters_count = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM candidates")
        candidates_count = cur.fetchone()[0]
        conn.close()
        return voters_count, candidates_count
    except:
        return 0, 0

# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    if consumer is None:
        return []
    
    try:
        messages = consumer.poll(timeout_ms=1000)
        data = []
        for message in messages.values():
            for sub_message in message:
                data.append(sub_message.value)
        return data
    except:
        return []

# Function to plot charts - FIXED VERSION
def plot_colored_bar_chart(results):
    plt.figure(figsize=(10, 6))
    
    # Ensure we have lists, not pandas Series with single values
    if len(results) == 1:
        # For single candidate, create a simple bar
        plt.bar([results['candidate_name'].iloc[0]], [results['total_votes'].iloc[0]], 
                color='blue', width=0.5)
    else:
        # For multiple candidates
        plt.bar(results['candidate_name'].tolist(), 
                results['total_votes'].tolist(), 
                color=plt.cm.Set3(range(len(results))))
    
    plt.xlabel('Candidate')
    plt.ylabel('Total Votes')
    plt.title('Vote Counts per Candidate')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return plt

def plot_donut_chart(results):
    fig, ax = plt.subplots(figsize=(8, 8))
    
    if len(results) == 1:
        # For single candidate - show full circle
        ax.pie([100], labels=['100%'], autopct='%1.0f%%', startangle=90)
        ax.text(0, 0, results['candidate_name'].iloc[0], 
                ha='center', va='center', fontsize=14, fontweight='bold')
    else:
        # For multiple candidates
        ax.pie(results['total_votes'].tolist(), 
               labels=results['candidate_name'].tolist(), 
               autopct='%1.1f%%', startangle=90)
    
    # Create donut hole
    centre_circle = plt.Circle((0,0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)
    
    ax.axis('equal')
    plt.title('Vote Distribution', fontsize=16, pad=20)
    return fig

# Dashboard title
st.set_page_config(page_title="Election Dashboard", layout="wide")
st.title('🏛️ Real-time Election Dashboard')

# Sidebar with refresh controls
if st.session_state.get('last_update') is None:
    st.session_state['last_update'] = time.time()

st.sidebar.header("Dashboard Controls")
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
st_autorefresh(interval=refresh_interval * 1000, key="auto")

if st.sidebar.button('🔄 Refresh Data Now'):
    st.rerun()

# Display last refresh time
st.write(f"**Last refreshed:** {time.strftime('%Y-%m-%d %H:%M:%S')}")

# Fetch voting statistics
voters_count, candidates_count = fetch_voting_stats()

# Display metrics
st.markdown("---")
col1, col2 = st.columns(2)
col1.metric("👥 Total Voters", voters_count, delta=None)
col2.metric("🎯 Total Candidates", candidates_count, delta=None)

# Fetch data from Kafka
consumer = create_kafka_consumer("aggregated_votes_per_candidate")
data = fetch_data_from_kafka(consumer)
results = pd.DataFrame(data)

# Store results in session state to accumulate data
if 'all_candidates' not in st.session_state:
    st.session_state.all_candidates = pd.DataFrame()

# Combine new data with existing data
if not results.empty and 'candidate_id' in results.columns:
    if st.session_state.all_candidates.empty:
        st.session_state.all_candidates = results.copy()
    else:
        # Update existing candidates or add new ones
        for _, row in results.iterrows():
            candidate_id = row['candidate_id']
            if candidate_id in st.session_state.all_candidates['candidate_id'].values:
                # Update existing candidate
                idx = st.session_state.all_candidates['candidate_id'] == candidate_id
                st.session_state.all_candidates.loc[idx, 'total_votes'] = row['total_votes']
            else:
                # Add new candidate
                st.session_state.all_candidates = pd.concat([st.session_state.all_candidates, pd.DataFrame([row])], ignore_index=True)
    
    # Use the accumulated data
    display_results = st.session_state.all_candidates.copy()
else:
    # If no data from Kafka, use dummy data
    st.markdown("---")
    st.info("⏳ Waiting for voting data from Kafka...")
    
    # Create dummy data for all 3 candidates
    dummy_data = [
        {'candidate_id': '8326f57e-fc71-49fd-b905-992354db7b62', 
         'candidate_name': 'Lewis Martin', 
         'party_affiliation': 'Management Party', 
         'total_votes': 0, 
         'photo_url': 'https://randomuser.me/api/portraits/men/18.jpg'},
        {'candidate_id': 'cf5ded17-3d54-414e-92e2-d6cf828b2de8', 
         'candidate_name': 'Faith Richards', 
         'party_affiliation': 'Savior Party', 
         'total_votes': 0, 
         'photo_url': 'https://randomuser.me/api/portraits/women/92.jpg'},
        {'candidate_id': '570c97c2-c2bb-4582-be30-f6b62d945096', 
         'candidate_name': 'Stephen Gonzalez', 
         'party_affiliation': 'Tech Republic Party', 
         'total_votes': 0, 
         'photo_url': 'https://randomuser.me/api/portraits/men/68.jpg'}
    ]
    display_results = pd.DataFrame(dummy_data)

# Find leading candidate
if not display_results.empty and 'total_votes' in display_results.columns:
    leading_candidate = display_results.loc[display_results['total_votes'].idxmax()]
else:
    leading_candidate = display_results.iloc[0] if not display_results.empty else None

# Display leading candidate
if leading_candidate is not None:
    st.markdown("---")
    st.header('🏆 Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200, caption=leading_candidate['candidate_name'])
    with col2:
        st.markdown(f"### {leading_candidate['candidate_name']}")
        st.markdown(f"**Party:** {leading_candidate['party_affiliation']}")
        st.markdown(f"### 📊 {leading_candidate['total_votes']} Votes")
        st.progress(min(leading_candidate['total_votes'] / 100, 1.0) if leading_candidate['total_votes'] > 0 else 0)

# Display statistics
st.markdown("---")
st.header('📊 Voting Statistics')

if not display_results.empty and 'candidate_name' in display_results.columns and 'total_votes' in display_results.columns:
    # Prepare display data
    stats_df = display_results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']].copy()
    stats_df = stats_df.reset_index(drop=True)
    stats_df.index = stats_df.index + 1  # Start index at 1
    
    # Display charts in columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Votes Distribution")
        try:
            bar_chart = plot_colored_bar_chart(stats_df)
            st.pyplot(bar_chart)
        except Exception as e:
            st.error(f"Error creating bar chart: {e}")
            # Show data as table instead
            st.dataframe(stats_df[['candidate_name', 'total_votes']])
    
    with col2:
        st.subheader("Percentage Share")
        try:
            donut_chart = plot_donut_chart(stats_df)
            st.pyplot(donut_chart)
        except Exception as e:
            st.error(f"Error creating donut chart: {e}")
            # Calculate percentages manually
            total = stats_df['total_votes'].sum()
            if total > 0:
                stats_df['percentage'] = (stats_df['total_votes'] / total * 100).round(1)
                st.dataframe(stats_df[['candidate_name', 'total_votes', 'percentage']])
    
    # Display detailed table
    st.markdown("---")
    st.subheader("📋 Detailed Results")
    st.dataframe(stats_df, use_container_width=True)
    
else:
    st.warning("No voting data available yet. Votes will appear here once they start coming in.")

# Location data
st.markdown("---")
st.header('📍 Voter Locations')

location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
location_data = fetch_data_from_kafka(location_consumer)
location_df = pd.DataFrame(location_data)

if not location_df.empty and 'state' in location_df.columns and 'count' in location_df.columns:
    # Sort by count descending
    location_df = location_df.sort_values('count', ascending=False)
    location_df = location_df.reset_index(drop=True)
    location_df.index = location_df.index + 1
    
    st.dataframe(location_df, use_container_width=True)
    
    # Show top location
    if len(location_df) > 0:
        top_location = location_df.iloc[0]
        st.metric("📍 Highest Turnout", f"{top_location['state']} - {top_location['count']} voters")
else:
    st.info("Location data will appear here once available")

# Update session state
st.session_state['last_update'] = time.time()

# Debug info (optional - can remove)
with st.sidebar.expander("🔧 Debug Information"):
    st.write(f"**Current results:** {len(display_results)} candidates")
    st.write(f"**Total votes:** {display_results['total_votes'].sum() if not display_results.empty else 0}")
    st.write(f"**Data points from Kafka:** {len(data)}")
    
    # Button to reset accumulated data
    if st.button("Clear Accumulated Data"):
        st.session_state.all_candidates = pd.DataFrame()
        st.rerun()

# Footer
st.markdown("---")
st.caption("Real-time Election Voting System • Data updates every {} seconds".format(refresh_interval))