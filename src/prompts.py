import streamlit as st
import pandas as pd

from streamlit_gsheets import GSheetsConnection

SCHEMA_PATH = st.secrets.get("SCHEMA_PATH", "GOLDCAST.ANALYTICS")
QUALIFIED_TABLE_NAME = f"{SCHEMA_PATH}.ACTIVITY_DENORMALIZED"
TABLE_DESCRIPTION = """
This table has information of users,their activities in events or broadcast or booth or session, organizations, surveys,
Activity_type column tells about activities performed by the user.
There are various activity_types performed by users, listing below :-
    * "ENGAGEMENT_SCORE" represents score of user_id or user in particular event_id or event,
    * ATTENDED represents user_id or user attended the live event and his heartbeat is recorded,
    * ATTENDED_ONDEMAND represents user_id or user attended the ondemand event_id or event and his heartbeat is recorded,
    * POLL_RESPONSE represents user_id or user responded to a poll in the broadcast in an event
    * QNA represents user_id or user asked question in the broadcast in an event,
    * ATTENDED_BOOTH_ONDEMAND represents user_id or user attended the booth of an event ondemand ,
    * EVENT_CTA_CLICKS  represents user_id or user clicked event cta,
    * TIME_SPENT_IN_EVENT represents user_id or user spent time in a  live event and his heartbeat is recorded,
    * TIME_SPENT_IN_BROADCAST_ONDEMAND represents user_id or user spent time in an ondemand broadcast in an event and his heartbeat is recorded,
    * ATTENDED_BROADCAST_ONDEMAND  represents user_id or user attended the broadcast of an event ondemand ,
    * TIME_SPENT_IN_BOOTH  represents user_id or user spent time in a  live booth in an event and his heartbeat is recorded,
    * TIME_SPENT_IN_DISCUSSION_GROUP represents user_id or user spent time in a  live disussion groop or room in an event and his heartbeat is recorded,
    * TIME_SPENT_IN_DISCUSSION_GROUP_ONDEMAND represents user_id or user spent time in an ondemand disussion groop or room in an event and his heartbeat is recorded,
    * ENGAGEMENT_SCORE_REAL_TIME represents score or leaderboard of user_id or user in particular  event_id or event which is still live,
    * null represents users who registered but did not perform any activity in the event
    * CHATS represents chat messages of a particular user
    * RESOURCE_DOWNLOAD represents resource click by a particular user
    * ATTENDED_DISCUSSION_GROUP represents user_id or user attended the discussiongroup or room of an event ondemand ,
    * TIME_SPENT_IN_BOOTH_ONDEMAND represents user_id or user spent time in an ondemand booth in an event and his heartbeat is recorded,
    * TIME_SPENT_IN_BROADCAST represents user_id or user spent time in a live broadcast in an event and his heartbeat is recorded,
    * BOOTH_CTA_CLICKS represents user_id or user clicked booth cta,
    * TIME_SPENT_IN_EVENT_ONDEMAND represents user_id or user spent time in an ondemand event and his heartbeat is recorded,
    * ATTENDED_BOOTH represents user_id or user attended the booth of an event in live mode ,
    * ATTENDED_BROADCAST represents user_id or user attended the broadcast of an event when event was live ,
    * ATTENDED_DISCUSSION_GROUP_ONDEMAND represents user_id or user attended the discussion group or room of an event ondemand ,
Useful Hints :-
    1. If the question is to find who all attended the event use filter event_attended_time column is not null (Very Important)
    2. If the question is to find who all attended the event on demand  use filter event_on_demand_attended_time column is not null
    3. If the question is to find who all attended the broadcast use filer activity_type = 'ATTENDED_BROADCAST'
    4. If the question is to find who all attended the broadcast ondemand use filer activity_type = 'ATTENDED_BROADCAST_ONDEMAND'
    5. If the question is to find who all attended the booth ondemand use filer activity_type = 'ATTENDED_BOOTH_ONDEMAND'
    6. If the question is to generate a leaderbord for a running event use filter activity_type = 'ENGAGEMENT_SCORE_REAL_TIME'
    7. If the question is to generate a leaderbord for an ended event use filter activity_type = 'ENGAGEMENT_SCORE'
    8. If the question is to generate chats for a user use filter activity_type = 'CHATS'
    9. If the question is to generate no show data do distinct user_id with filter PK is null
    10. session_id column basically holds broadcast_id or booth_id or room_id, therefore to find out if session_id is reprenting broadcast the broadcast_name column should not be null, similarly for booth and discussion group
    11. whenever using filter on  activity_type column make sure to distinct on activity id (Very Important)
    12. If the question is to find all survey responses or answers related info filter survey_answer_text is not null
    
"""
# This query is optional if running Frosty on your own table, especially a wide table.
# Since this is a deep table, it's useful to tell Frosty what variables are available.
# Similarly, if you have a table with semi-structured data (like JSON), it could be used to provide hints on available keys.
# If altering, you may also need to modify the formatting logic in get_table_context() below.
METADATA_QUERY = f"SELECT VARIABLE_NAME, DEFINITION FROM {SCHEMA_PATH}.FINANCIAL_ENTITY_ATTRIBUTES_LIMITED;"

GEN_SQL = """
You will be acting as an AI Snowflake SQL Expert named Frosty.
Your goal is to give correct, executable sql query to users.
You will be replying to users who will be confused if you don't respond in the character of Frosty.
You are given one table, the table name is in <tableName> tag, the columns are in <columns> tag.
The user will ask questions, for each question you should respond and include a sql query based on the question and the table. 
{context}

Here are 8 critical rules for the interaction you must abide:
<rules>
1. You MUST MUST wrap the generated sql code within ``` sql code markdown in this format e.g
```sql
(select 1) union (select 2)
```
2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
4. Make sure to generate a single snowflake sql code, not multiple. 
5. You should only use the table columns given in <columns>, and the table given in <tableName>, you MUST NOT hallucinate about the table names
6. DO NOT put numerical at the very front of sql variable.
7. You MUST MUST ensure ORGANIZATION_ID and EVENT_ID exist in the sql query, if it not provided please ask from the user.
8. In every query you generate, you must ensure to deduplicate records as much as possible.

</rules>

Don't forget to use "ilike %keyword%" for fuzzy match queries (especially for variable_name column)
and wrap the generated sql code with ``` sql code markdown in this format e.g:
```sql
(select 1) union (select 2)
```

For each question from the user, make sure to include a query in your response.

Now to get started, please briefly introduce yourself, describe the table at a high level, and share the available metrics in 2-3 sentences.
Then provide 3 example questions using bullet points.
"""

@st.cache_data(show_spinner="Loading Frosty's context...")
def get_table_context(table_name: str, table_description: str, metadata_query: str = None):
    table = table_name.split(".")
    conn = st.connection("snowflake")
    columns = conn.query(f"""
        SELECT COLUMN_NAME, DATA_TYPE FROM {table[0].upper()}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{table[1].upper()}' AND TABLE_NAME = '{table[2].upper()}'
        """, show_spinner=False,
    )
    columns = "\n".join(
        [
            f"- **{columns['COLUMN_NAME'][i]}**: {columns['DATA_TYPE'][i]}"
            for i in range(len(columns["COLUMN_NAME"]))
        ]
    )
    context = f"""
Here is the table name <tableName> {'.'.join(table)} </tableName>

<tableDescription>{table_description}</tableDescription>

Here are the columns of the {'.'.join(table)}

<columns>\n\n{columns}\n\n</columns>
    """
    if metadata_query: #Using G sheet
        #metadata = conn.query(metadata_query, show_spinner=False)
        metadata = load_metadata_gsheet()
        metadata = "\n".join(
            [
                f"- **{var_name}**: {metadata[var_name]}"
                for var_name in metadata
            ]
        )
        context = context + f"\n\nAvailable variables by VARIABLE_NAME:\n\n{metadata}"
    return context

def load_metadata_gsheet():
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read()
    metadata_columns = {}

    # Print results.
    for row in df.itertuples():
        # st.write(f"{row.name} has a :{row.pet}:")
        if not pd.isnull(row.DEFINITION):
            metadata_columns[row.VARIABLE_NAME] = row.DEFINITION

    return metadata_columns

def get_system_prompt():
    table_context = get_table_context(
        table_name=QUALIFIED_TABLE_NAME,
        table_description=TABLE_DESCRIPTION,
        metadata_query="True"
    )
    return GEN_SQL.format(context=table_context)

# do `streamlit run prompts.py` to view the initial system prompt in a Streamlit app
if __name__ == "__main__":
    st.header("System prompt for Frosty")
    st.markdown(get_system_prompt())
