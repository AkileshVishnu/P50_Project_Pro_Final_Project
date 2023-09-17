import snowflake.connector

# Replace with your Snowflake account details
conn = snowflake.connector.connect(
    user='akileshvishnu10',
    password='<your_password>',
    account='<your_account>.snowflakecomputing.com',
    warehouse='<your_warehouse>',
    database='<your_database>',
    schema='<your_schema>'
)

# Create a cursor object
cursor = conn.cursor()

# Execute a sample query
cursor.execute("SELECT * FROM your_table")

# Fetch results
for row in cursor.fetchall():
    print(row)

# Close the connection
cursor.close()
conn.close()
