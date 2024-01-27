"""This file contains my solutions about some SQL queries
    using the famous painting database"""

import pandas as pd
from sqlalchemy import create_engine
from configuration.config import (
    DATABASE_USER,
    DATABASE_PASSWORD,
    DATABASE_HOST,
    DATABASE_PORT,
    DATABASE_NAME,
)

# Create a database connection with SQLAlchemy (MySQL Server)
engine = create_engine(
    f"mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
)
conn = engine.connect()  # Connection to the database

# List of csv file names
files = [
    "artist",
    "canvas_size",
    "image_link",
    "museum_hours",
    "museum",
    "product_size",
    "subject",
    "work",
]

# Save data in your SQL database
for file in files:
    df = pd.read_csv(f"data/{file}.csv")
    df.to_sql(file, con=conn, if_exists="replace", index=False)
