# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Snowflake connection
cnx = st.connection("snowflake")
session = cnx.session()

# App title and intro
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write("Choose the fruits you want in your custom Smoothie!")

# Name input
name_on_order = st.text_input('Name on smoothie:')
st.write("The name on your Smoothie will be:", name_on_order)

# Fetch fruit options from Snowflake
my_dataframe = session.table("smoothies.public.fruit_options").select(
    col('FRUIT_NAME'), col('SEARCH_ON')
)

# Convert Snowpark DF to Pandas DF
pd_df = my_dataframe.to_pandas()

# Ingredient selection using fruit names
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    pd_df['FRUIT_NAME'],
    max_selections=5
)

# If ingredients are selected
if ingredients_list:
    ingredients_string = ''
    
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '

        # Get SEARCH_ON value safely
        filtered_row = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen]
        if not filtered_row.empty:
            search_on = filtered_row['SEARCH_ON'].iloc[0]

            # Display fruit nutrition info from external API
            st.subheader(f"{fruit_chosen} Nutrition Information")
            response = requests.get("https://my.smoothiefroot.com/api/fruit/" + search_on)
            
            if response.status_code == 200:
                st.dataframe(data=response.json(), use_container_width=True)
            else:
                st.error(f"Failed to fetch data for {fruit_chosen}")
        else:
            st.warning(f"No matching data found for {fruit_chosen}.")

    # Prepare SQL insert
    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string.strip()}', '{name_on_order}')
    """
    
    st.write("SQL to be executed:")
    st.code(my_insert_stmt)

    # Submit button
    if st.button('SUBMIT ORDER'):
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
        st.stop()
