# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

st.title(" :cup_with_straw: Customize Your Smoothie! :cup_with_straw: ")
st.write("""Choose the fruits you want in your custom Smoothie!""")

name_on_order = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)

# Ensure Snowpark connector
cnx = st.connection("snowflake", type="snowflake")
session = cnx.session()

# Pull fruit list
sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col('FRUIT_NAME'), col('SEARCH_ON'))
pd_df = sp_df.to_pandas()

# Build options as strings
fruit_options = pd_df['FRUIT_NAME'].tolist()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    options=fruit_options,
    max_selections=5
)

if ingredients_list:
    # Compute once
    ingredients_string = " ".join(ingredients_list).strip()

    # Fast lookup map: FRUIT_NAME -> SEARCH_ON
    lookup = dict(zip(pd_df['FRUIT_NAME'], pd_df['SEARCH_ON']))

    for fruit_chosen in ingredients_list:
        search_on = lookup.get(fruit_chosen, fruit_chosen)
        st.subheader(f'{fruit_chosen} Nutrition Information')
        resp = requests.get(f"https://my.smoothiefroot.com/api/fruit/{search_on}")
        st.dataframe(data=resp.json(), use_container_width=True)

    # Robust insert via Snowpark (avoids SQL string munging)
    time_to_insert = st.button('Submit Order')
    if time_to_insert:
        order_df = session.create_dataframe(
            [(ingredients_string, name_on_order or "")],
            schema=["INGREDIENTS", "NAME_ON_ORDER"]
        )
        order_df.write.mode("append").save_as_table("SMOOTHIES.PUBLIC.ORDERS")
        st.success('Your Smoothie is ordered!', icon="✅")
