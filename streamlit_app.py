# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(f" :cup_with_straw: Customize Your Smoothie! :cup_with_straw: ")
st.write(
  """Choose the fruits you want in your custom Smoothie!
  """
)

name_on_order = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
# st.dataframe(data=my_dataframe, use_container_width=True)

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:'
    , my_dataframe,
    max_selections=5
)
if ingredients_list:
    # Normalize ingredients string (space-delimited per your pattern; consider commas if preferred)
    ingredients_string = " ".join(ingredients_list).strip()

    # Escape single quotes to avoid breaking the SQL (still not as safe as binding)
    safe_ingredients = ingredients_string.replace("'", "''")
    safe_name = (name_on_order or "").replace("'", "''")

    my_insert_stmt = f"""
        insert into smoothies.public.orders(ingredients, name_on_order)
        values ('{safe_ingredients}', '{safe_name}')
    """

    # st.code(my_insert_stmt, language="sql")
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")


import requests
smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
st.text(smoothiefroot_response)
