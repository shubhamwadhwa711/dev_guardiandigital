import re
import pandas as pd
from sqlalchemy import create_engine, update, MetaData, text,select, Table, Column, String, Integer,insert
import logging
import numpy as np
import json

from bs4 import BeautifulSoup

logging.basicConfig(filename="newlog.txt", level=logging.DEBUG)

import time 

start_time = time.time()

# infodf = pd.read_excel('Meta_H1_Info-121123.xlsx')
infodf = pd.read_excel('new.xlsx',"New Website Plan Draft")
# mapdf = pd.read_excel('Guardian Digital - Workbook(2).xlsx', 'Meta Data Mapping')
# mapdf = pd.read_excel('GuardianDigital-Workbook-121123.xlsx', 'Meta Data Mapping')
mapdf = pd.read_excel('new.xlsx', 'Meta Data Mapping')
infodf.columns = infodf.iloc[0]
infodf = infodf.iloc[1:]

infodf['map_id'] = infodf.index + 2
infodf['map_data'] = ''
infodf['meta_tag'] = False
infodf['H1_tag'] = False

# Connection parameters
# conn_params = {
#     'user': "###",
#     'password': "####",
#     'host': "####",
#     'port': 3306,
#     'database': "###"
# }

# conn_params = {
#     'user': "gdjoomla",
#     'password': "XXXXX",
#     'host': "havoc.guardiandigital.com",
#     'port': 3306,
#     'database': "gdv4webstagej4"
# }
conn_params = {
    'user': "root",
    'password': "1234",
    'host': "127.0.0.1",
    'port': 3306,
    'database': "test"
}

# Establish a connection
# db_url = "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s" % conn_params
# engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)

engine = create_engine("mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s" % conn_params , future=True, pool_pre_ping=True, pool_recycle=3600)
conn = engine.connect()

meta = MetaData()
meta.reflect(bind=engine)
menu_table = pd.read_sql_query(text("SELECT * FROM w5zxq_menu where type <> 'url'"), conn)
content_table = pd.read_sql_query(text("SELECT * FROM w5zxq_content"), conn)
edocman_categories_table = pd.read_sql_query(text("SELECT * FROM w5zxq_edocman_categories"), conn)
categories_table = pd.read_sql_query(text("SELECT * FROM w5zxq_categories"), conn)
casestudies_table = pd.read_sql_query(text("SELECT * FROM w5zxq_casestudies_iq"), conn)
modules_table = pd.read_sql_query(text("SELECT * FROM w5zxq_modules where module='mod_header_iq' "), conn)
fields_values_table = pd.read_sql_query(text("SELECT * FROM w5zxq_fields_values"), conn)


infodf['Inspiration / Current URL (if existing page)'] = infodf['Inspiration / Current URL (if existing page)'].fillna('')
infodf['Old H1'] = infodf['Old H1'].fillna('')
infodf['New Header (H1)'] = infodf['New Header (H1)'].fillna('')
infodf['Old Title'] = infodf['Old Title'].fillna('')
infodf['New Title'] = infodf['New Title'].fillna('')



infodf.replace({np.nan: ''}, inplace=True)

# Handling null
infodf['Old Metas'] = infodf['Old Metas'].fillna('')
infodf['URL'] = infodf['URL'].fillna('')

infodf['Old Metas'] = infodf['Old Metas'].apply(lambda x: x.replace('\\n', '\n').replace('\\r', '\r'))

infodf['Extracted Text'] = infodf['Inspiration / Current URL (if existing page)'].apply(lambda url: url.split('/')[-1])  
map_dic = {}
for i, vals in mapdf.iterrows(): 
    if '-' in str(vals['Rows']):
        if vals["Rows"] == "-":
            continue 
        strat = int(vals['Rows'].split('-')[0])
        end = int(vals['Rows'].split('-')[1])

        for num in range(strat, end+1):
            map_dic[num] = [vals['Type'],vals['H1 Location (Table - Column)'],vals['Meta Desc Location (Table - Column)']]
    else:
        map_dic[vals['Rows']] = [vals['Type'],vals['H1 Location (Table - Column)'],vals['Meta Desc Location (Table - Column)']]

# Initialize a list to store the link values
Meta_link_values = []
H1_link_values = []
Article_link_values = []

for i, extracted_text in infodf.iterrows():
    Meta_link_value = ''
    H1_link_value = ''
    Article_link_value = ''
    
    if extracted_text['map_id'] in map_dic:
        if extracted_text['map_id'] == 345:
            print('heeee')
        # extracted_text['map_data'] = map_dic[extracted_text['map_id']]
        infodf.loc[i, 'map_data'] = str(map_dic[extracted_text['map_id']])
        if extracted_text['URL'] == 'https://guardiandigital.com/':
            infodf.loc[i, 'Extracted Text'] =  'home'
            extracted_text['Extracted Text'] = 'home'
            extracted_text['Inspiration / Current URL (if existing page)'] = 'https://guardiandigital.com/home'

        if '.jpg' in extracted_text['URL']:
            match = re.search(r'/([^/]+)\?', extracted_text['URL'])
            updated_url = match.group(1)
            infodf.loc[i, 'Extracted Text'] =  updated_url
            extracted_text['Extracted Text'] = updated_url
            extracted_text['Inspiration / Current URL (if existing page)'] = 'https://guardiandigital.com/'+updated_url

        if 'w5zxq_menu' in map_dic[extracted_text['map_id']][2]:
            try:
                if extracted_text['Extracted Text'] in menu_table['alias'].values:
                    # link_value = menu_table.loc[menu_table['alias'] == extracted_text['Extracted Text'], 'link'].values[0]
                    infodf.loc[i, 'meta_tag'] = True
                    #Meta_link_value = map_dic[extracted_text['map_id']][2].split(' - ')[0]
                    
                    Meta_link_h1 = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'link'].values[0]
                    # metaMetaTable = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'params'].values[0]
                    Meta_link_id = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'id'].values[0]
                    # print(metaMetaTable)
                    Meta_link_value = f'index.php?option=com_menu&view=metadescription&id={Meta_link_id}'
                    h1_table = map_dic[extracted_text['map_id']][1].split('_')[1].split(' - ')[0]
                    if h1_table in Meta_link_h1 and h1_table == 'sppagebuilder' and extracted_text['Extracted Text'] != 'home':
                        infodf.loc[i, 'H1_tag'] = True
                        H1_link_value = Meta_link_h1
                elif  extracted_text['Extracted Text'] in content_table['alias'].values and 345 <= extracted_text['map_id'] <= 366:
                    infodf.loc[i, 'meta_tag'] = True
                    id_content = content_table.loc[content_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                    Meta_link_value = f'index.php?option=com_content&view=article&id={id_content}'

            except Exception as e:
                Meta_link_value = ''
        elif 'w5zxq_content' in map_dic[extracted_text['map_id']][2] and extracted_text['Extracted Text'] in content_table['alias'].values:
            infodf.loc[i, 'meta_tag'] = True
            try:
                id_content = content_table.loc[content_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                Meta_link_value = f'index.php?option=com_content&view=article&id={id_content}'
                Article_link_value  =f'index.php?option=com_content&view=article&id={id_content}'

            except:
                Meta_link_value = ''
                Article_link_value   = ''
        elif 'w5zxq_edocman_categories' in map_dic[extracted_text['map_id']][2] and extracted_text['Extracted Text'] in edocman_categories_table['alias'].values:
            infodf.loc[i, 'meta_tag'] = True
            try:
                id_category = edocman_categories_table.loc[edocman_categories_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                Meta_link_value = f'index.php?option=com_edocman_categories&view=categories&id={id_category}'
            except:
                Meta_link_value = ''
        elif 'w5zxq_casestudies_iq' in map_dic[extracted_text['map_id']][1] and extracted_text['Extracted Text'] in casestudies_table['alias'].values:
            infodf.loc[i, 'meta_tag'] = True
            try:
                id_casestudios = casestudies_table.loc[casestudies_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                Meta_link_value = f'index.php?option=com_casestudies_iq&view=casestudies&id={id_casestudios}'
            except:
                Meta_link_value = ''
    
        else:
            infodf.loc[i, 'meta_tag'] = False
            Meta_link_value = ''

        # H1_link_value
        if 'w5zxq_menu' in map_dic[extracted_text['map_id']][1] and extracted_text['Extracted Text'] in menu_table['alias'].values:
            # link_value = menu_table.loc[menu_table['alias']   == extracted_text['Extracted Text'], 'link'].values[0]
            infodf.loc[i, 'H1_tag'] = True
            try:
                # H1_link_value = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'link'].values[0]
                
                H1_link_value_id = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'id'].values[0]
                # print(metaMetaTable)
                H1_link_value = f'index.php?option=com_menu&view=metadescription&id={H1_link_value_id}'
 
            except Exception as e:
                H1_link_value = ''
        elif 'w5zxq_content' in map_dic[extracted_text['map_id']][1] and extracted_text['Extracted Text'] in content_table['alias'].values:
            infodf.loc[i, 'H1_tag'] = True
            try:
                id_content = content_table.loc[content_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                H1_link_value = f'index.php?option=com_content&view=article&id={id_content}'

            except:
                H1_link_value = ''
        elif 'w5zxq_edocman_categories' in map_dic[extracted_text['map_id']][1] and extracted_text['Extracted Text'] in edocman_categories_table['alias'].values:
            infodf.loc[i, 'H1_tag'] = True
            try:
                id_category = edocman_categories_table.loc[edocman_categories_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                # H1_link_value = f'index.php?option=com_categories&view=categories&id={id_category}'
                H1_link_value = f'index.php?option=com_edocman_categories&view=categories&id={id_category}'
            except:
                H1_link_value = ''
        elif 'w5zxq_categories' in map_dic[extracted_text['map_id']][1] and extracted_text['Extracted Text'] in categories_table['alias'].values:
            infodf.loc[i, 'H1_tag'] = True
            try:
                id_category = categories_table.loc[categories_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                # H1_link_value = f'index.php?option=com_edocman_categories&view=categories&id={id_category}'
                H1_link_value = f'index.php?option=com_categories&view=categories&id={id_category}'
            except:
                H1_link_value = ''
        elif 'w5zxq_casestudies_iq' in map_dic[extracted_text['map_id']][1] and extracted_text['Extracted Text'] in casestudies_table['alias'].values:
            infodf.loc[i, 'H1_tag'] = True
            try:
                id_casestudios = casestudies_table.loc[casestudies_table['alias'] == extracted_text['Extracted Text'], 'id'].values[0]
                H1_link_value = f'index.php?option=com_casestudies_iq&view=casestudies&id={id_casestudios}'
            except:
                H1_link_value = ''
        elif 'w5zxq_modules' in map_dic[extracted_text['map_id']][1] and modules_table['params'].str.contains(re.escape(extracted_text['Old H1'])).sum() > 0:
            infodf.loc[i, 'H1_tag'] = True
            try:
                if (extracted_text['Old H1'] and modules_table['params'].str.contains('"'+re.escape(extracted_text['Old H1']+'"')).sum() == 1 ):
                    id_menutable = modules_table.loc[modules_table['params'].str.contains(re.escape(extracted_text['Old H1'])), 'id'].values[0]
                    H1_link_value = f'index.php?option=com_modules&view=modules&id={id_menutable}'
            except:
                H1_link_value = ''


    Meta_link_values.append(Meta_link_value)
    H1_link_values.append(H1_link_value)
    Article_link_values.append(Article_link_value)


print("Meta Description is updated")


# Add the link_values list as a new column in infodf
infodf['Meta_Link'] = Meta_link_values
infodf['H1_Link'] = H1_link_values
infodf['Article_Link'] = Article_link_values

# Extract values for 'id' and 'com' columns from the 'Link' column
infodf['H1_id'] = infodf['H1_Link'].apply(lambda link: link.split('=')[-1] if '=' in link else '')
infodf['H1_com'] = infodf['H1_Link'].apply(lambda link: 'w5zxq_' + link.split('com_')[1].split('&')[0]  if 'com_' in link else '')
infodf['Meta_id'] = infodf['Meta_Link'].apply(lambda link : link.split('=')[-1] if '=' in link else '')
infodf['Meta_com'] = infodf['Meta_Link'].apply(lambda link: 'w5zxq_' + link.split('com_')[1].split('&')[0]  if 'com_' in link else '')
infodf['Article_link'] = infodf['Article_Link'].apply(lambda link: 'w5zxq_' + link.split('com_')[1].split('&')[0]  if 'com_'  and 'content' in link else '')
infodf['Article_id'] = infodf['Article_Link'].apply(lambda link: link.split('=')[-1] if '=' in link else '')
article_ids =[]
for article_id in infodf['Article_id']:
    article_ids.append(article_id)
    print(article_id)


infodf['H1_Updated_DB'] = False
infodf['Meta_Updated_DB'] = False

infodf['Fields_Tables_D'] = False

infodf.to_csv('file_name55.csv')
#print(infodf)
#exit(1)
# Create a list to store the queries and results
query_results = []

updated_values = set()

w5zxq_modules = meta.tables['w5zxq_modules']

logging.info(f'\t\t\t\t\t\tOLD\t\t\t\t\t\t\t\t\tNew')






# def update_fields_values_table()
        


for h1_id_value, h1_com_value, meta_id_value, meta_com_value, map_id, map_data, meta_tag, H1_tag, new_H1, old_h1, new_title, old_title, old_Meta, New_Meta in zip( 
    infodf['H1_id'], infodf['H1_com'],                                                                                           
    infodf['Meta_id'], infodf['Meta_com'],
    infodf['map_id'], infodf['map_data'],
    infodf['meta_tag'], infodf['H1_tag'],
    infodf['New Header (H1)'],
    infodf['Old H1'],
    infodf['New Title'],
    infodf['Old Title'],
    infodf['Old Metas'],
    infodf['New Meta Description']):
    
    try:
        if h1_com_value and h1_id_value.isdigit():
            if h1_com_value != 'w5zxq_rsform' and h1_com_value != 'w5zxq_blockcontent' and h1_com_value != 'w5zxq_edocman':
            # if com_value in ('w5zxq_sppagebuilder' , 'w5zxq_content'):
                # Construct the SQL query with the table name from 'com' column
                tableComValue = meta.tables[h1_com_value]
                print(tableComValue)
                # query = select(tableComValue).where(tableComValue.c.id == h1_id_value) 
                query = select(tableComValue).where(tableComValue.c.id == h1_id_value) # # change back to above condition

                # print(query)
                try:
                    # Execute the query to check if the table exists
                    df = pd.read_sql_query(query, conn)
                except pd.io.sql.DatabaseError as e:
                    # print(f"Error executing query for table '{com_value}': {e}")
                    continue

                # Check if the table exists
                if df.empty:
                    # print(f"Table '{com_value}' doesn't exist. Skipping...")
                    continue
                
                if map_id in ( 401, 403,  406):
                    print(map_id)

                for i, row in df.iterrows():
                    update_stmt = ''
                    # 1 sppage_builder --> replace of H1
                    if 'sppagebuilder' in h1_com_value:
                        tablePageBuilder = meta.tables[h1_com_value]

                        column_name  = re.search(r'`([^`]+)`', map_dic[map_id][1])
                        if column_name:
                            col_name = column_name.group(1)                       

                        if col_name == 'text':
                            text = row[col_name]

                            updated_string = text.replace(old_h1.strip(), new_H1)    

                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        spacing1 = max_spacing - len(new_H1)
                        # Update the value in the database
                        
                        if updated_string != text and len(old_h1) != 0:
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True

                            logging.info(f'Sppagebuilder Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}')
                        
                             # Construct the SQL update statement
                            update_stmt = update(tablePageBuilder).where(tablePageBuilder.c.id == int(h1_id_value)).values(text=updated_string )
                            with engine.begin() as connection:
                                connection.execute(update_stmt)
                        else:
                            updated_flag = False
                            sp_text = json.loads(text)

                            for data in sp_text:
                                # print(data)
                                for cols_data in data['columns']:
                                    if len(cols_data['addons']) != 0:
                                        for addons_data in cols_data['addons']:
                                            if 'heading_selector' in addons_data['settings']:
                                                if addons_data['settings']['heading_selector'] == 'h1':
                                                    title = addons_data['settings']['title']

                                                    soup = BeautifulSoup(title, 'html.parser')

                                                    span_tag_exists = soup.find('span') is not None
                                                    old_titile_sp = soup.get_text().replace('\n', ' ')
                                                    old_titile_sp = re.sub(r'\s+', ' ', old_titile_sp).strip()
                                                    # if span_tag_exists:
                                                    #     span_names = ['sppb-addon-title', 'fince-txt']

                                                        # text_dict = {}
                                                        # for span_name in span_names:
                                                        #     elements = soup.find_all(class_=span_name)
                                                        #     span_text = ' '.join([element.get_text() for element in elements])
                                                        #     text_dict[span_name] = span_text.replace('\n', '')
                                                        # old_titile_sp = ' '.join(text_dict.values())
                                                        # if len(old_titile_sp) == 1 or len(old_titile_sp) == 0:

                                                    # else:
                                                    #     old_titile_sp = soup.get_text().replace('\n', '')

                                                    new_title_sp = old_titile_sp.replace(old_h1.strip(), new_H1)
                                                    if old_titile_sp != new_title_sp:
                                                        updated_flag = True 
                                                        if span_tag_exists:
                                                            addons_data['settings']['title'] = f'<span class="sppb-addon-title">{new_title_sp}</span>'
                                                        else:
                                                            addons_data['settings']['title'] = new_title_sp

                            updated_string = json.dumps(sp_text)
                            if updated_string != text and updated_flag == True and len(old_h1) != 0:
                                infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True
                                logging.info(f'Sppagebuilder Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}')
                            
                                # Construct the SQL update statement
                                update_stmt = update(tablePageBuilder).where(tablePageBuilder.c.id == int(h1_id_value)).values(text=json.dumps(sp_text) )
                                with engine.begin() as connection:
                                    connection.execute(update_stmt)



                        # logging.info(f'Sppagebuilder Table: {h1_id_value}-{old_title}{" " * spacing1}{new_title}')
                        #     # Construct the SQL update statement
                        # update_stmt = update(tablePageBuilder).where(tablePageBuilder.c.id == int(h1_id_value)).values(title=new_title)
                        # with engine.begin() as connection:
                        #     connection.execute(update_stmt)
                    # 2 _content --> replace of title
                    elif 'w5zxq_menu' in h1_com_value: # 
                        tablemenu = meta.tables['w5zxq_menu']
                        params = row['params']

                        updated_string = params.replace(old_h1.strip(), new_H1)    
                        
                        if updated_string != params :
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True
                            logging.info(f'w5zxq_menu Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}')
                        
                             # Construct the SQL update statement
                            update_stmt = update(tablemenu).where(tablemenu.c.id == int(h1_id_value)).values(params=updated_string )
                            with engine.begin() as connection:
                                connection.execute(update_stmt)

                    elif '_content' in h1_com_value: # Done for both H1 and Title
                        tableContent = meta.tables['w5zxq_content']
                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        logging.info(f'Content	Table:{h1_id_value}-{old_h1}{" " * spacing}{new_H1}')
                        # Construct the SQL update statement
                        update_stmt_h1 = update(tableContent).where(tableContent.c.id == int(h1_id_value)).values(title=new_H1)
                        with engine.begin() as connection:
                            connection.execute(update_stmt_h1)
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True
                                
                        newMeta = New_Meta
                        update_stmt_meta = update(tableContent).where(tableContent.c.id == int(h1_id_value)).values(metadesc=newMeta)

                        logging.info(f'Content Meta Table:{h1_id_value} --- New Meta Description = {newMeta}')
                        # Execute the update statement
                        with engine.begin() as connection:
                            connection.execute(update_stmt_meta)
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'Meta_Updated_DB'] = True

                    # 3 _edocman_categories --> replace of title
                    elif 'edocman' in h1_com_value:
                        tableCategories = meta.tables['w5zxq_edocman_categories']
                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        logging.info(f'edocman_categories: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}')
                        # Construct the SQL update statement
                        update_stmt_h1 = update(tableCategories).where(tableCategories.c.id == int(h1_id_value)).values(title=new_H1)
                        with engine.begin() as connection:
                            connection.execute(update_stmt_h1)
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True
                        # print("Value updated in the database.")

                        newMeta = New_Meta
                        update_stmt_meta = update(tableCategories).where(tableCategories.c.id == int(h1_id_value)).values(metadesc=newMeta)

                        logging.info(f'edocman_categories Meta Table:{h1_id_value} --- New Meta Description = {newMeta}')
                        # Execute the update statement
                        with engine.begin() as connection:
                            connection.execute(update_stmt_meta)
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'Meta_Updated_DB'] = True

                    elif 'categories' in h1_com_value:
                        max_spacing = 90
                        tableCategories = meta.tables['w5zxq_categories']
                        description = row['description']
                        updated_string = description.replace(old_h1.strip(), new_H1)    
                        
                        if updated_string != description :
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True
                            logging.info(f'categories Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}')
                        
                             # Construct the SQL update statement
                            update_stmt = update(tableCategories).where(tableCategories.c.id == int(h1_id_value)).values(description=updated_string )
                            with engine.begin() as connection:
                                connection.execute(update_stmt)

                        # print("Value updated in the database.")

                    # 4 __casestudies_iq --> replace of title
                    elif 'casestudies_iq' in h1_com_value:
                        tableCaseStudies = meta.tables['w5zxq_casestudies_iq']
                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        logging.info(f'casestudies_iq Table: {old_h1}{" " * spacing}{new_H1}')
                        # Construct the SQL update statement
                        update_stmt = update(tableCaseStudies).where(tableCaseStudies.c.id == int(h1_id_value)).values(title=new_H1)
                        with engine.begin() as connection:
                            connection.execute(update_stmt)
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True
                        # print("Value updated in the database.")

                    # 4 __casestudies_iq --> replace of title
                    elif 'modules' in h1_com_value:
                        tableModules = meta.tables['w5zxq_modules']
                        paramsJson = row['params']
                        if (old_h1 != new_H1):
                            updated_string_params = paramsJson.replace(old_h1.strip(), new_H1)
                            logging.info(f'Module Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}')
                                # Construct the SQL update statement
                            update_stmt = update(tableModules).where(tableModules.c.id == int(h1_id_value)).values(params=updated_string_params )
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'H1_Updated_DB'] = True
                            with engine.begin() as connection:
                                connection.execute(update_stmt)
                                

        # Update Meta Drescription
        if meta_com_value and meta_id_value.isdigit():
                tableMetaComValue = meta.tables[meta_com_value]
                # query = select(tableComValue).where(tableComValue.c.id == h1_id_value) 
                meta_query = select(tableMetaComValue).where(tableMetaComValue.c.id == meta_id_value) # change back to above condition

                # print(query)
                try:
                    # Execute the query to check if the table exists
                    meta_df = pd.read_sql_query(meta_query, conn)
                except pd.io.sql.DatabaseError as e:
                    # print(f"Error executing query for table '{com_value}': {e}")
                    continue

                # Check if the table exists
                if meta_df.empty:
                    # print(f"Table '{com_value}' doesn't exist. Skipping...")
                    continue

                for i, row in meta_df.iterrows():

                    if 'w5zxq_menu' in meta_com_value:
                        tablemenu = meta.tables['w5zxq_menu']
                        params = row['params']

                        oldMeta = json.loads(params)
                        newMeta = New_Meta
                        if   newMeta[-1] == '"':
                            newMeta = newMeta[:-1]
                        oldMeta['menu-meta_description'] = newMeta

                        # updated_meta = params.replace(old_Meta, New_Meta)    
                        update_stmt = update(tablemenu).where(tablemenu.c.id == int(meta_id_value)).values(params=json.dumps(oldMeta))
                        infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'Meta_Updated_DB'] = True
                        with engine.begin() as connection:
                            connection.execute(update_stmt)
                    
                    elif '_content' in meta_com_value and 345 <= map_id <= 366: # Done for both H1 and Title
                        tableContent = meta.tables['w5zxq_content']
  
                        newMeta = New_Meta
                        update_stmt_meta = update(tableContent).where(tableContent.c.id == int(meta_id_value)).values(metadesc=newMeta)

                        logging.info(f'Content Meta Table:{meta_id_value} --- New Meta Description = {newMeta}')
                        # Execute the update statement
                        with engine.begin() as connection:
                            connection.execute(update_stmt_meta)
                            infodf.loc[(infodf['map_id'] == map_id) & (infodf['map_data'] == map_data ), 'Meta_Updated_DB'] = True

                        # if oldMeta != params:
                        #     update_stmt = update(tablemenu).where(tablemenu.c.id == int(meta_id_value)).values(params=updated_meta )
                        #     with engine.begin() as connection:
                        #         connection.execute(update_stmt)
                        # else:
                        #     print('w5zxq_menu not updated some error')

                        # metaMetaTable = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'params'].values[0]
                        # if not pd.isna(metaMetaTable):
                        #     oldMeta = json.loads(metaMetaTable)
                        #     newMeta = extracted_text['New Meta Description']
                        #     oldMeta['menu-meta_description'] = newMeta
                        #     tableMeta = meta.tables['w5zxq_menu']
                            
                        #     id_to_update = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'id'].values[0]
                            # update_stmt = update(tableMeta).where(tableMeta.c.id == id_to_update).values(params=json.dumps(oldMeta))
                            

                            # logging.info(f'Menu Table: {id_to_update} \n  New Meta Description: {newMeta}')
                            # # Execute the update statement
                            # with engine.begin() as connection:
                    #     connection.execute(update_stmt)



        # if meta_com_value and meta_id_value.isdigit():

    except pd.io.sql.DatabaseError as e:
        print(f"Error executing query for table '{h1_com_value}': {e}")



def update_fields_values_table(article_ids):
    for i in article_ids:
        if i == "":
            continue
        else:
            new_title = infodf['New Title'][int(i)]
            infodf["field_id"] = 2
            table = meta.tables['w5zxq_fields_values']
            
            insert_query = insert(table).values(field_id=2, item_id=i, value=new_title)
            
            # Start a transaction
            with engine.begin() as conn:
                result = conn.execute(insert_query)
                
            # Commit the transaction
            conn.commit()

update_fields_values_table(article_ids)


print("Text and Titles are upadted in DB")

#commit the changes into DB
connection.commit()
# Close the cursor and connection
conn.close()
end_time = time.time()

print("time: ",end_time - start_time)

infodf.to_csv('file_name55.csv')








