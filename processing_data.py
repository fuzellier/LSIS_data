import pandas as pd
import xml.etree.ElementTree as ET
import os
import re
import json
from pyproj import Transformer
from tqdm import tqdm

# List of paths for each XML file in each folder
file_list = []

de_months = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni',
            'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember']

en_months = ['January', 'February', 'March', 'April', 'May', 'June',
            'July', 'August', 'September', 'October', 'November', 'December']

for i in os.listdir('data/')[:-2]: # skipping fixed & raw folders
    
    for filename in os.listdir('data/' + str(i) + '/'):
        
        for de_m, en_m in zip(de_months, en_months):
        
            # Renaming original files (and moving if year in name != year in path)
            if bool(re.search(de_m, filename)) == True:

                # Get year string from file name 
                # (not from the path, to prevent issues if a file is a wrong dir)
                year = re.split('(?:.*?\ ){2}([^\.?#]+)', filename)[1]
                
                # Creating folder (for another year) if it does not exist yet
                if not os.path.exists('data/' + year):
                    os.makedirs('data/' + year)
                
                os.rename(
                    'data/' + i + '/' + filename, 
                    'data/' + year + '/' + str((en_months.index(en_m) + 1)) + '_' + year + "_" + en_m.lower() + '.xml'
                )

                filename = str((en_months.index(en_m) + 1)) + '_' + year + "_" + en_m.lower() + '.xml'
        
        # Filling list of paths for each XML file in each folder
        if filename.endswith(".xml"): 
            
            # Year string from new file name
            year = re.split('^[^_]+_([^_]+)_[^_]+$', filename)[1]
            
            # Append path to the list
            file_list.append(os.path.join('data/' + year + '/', filename))
            continue
        else:
            continue
            
def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    return [atoi(c) for c in re.split(r'(\d+)', text)]

# Sorting the list, human order
file_list.sort(key = natural_keys)

# MISSION DataFrame
def mission_infos(root): # root: input XML file root node
    
    df = pd.DataFrame({
        'mission_id'          : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/eid')],
        'mission_nb'          : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatz_nr')],
        'mission_type'        : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatztyp')],
        'mission_tags'        : [x.text.strip() for x in root.findall('./Einsatz/Einsatzgrunddaten/schlagwort')],
        'keyword_1'           : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatzstichwort1')],
        'keyword_2'           : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatzstichwort2')],
        'keyword_3'           : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatzstichwort3')],
        'keyword_4'           : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatzstichwort4')],
        'keyword_5'           : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatzstichwort5')],
        'keyword_6'           : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatzstichwort6')],
        
        # Non-relevant data still present but filled with null values
        'blue_light'          : [None for i in range(len(root))],
        'free_text'           : [None for i in range(len(root))],
        'invoicing_type'      : [None for i in range(len(root))],
        'final_keyword'       : [None for i in range(len(root))],
        'father_id'           : [None for i in range(len(root))],
        'mft_id'              : [None for i in range(len(root))],
        'standing_order_id'   : [None for i in range(len(root))],
        'plan_date'           : [None for i in range(len(root))],
        'fix'                 : [None for i in range(len(root))],
        
        'dispatch_center'     : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/leitstelle')],
        
        'transfer_status'     : [None for i in range(len(root))], 
        'kez_status'          : [None for i in range(len(root))],
        
        'call_time'           : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/meldeeingang')],
        'call_accept_time'    : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/rufannahme')],
        'input_db_time'       : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/beginn_meldungsaufnahme')],
        'end_input_db_time'   : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/ende_meldungsaufnahme')],
        'info_transfer_time'  : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/weiterleitung')],
        'end_mission_time'    : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/einsatzende')],
        'travel_time'         : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/fahrtdauer')],
        'travel_dist_km'      : [x.text for x in root.findall('./Einsatz/Einsatzgrunddaten/fahrtkm')],
    })
    
    # Formatting timestamp data
    l = ['call_time', 'input_db_time', 'end_mission_time']

    for i in l:
        df[i][df[i].notnull()] = [
            pd.to_datetime(x, format = '%d.%m.%Y %H:%M:%S') for x in df[i] if x is not None]
    
    l2 = ['call_accept_time', 'end_input_db_time', 'info_transfer_time']

    for i in l2:
        df[i][df[i].notnull()] = [
            pd.to_datetime(x, format = '%d.%m.%Y-%H:%M:%S') for x in df[i] if x is not None]
    
    df.apply(lambda x: x.str.strip() if x.dtype == "str" else x)
    
    return df   
# end function

# LOCATION DataFrame
def location_infos(root): # root: input XML file root node
    
    df = pd.DataFrame({
        'object'                   : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_objekt')],
        'department'               : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_objekt_abteilung')],
        'street'                   : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_strasse')],
        'city'                     : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_ort')],
        'zip'                      : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_plz')],
        'x_coord'                  : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_xkoord')],
        'y_coord'                  : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_ykoord')],
        'city_district'            : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_ortsteil')],
        'county'                   : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_kreis')],
        'district'                 : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_bezirk')],
        'alias'                    : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_aliasname')],
        'type'                     : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_objekttyp')],
        'department_type_hospital' : [x.text for x in root.findall('./Einsatz/Einsatzort/eo_kh_abteilungtyp')]
    })
    
    # Formatting city_district (slicing before blank + '(' or '-')
    df['city_district'][df['city_district'].notnull()] = [
        re.split(' [(-].*$', x)[0] for x in [x for x in df['city_district']] if x is not None]
    
    # Coordinates convertion from Gauss-Krüger format to lat/long (WGS84)
    # 5678 = DHDN / 3-degree Gauss-Krüger zone 4  (CRS code 31468) but with axis order reversed for GIS applications
    # 4326 = WGS84
    transformer = Transformer.from_crs("epsg:5678", "epsg:4326")
    coord_x = []
    coord_y = []

    for i, j in zip(df['x_coord'], df['y_coord']):
        if i and j is not None:
            x, y  = transformer.transform(i, j)
            coord_x.append(x)
            coord_y.append(y)
        else:
            coord_x.append(i)
            coord_y.append(j)

    # Replace both columns by coord_x, coord_y lists and convert is as numeric
    df['x_coord'], df['y_coord'] = pd.to_numeric(coord_x), pd.to_numeric(coord_y)
    
    df.apply(lambda x: x.str.strip() if x.dtype == "str" else x)
    
    return df
# end function

# DESTINATION DataFrame
def destination_infos(root): # root: input XML file root node
    
    df = pd.DataFrame({
        'object'                   : [x.text for x in root.findall('./Einsatz/Zielort/zo_objekt')],
        'department'               : [x.text for x in root.findall('./Einsatz/Zielort/zo_objekt_abteilung')],
        'street'                   : [x.text for x in root.findall('./Einsatz/Zielort/zo_strasse')],
        'city'                     : [x.text for x in root.findall('./Einsatz/Zielort/zo_ort')],
        'zip'                      : [x.text for x in root.findall('./Einsatz/Zielort/zo_plz')],
        'x_coord'                  : [x.text for x in root.findall('./Einsatz/Zielort/zo_xkoord')],
        'y_coord'                  : [x.text for x in root.findall('./Einsatz/Zielort/zo_ykoord')],
        'city_district'            : [x.text for x in root.findall('./Einsatz/Zielort/zo_ortsteil')],
        'county'                   : [x.text for x in root.findall('./Einsatz/Zielort/zo_kreis')],
        'district'                 : [x.text for x in root.findall('./Einsatz/Zielort/zo_bezirk')],
        'alias'                    : [x.text for x in root.findall('./Einsatz/Zielort/zo_aliasname')],
        'type'                     : [x.text for x in root.findall('./Einsatz/Zielort/zo_objekttyp')],
        'department_type_hospital' : [x.text for x in root.findall('./Einsatz/Zielort/zo_kh_abteilungtyp')],
        'hospital_specialty'       : [x.text for x in root.findall('./Einsatz/Zielort/zo_kh_fachrichtung')]
    })
    
    # Formatting city_district (slicing before blank + '(' or '-')
    df['city_district'][df['city_district'].notnull()] = [
        re.split(' [(-].*$', x)[0] for x in [x for x in df['city_district']] if x is not None]
    
    # Coordinates convertion from Gauss-Krüger format to lat/long (WGS84)
    # 5678 = DHDN / 3-degree Gauss-Krüger zone 4  (CRS code 31468) but with axis order reversed for GIS applications
    # 4326 = WGS84
    transformer = Transformer.from_crs("epsg:5678", "epsg:4326")
    coord_x = []
    coord_y = []

    for i, j in zip(df['x_coord'], df['y_coord']):
        if i and j is not None:
            x, y  = transformer.transform(i, j)
            coord_x.append(x)
            coord_y.append(y)
        else:
            coord_x.append(i)
            coord_y.append(j)

    # Replace both columns by coord_x, coord_y lists and convert is as numeric
    df['x_coord'], df['y_coord'] = pd.to_numeric(coord_x), pd.to_numeric(coord_y)
    
    df.apply(lambda x: x.str.strip() if x.dtype == "str" else x)
    
    return df  
# end function

# ASSETS DataFrame
def assets_infos(root): # root: input XML file root node
    
    df_sub  = pd.DataFrame({})
    df_time = pd.DataFrame({})
    
    assets_col_sub = [
        'did', 'asset_type', 'asset_type_2', 'id', 'radio_name', 'radio_id', 
        'station', 'station_id', 'driving_time', 'distance', 'job_nb', 'x_coord', 
        'y_coord', 'asset_public', 'invoice_nb', 'station_nb', 'station_name'
    ]
    
    assets_time_col = [
        'begin_mission_time', 'alert_time', 'responding_time', 'arrival_time', 
        'leaving_time', 'arrival_patient_time', 'back_vehicle_time', 'back_station_time', 
        'ready_mission_time', 'asset_time_penalty', 'requesting_radio_call', 'radio_call_acceptance'
    ]
    
    # Asset columns names (german)
    de_assets_col = [x.tag for x in root[0][3]]
    
    # Asset columns except timestamps (german)
    de_assets_col_sub = list(filter(re.compile('^(?!em_zeit|sprechwunsch)').match, de_assets_col))
    
    # Timestamps columns names (german)
    de_assets_time = list(filter(re.compile('^(em_zeit|sprechwunsch)').match, de_assets_col))
    
    # Assets DataFrame (w/o timestamps and non-relevant cols)
    for i, j in zip(assets_col_sub, de_assets_col_sub):
        assets_sub = []

        for k in range(len(root.findall('./Einsatz'))):
            assets_sub.append(
                [x.text for x in root[k].findall('./Einsatzmittel/' + j)]
            )

        df_sub[i] = assets_sub

        assets_sub = []
    
    # Given the inversion in the form itself, swap x_coord y_coord columns values
    df_sub.loc[:, ['x_coord','y_coord']] = df_sub.loc[:, ['y_coord', 'x_coord']].values
    
    # Assets timestamps DataFrame (except 2 last)
    for i, j in zip(assets_time_col[:-2], de_assets_time[:-2]):
        assets_timestamp = []

        for k in range(len(root.findall('./Einsatz'))):
            assets_timestamp.append(
                [
                    pd.to_datetime(
                        x.text, errors = 'coerce', format = '%d.%m.%Y %H:%M:%S'
                    ) for x in root[k].findall('./Einsatzmittel/' + j)
                ]
            )

        df_time[i] = assets_timestamp

        assets_timestamp = []
    
    # Add 2 lasts timestamps elements (handling "-" in the regex format)
    for i, j in zip(assets_time_col[-2:], de_assets_time[-2:]):
        assets_timestamp_2 = []

        for k in range(len(root.findall('./Einsatz'))):
            assets_timestamp_2.append(
                [
                    pd.to_datetime(
                        x.text, errors = 'coerce', format = '%d.%m.%Y-%H:%M:%S'
                    ) for x in root[k].findall('./Einsatzmittel/' + j)
                ]
            )

        df_time[i] = assets_timestamp_2

        assets_timestamp_2 = []
    
    # Concatenation, output df
    df = pd.concat([df_sub, df_time], axis = 1)
    
    df.apply(lambda x: x.str.strip() if x.dtype == "str" else x)
        
    return df
# end function

# Converting dataframes into JSON objects
def df_to_json(df_list): 
    
    '''
    Usage:
            parsed_data = df_to_json(df_list)

    Arguments:
            df_list    - list of dataframes

    Returns:
            a list of JSON objects (parsed dataframes)
    '''
    
    res_mission = df_list[0].to_json(
        orient = "records", date_format = "iso", date_unit = "s")
    parsed_mission = json.loads(res_mission)

    res_location = df_list[1].to_json(
        orient = "records", date_format = "iso", date_unit = "s")
    parsed_location = json.loads(res_location)

    res_destination = df_list[2].to_json(
        orient = "records", date_format = "iso", date_unit = "s")
    parsed_destination = json.loads(res_destination)

    res_assets = df_list[3].to_json(
        orient = "records", date_format = "iso", date_unit = "s")
    parsed_assets = json.loads(res_assets)

    return([parsed_mission, parsed_location, parsed_destination, parsed_assets])
# end function

# Building the final JSON object
def json_obj(dict_size, parsed_data):
    
    '''
    Usage:
            json_data = json_obj(dict_size, parsed_data)

    Arguments:
            dict_size    - dictionary size (nb of records)
            parsed_data  - list of JSON objects

    Returns:
            json_data    - final JSON object
    '''

    # Creating dict (json_data) skeleton
    missions_data = [None] * dict_size

    for i in range(dict_size):
        missions_data[i] = { "infos": {}, "location": {}, "destination": {}, "assets": [] }
        
    # Filling the dict
    col_assets = list(df_list[3])

    for i in range(dict_size):
        missions_data[i]['infos']       = parsed_data[0][i]
        missions_data[i]['location']    = parsed_data[1][i]
        missions_data[i]['destination'] = parsed_data[2][i]

        # Filling assets lists
        for j in range(len(parsed_data[3][i]['begin_mission_time'])):
            assets_json = {}

            for item in col_assets:
                assets_json[item] = parsed_data[3][i][item][j]

            missions_data[i]['assets'].append(assets_json) 
    
    # Storing in a 'missions' object
    json_data = {}
    json_data['missions'] = missions_data
    
    return(json_data)
# end function

# Generating the JSON output file
def json_output(data, file, path, in_ext = '.xml', out_ext = '.json'):
    
    '''
    Usage:
            json_file = json_output(data, file, path, in_ext, out_ext)

    Arguments:
            data      - input data to write in the output file
            file      - input file from which to extract the name 
            path      - new folder where to save the file
            in_ext    - input file (2nd argument) extension  (default value: '.xml')
            out_ext   - output file extension                (default value: '.json')

    Returns:
            json_file - output file (same name as the input file, with another extension)
    '''

    # Extract file name from path name (after 2nd slash "/")
    fname = re.split('(?:.*?\/){2}([^\/?#]+)', file)[1]
    fout  = fname.replace(in_ext, out_ext) # replace in_ext extension by out_ext
    
    # Create output folder if it does not exist yet
    if not os.path.exists(path):
        os.mkdir(path)
    
    # JSON output file, now renamed with XML input name
    with open(path + '/' + fout, 'w', encoding = 'utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii = False, indent = 4)
        
    return(json_file)
# end function

# Iterate through the list that contains file paths
# Then open each one and execute the whole script for each

# Progress bar
pbar = tqdm(file_list)

for f in pbar:
    
    # Getting the input XML file root node
    with open(f, 'r', encoding = 'utf-8') as xml_file:
        root = ET.parse(xml_file).getroot()
    
    # Progress bar message
    pbar.set_description("[Processing...]")
       
    # Get file name
    fname = re.split('(?:.*?\/){2}([^\/?#]+)', f)[1]
          
    # Get year string from file name
    year  = re.split('^[^_]+_([^_]+)_[^_]+$', f)[1]
    
    # If JSON version of 'f 'doesn't exist, run the whole process
    if not os.path.isfile(os.path.join('output/', re.split('data/', f.replace('.xml', '.json'))[1])):

        # Building dataframes
        mission_df     = mission_infos(root)
        location_df    = location_infos(root)
        destination_df = destination_infos(root)
        assets_df      = assets_infos(root)

        # List of built dataframes
        df_list = [mission_df, location_df, destination_df, assets_df]

        # Calling fct to convert dataframes into JSON objects
        parsed_data = df_to_json(df_list)

        # Calling fct to build the final JSON object
        json_data = json_obj(len(parsed_data[0]), parsed_data)
        
        # Generating the JSON file
        json_output(json_data, f, path = 'output/' + year)
        pbar.set_postfix_str(fname + " has been processed.")
        
    else: # else... no need to process a file that has been processed
        pbar.set_postfix_str(fname.replace('.xml', '.json') + " already exists.")

print("All the files have been processed.")
