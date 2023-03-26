from flask import Flask,jsonify,request
import json
import pandas as pd
import numpy as np
from pandas import json_normalize
import warnings
import re
app =   Flask(__name__)

@app.route('/up_json', methods=['POST'])
def upload_json():
    Input=request.json
    df_input =json_normalize(Input['Input'])
    df_EUTaxonomie = json_normalize(Input['EU_Taxonomy_version1'])
    df_performance=json_normalize(Input['Draft_Performance_Tracking_Global+Predicted'])

    """Standardize the "Building" column in both dataframes by making the following modifications:
    - Add a space before any digit in the "Building" column value using regex expression r'(\d+)', r' \1'
    - Capitalize the first letter of each word in the "Building" column value using .title() method
    - Replace multiple spaces with a single space in the "Building" column value using '\s+', ' ' regex expression"""
    df_input['Building'] = df_input['Building'].apply(lambda x: re.sub(r'(\d+)', r' \1', x)).str.title().replace('\s+', ' ', regex=True)
    df_EUTaxonomie['Building'] = df_EUTaxonomie['Building'].apply(lambda x: re.sub(r'(\d+)', r' \1', x)).str.title().replace('\s+', ' ', regex=True)
    # Get unique building names from the EUTaxonomie dataframe
    buildings_EUTaxonomie = set(df_EUTaxonomie['Building'].unique())
    # Filter the input dataframe to only keep buildings that exist in the EUTaxonomie dataframe
    df_input = df_input[df_input['Building'].isin(buildings_EUTaxonomie)]
    # Check that all necessary inputs are present in the DataFrame
    necessary_input = ["Solaire", "Quotation", "Opex", "Target_Opex", "Aligned_Opex", "Current_Opex", "NewQuotation","UserID"]
    necessary_input_now = ["Solaire", "Quotation", "Opex","UserID"]
    for n in necessary_input_now:
        if n not in df_input.columns:
            df_input[n]=0 # To this or # error_msg = {'error': 'The column {} is not present in the DataFrame.'.format(n)}
            
    # Convert columns to the right type and format
    df_input[necessary_input_now[:-1]] = df_input[necessary_input_now[:-1]].astype(np.float32) # Convert all necessary input columns to float32 except UserID
    df_input["UserID"]=df_input["UserID"].fillna(0)
    df_input["UserID"] = df_input["UserID"].astype(int)

    df1=pd.merge(df_input,df_EUTaxonomie, how='right', on = 'Building')
    df1_input=pd.merge(df_input,df_EUTaxonomie, how='right', on = 'Building')
    df2=pd.merge(df_input,df_performance, how='right', on = 'Building')
      
    df1[['Solaire', 'Solaire_(panneaux_Pv)']] = df1[['Solaire', 'Solaire_(panneaux_Pv)']].astype('double').fillna(0)
    df1['Solaire_Totale'] = df1['Solaire'] + df1['Solaire_(panneaux_Pv)']
    df1_input[['Solaire', 'Solaire_(panneaux_Pv)']] = df1_input[['Solaire', 'Solaire_(panneaux_Pv)']].astype('double').fillna(0)
    df1_input['Solaire_Totale'] = df1_input['Solaire'] + df1_input['Solaire_(panneaux_Pv)']

    def data_energie():
        # Convert 'Quotation' column to float and fill NaN with 0
        df1_input['Quotation'] = df1_input['Quotation'].astype('float').fillna(0)
        # If 'Quotation' is not 0, keep it, otherwise set it to 0.131
        df1_input['Quotation'] = df1_input['Quotation'].apply(lambda x: x if x != 0 else 0.131)
        # Convert 'Solaire' column to float and fill NaN with 0
        df1_input['Solaire'] = df1_input['Solaire'].astype('float').fillna(0)
        # Calculate 'Saving_Self_Conso' and 'Saving_Self_Conso_Input' columns
        df1_input['Saving_Self_Conso'] = df1_input['Solaire_Totale'] * df1_input['Quotation']
        df1_input['Saving_Self_Conso_Input'] = df1_input['Solaire'] * df1_input['Quotation']
        # Calculate the energy-related metrics and store them in a dictionary
        jsonEnergie = {}
        jsonEnergie['Volume_Of_Generated_KWh'] = df1_input['Solaire_Totale'].sum().round(2)
        jsonEnergie['Volume_Of_Generated_vs_default'] = ((df1_input['Solaire'].sum() * 100) / df1_input['Solaire_(panneaux_Pv)'].sum()).round(2)
        jsonEnergie['Self-Consumption_Rate'] = ((df1_input['Solaire_Totale'].sum() / df1_input['Consommation'].sum()) * 100).round(2)
        jsonEnergie['Self-Conso_vs_default'] = ((df1_input['Solaire'].sum() / df1_input['Consommation'].sum()) * 100).round(2)
        jsonEnergie['Saving_Self_Conso'] = df1_input['Saving_Self_Conso'].sum().round(2)
        jsonEnergie['Saving_Self_Conso_vs_default'] = df1_input['Saving_Self_Conso_Input'].sum().round(2)
        jsonEnergie['Percentage_Saving_Self_Conso_vs_default'] = (jsonEnergie['Saving_Self_Conso_vs_default'] * 100 / jsonEnergie['Saving_Self_Conso']).round(2)
        
        # Return the dictionary containing the energy-related metrics
        return jsonEnergie


    def data_building():
        # Assign df1 to df_building for clarity
        df_building = df1
        # Sort the data by building name in ascending order
        df_building.sort_values(by='Building', ascending=True, inplace=True)
        # Convert Dep_Kwh_m2_an and Solaire_Totale columns to float and fill in missing values with 0
        df_building[['Dep_Kwh_m2_an','Solaire_Totale']] = df_building[['Dep_Kwh_m2_an','Solaire_Totale']].astype('float').fillna(0).round(2)
        # df_building['Solaire_Totale'] = df_building['Solaire_Totale'].astype('float').fillna(0).round(2)
        # Merge df_input with df_building on the "Building" column
        df_building = pd.merge(df_input, df_building, how='left', on='Building')
        # Select only the relevant columns
        df_building = df_building[['Building', 'Dep_Kwh_m2_an', 'Consommation', 'Solaire_Totale']]
        # Define dictionary to convert data types and dictionary to round data
        convert_dict = {"Building": str, "Dep_Kwh_m2_an": float, "Consommation": float, "Solaire_Totale": float}
        arround = {"Building": 2, "Dep_Kwh_m2_an": 2, "Consommation": 2, "Solaire_Totale": 2}
        # Convert data types and round data
        df_building = df_building.astype(convert_dict)
        df_building = df_building.round(arround)
        # Convert the dataframe to a list of dictionaries
        json_Building = df_building.to_dict(orient='records')
        # Return the list of dictionaries
        return json_Building

    def conso_vs_solar():
        # Use inner join instead of left join and pass the on argument as a string
        df_conso_vs_solar=pd.merge(df_input,df1, how='left', on = 'Building')
        # Keep only the necessary columns in df_conso_vs_solar
        df_conso_vs_solar=df_conso_vs_solar[['Building','Solaire_Totale','Consommation']]
        # Use the melt function to convert the wide format to long format
        df_conso_vs_solar = df_conso_vs_solar.melt(id_vars='Building', var_name='Label', value_name='Value')
        # Use a dictionary to specify the dtypes for each column
        convert_dict = {'Building': str, 'Label': str, 'Value': float}
        df_conso_vs_solar = df_conso_vs_solar.astype(convert_dict)
        # Round the 'Value' column to 2 decimal places
        df_conso_vs_solar['Value'] = df_conso_vs_solar['Value'].round(2)
        # Convert the dataframe to a list of dictionaries
        json_conso_vs_solar = df_conso_vs_solar.to_dict(orient='records')
        return json_conso_vs_solar
    
    jsonClassOnEmission='''[{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 11,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 11,"Max CO2-m2-an": 30,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 30,"Max CO2-m2-an": 50,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 50,"Max CO2-m2-an": 70,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 70,"Max CO2-m2-an": 100,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 100,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 15,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 16,"Max CO2-m2-an": 25,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 26,"Max CO2-m2-an": 35,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 36,"Max CO2-m2-an": 55,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 56,"Max CO2-m2-an": 80,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 81,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 15,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 16,"Max CO2-m2-an": 30,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 31,"Max CO2-m2-an": 60,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 61,"Max CO2-m2-an": 100,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 101,"Max CO2-m2-an": 145,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 145,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "bureau"}]'''
    # Read JSON data into pandas dataframe
    df_ClassOnEmission=pd.read_json(jsonClassOnEmission, orient ='records')
    # Merge two dataframes on a common column
    df_taxonomy=pd.merge(df1,df_ClassOnEmission, how='inner', on = 'Type_Du_Batiment')
    # Calculate new column values based on existing columns
    df_taxonomy['Emission/m2']=df_taxonomy['Emission_Ges:_Scope_1_Plus_2']/df_taxonomy['Floor_Area_(m2)']
    # Filter rows based on a condition
    df_taxonomy=df_taxonomy[(df_taxonomy['Min CO2-m2-an']<=df_taxonomy['Emission/m2']) & (df_taxonomy['Emission/m2']<=df_taxonomy['Max CO2-m2-an'])]
    # Calculate new column values based on existing columns
    df_taxonomy['Self_Consumption']=((df_taxonomy['Solaire_Totale']/df_taxonomy['Consommation'])*100).astype('double')
    # Reset dataframe index
    df_taxonomy.reset_index(inplace=True)
    # Add empty columns to the dataframe
    df_taxonomy['SC_CC_Adaptation']=''
    df_taxonomy['DNSH_Adaptation']=''
    df_taxonomy['Taxonomy']=''
    # Iterate over rows and update column values based on a condition
    for i in range (0,len(df_taxonomy)):
        if df_taxonomy['Self_Consumption'][i]<20:
            df_taxonomy.at[i, 'SC_CC_Adaptation']='SC'
        else:
            df_taxonomy.at[i, 'SC_CC_Adaptation']=''
        if df_taxonomy['Self_Consumption'][i]>10:
            df_taxonomy.at[i, 'DNSH_Adaptation']='DNSH'
        else:
            df_taxonomy.at[i, 'DNSH_Adaptation']=''
    # Iterate over rows and update column values based on multiple conditions
    for i in range (0,len(df_taxonomy)):
        if (df_taxonomy.at[i, 'SC_CC_Mitigation']=="SC" or df_taxonomy.at[i, 'SC_CC_Adaptation']=="SC") and df_taxonomy.at[i, 'DNSH_CC__Mitigation']=='DNSH':
            df_taxonomy.at[i, 'Taxonomy']="Eligible and Aligned"
        else:
            df_taxonomy.at[i, 'Taxonomy']="Eligible and Non Aligned"
    # Convert columns to numeric data type and fill missing values with 0
    df_taxonomy['Opex']=pd.to_numeric(df_taxonomy['Opex'], errors='coerce').fillna(0)
    df_taxonomy['Total_Operating_Expenses']=pd.to_numeric(df_taxonomy['Total_Operating_Expenses'], errors='coerce').fillna(0)
    # Calculate new column values based on existing columns
    df_taxonomy['Opex_Totale']=df_taxonomy['Opex']+df_taxonomy['Total_Operating_Expenses']

    

    def data_global():
        # Group the data by Taxonomy and sum the relevant columns
        Global = df_taxonomy.groupby(['Taxonomy'])['Opex_Totale', 'Absolute_Turnover(M_Euro)', 'Capex'].sum()
        # Calculate the percentages and round to two decimal places
        Global["Pourcentage_Turnover"] = (Global['Absolute_Turnover(M_Euro)'] / Global['Absolute_Turnover(M_Euro)'].sum() * 100).round(2)
        Global["Pourcentage_Capex"] = (Global['Capex'] / Global['Capex'].sum() * 100).round(2)
        Global["Pourcentage_Opex"] = (Global['Opex_Totale'] / Global['Opex_Totale'].sum() * 100).round(2)
        # Reset the index to make Taxonomy a column
        Global.reset_index(inplace=True)
        # Define the data types for each column and round to two decimal places
        convert_dict = {'Taxonomy': str, 'Pourcentage_Turnover': float, 'Pourcentage_Capex': float, 'Pourcentage_Opex': float}
        arround = {'Pourcentage_Turnover': 2, 'Pourcentage_Capex': 2, 'Pourcentage_Opex': 2}
        Global = Global.astype(convert_dict)
        Global = Global.round(arround)
        # Convert the data to a dictionary and return it
        json_Global = Global[['Taxonomy', 'Pourcentage_Turnover', 'Pourcentage_Capex', 'Pourcentage_Opex']].to_dict(orient='records')
        return json_Global

    # Use apply() to iterate through each row of the dataframe and determine the CC adaptation value
    # If the SC_CC_Adaptation column value is 'SC', set the value to 'SC'
    # If the DNSH_Adaptation column value is 'DNSH', set the value to 'DNSH'
    # Otherwise, set the value to 'LowPerformance'
    cc_adaptation = df_taxonomy.apply(lambda row: row['SC_CC_Adaptation'] if row['SC_CC_Adaptation'] == 'SC' else row['DNSH_Adaptation'] if row['DNSH_Adaptation'] == 'DNSH' else 'LowPerformance', axis=1)
    # Do the same for mitigation
    cc_mitigation = df_taxonomy.apply(lambda row: row['SC_CC_Mitigation'] if row['SC_CC_Mitigation'] == 'SC' else row['DNSH_CC__Mitigation'] if row['DNSH_CC__Mitigation'] == 'DNSH' else 'LowPerformance', axis=1)
    # Combine the CC adaptation and mitigation values into a single dataframe
    cc = pd.concat([cc_adaptation, cc_mitigation], axis=1, keys=['CC_Adaptation', 'CC_Mitigation'])    
    # Define a list of possible CC values
    cc_values = ["SC", "DNSH", "LowPerformance"]
    # Add a new 'record' column to the CC dataframe with the current index values
    cc['record'] = cc.index

    # Define a function to calculate the Climate change  Adaptation (CCA) values based on the CC values
    def data_cca():
        # Group the CC dataframe by CC_Adaptation and count the number of records in each group
        cca = cc.groupby(['CC_Adaptation'])['record'].count()
        # Calculate the total count of records
        count_cca = cca.sum()
        # Calculate the percentage of records in each CC_Adaptation group and round to 2 decimal places
        json_cca = ((cca/count_cca)*100).round(2)
        # Convert the json_cca dataframe to a dictionary
        json_cca = json_cca.to_dict()
        # Define a list of possible CC values
        cc_values = ["SC", "DNSH", "LowPerformance"]
        # Add any CC values that are not present in the dictionary and set their value to 0
        for i in cc_values:
            if i not in json_cca:
                json_cca[i] = 0
        # Set the SC_vs_default, DNSH_vs_default, and LowP_vs_default values to 10
        json_cca['SC_vs_default'] = 10
        json_cca['DNSH_vs_default'] = 10
        json_cca['LowP_vs_default'] = 10
        # Create a new dictionary and round all values to 2 decimal places
        res = {}
        for k, v in json_cca.items():
            res[k] = round(float(v), 2)
        return res
    
    def data_ccm():
        # Next, the DataFrame is grouped by the 'CC_Mitigation' column, and a new Series object named 'CCM' is created containing the count of each unique value in the 'CC_Mitigation' column.
        CCM = cc.groupby(['CC_Mitigation'])['record'].count()
        # The sum of all values in the 'CCM' Series is calculated and stored in the variable 'countCCM'.
        countCCM = CCM.sum()
        # A new Series object named 'jsonCCM' is created by dividing each value in the 'CCM' Series by 'countCCM', multiplying the result by 100, rounding the result to 2 decimal places, and converting it to a dictionary.
        jsonCCM = ((CCM/countCCM)*100).round(2).to_dict()
        # Define a list of possible CC values
        cc_values = ["SC", "DNSH", "LowPerformance"]
        # For each value in the list 'cc_values', if it is not present in the 'jsonCCM' dictionary, a new key-value pair is added with a value of 0.
        for i in cc_values:
            if i not in jsonCCM:
                jsonCCM[i] = 0
        # Finally, three new key-value pairs are added to the 'jsonCCM' dictionary, and the dictionary is converted to a new dictionary named 'res' with each value rounded to 2 decimal places. This new dictionary is then returned.
        jsonCCM['SC_vs_default'] = 10
        jsonCCM['DNSH_vs_default'] = 10
        jsonCCM['LowP_vs_default'] = 10
        res = {}
        for k, v in jsonCCM.items():
            jsonCCM[k] = float(v)
            res[k] = round(jsonCCM[k], 2)
        jsonCCM = res
        return jsonCCM


    def data_buildingYear(df_performance):
        # Rename columns and fill missing values
        df_performance.rename(columns={"Energy_Consumption": "Consommation"},
                        inplace=True)
        df_performance['Consommation'] = df_performance['Consommation'].astype('double').fillna(0).round(2)
        df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'] = df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'].astype('float').fillna(0)
        # Replace 0 values in Consommation and Emission with predicted values
        df_performance.loc[df_performance['Consommation'] == 0, 'Consommation'] = df_performance['Energy_Consumption_predicted']
        df_performance.loc[df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'] == 0, 'Emission_Ges:_Scope_1_Plus_2(Kg)'] = df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)_predicted']
        # Merge input data with filtered data and select required columns
        df_building_per_year = pd.merge(df_input, df_performance[['Building', 'Consommation', 'Emission_Ges:_Scope_1_Plus_2(Kg)', 'Year', 'Month']],
                                        how='left', on='Building')
        df_building_per_year = df_building_per_year[['Building', 'Consommation', 'Emission_Ges:_Scope_1_Plus_2(Kg)', 'Year', 'Month']]
        # Sort the data by Building, Year, and Month
        df_building_per_year.sort_values(by=['Building', 'Year', 'Month'], axis=0, ascending=[True, True, True], inplace=True)
        # Convert data types and round the float columns to 2 decimal places
        convert_dict = {"Building": str, "Consommation": float, "Emission_Ges:_Scope_1_Plus_2(Kg)": float, "Year": int, "Month": int}
        arround = {"Consommation": 2, "Emission_Ges:_Scope_1_Plus_2(Kg)": 2}
        df_building_per_year = df_building_per_year.astype(convert_dict)
        df_building_per_year = df_building_per_year.round(arround)

        # Convert data to JSON format
        json_building_per_year = df_building_per_year.to_dict(orient='records')
        return json_building_per_year


    # Suppress all warnings
    warnings.filterwarnings('ignore')

    Output = {'Global': data_global(),
    'CCA': data_cca(),
    'CCM': data_ccm(),
    'Energie': data_energie(),
    'Building': data_building(),
    'Building_Year': data_buildingYear(df_performance),
    'Conso_vs_Solar': conso_vs_solar()}
 
    return jsonify(Output)
  
if __name__=='__main__':
    app.run(debug=True)
