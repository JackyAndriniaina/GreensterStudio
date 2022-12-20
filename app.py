from flask import Flask,jsonify,request
import json
import pandas as pd
import numpy as np
from pandas import json_normalize
app =   Flask(__name__)

@app.route('/up_json', methods=['POST'])
def upload_json():
    Input=request.json
    df_Input =json_normalize(Input['Input'])
    df_EUTaxonomie = json_normalize(Input['EU_Taxonomy_version1'])
    df_Performance=json_normalize(Input['Draft_Performance_Tracking_Global+Predicted'])
    df_SavingQuarterly= json_normalize(Input['SavingQuarterly'])
    df1=pd.merge(df_Input,df_EUTaxonomie, how='right', on = 'Building')
    df1_Input=pd.merge(df_Input,df_EUTaxonomie, how='right', on = 'Building')
    df2=pd.merge(df_Input,df_Performance, how='right', on = 'Building')
    df1['Solaire']=df1['Solaire'].astype('double').fillna(0)
    df1['Solaire_(panneaux_Pv)']=df1['Solaire_(panneaux_Pv)'].astype('double').fillna(0)
    df1['Solaire_Totale']=df1['Solaire']+df1['Solaire_(panneaux_Pv)']
    df1_Input['Solaire']=df1_Input['Solaire'].astype('double').fillna(0)
    df1_Input['Solaire_(panneaux_Pv)']=df1_Input['Solaire_(panneaux_Pv)'].astype('double').fillna(0)
    df1_Input['Solaire_Totale']=df1_Input['Solaire']+df1_Input['Solaire_(panneaux_Pv)']
 
    def data_Energie():
        df1_Input['Quotation']=df1_Input['Quotation'].astype('double').fillna(0)
        for i in range (0,len(df1_Input)):
            if df1_Input['Quotation'][i]!=0:
                df1_Input['Quotation'][i]=df1_Input['Quotation'][i]
            else:
                df1_Input['Quotation'][i]=0.131   
        df1_Input['Saving_Self_Conso']=df1_Input['Solaire_Totale']*df1_Input['Quotation']
        json_Energie={}
        json_Energie['Volume_Of_Generated_KWh']=df1_Input['Solaire_Totale'].sum().round(2)
        json_Energie['Volume_Of_Generated_vs_default']=((df1_Input['Solaire'].sum()*100)/df1_Input['Solaire_(panneaux_Pv)'].sum()).round(2)
        json_Energie['Self-Consumption_Rate']=((df1_Input['Solaire_Totale'].sum()/df1_Input['Consommation'].sum())*100).round(2)
        json_Energie['Self-Conso_vs_default']=((df1_Input['Solaire'].sum()/df1_Input['Consommation'].sum())*100).round(2)
        json_Energie['Saving_Self_Conso']=df1_Input['Saving_Self_Conso'].sum().round(2)
        return json_Energie

    def data_Building():
        df_Building=df1
        df_Building.sort_values(by='Building', ascending=True,inplace=True)
        df_Building['Dep_Kwh_m2_an']=df_Building['Dep_Kwh_m2_an'].astype('double').fillna(0).round(2)
        df_Building['Solaire_Totale']=df_Building['Solaire_Totale'].astype('double').fillna(0).round(2)
        df_Building=pd.merge(df_Input,df_Building, how='left', on = 'Building')
        df_Building=df_Building[["Building","Dep_Kwh_m2_an","Consommation","Solaire_Totale"]]
        convert_dict={"Building":str,"Dep_Kwh_m2_an":float,"Consommation":float,"Solaire_Totale":float}
        arround={"Building":2,"Dep_Kwh_m2_an":2,"Consommation":2,"Solaire_Totale":2}
        df_Building=df_Building.astype(convert_dict)
        df_Building=df_Building.round(arround)
        json_Building=df_Building.to_dict(orient='records')
        return json_Building

    def Conso_VS_Solar():
        df_Conso_vs_Solar=df1
        df_Conso_vs_Solar=pd.merge(df_Input,df_Conso_vs_Solar, how='left', on = 'Building')
        df_Conso_vs_Solar=df_Conso_vs_Solar[['Building','Solaire_Totale','Consommation']]
        df_Conso_vs_Solar=df_Conso_vs_Solar.melt(id_vars='Building', var_name="Label", value_name="Value")
        convert_dict={"Building":str,"Label":str,"Value":float}
        arround={"Value":2}
        df_Conso_vs_Solar=df_Conso_vs_Solar.astype(convert_dict)
        df_Conso_vs_Solar=df_Conso_vs_Solar.round(arround)
        json_Conso_vs_Solar=df_Conso_vs_Solar.to_dict(orient='records')
        return json_Conso_vs_Solar

    jsonClassOnEmission='''[{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 11,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 11,"Max CO2-m2-an": 30,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 30,"Max CO2-m2-an": 50,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 50,"Max CO2-m2-an": 70,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 70,"Max CO2-m2-an": 100,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 100,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 15,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 16,"Max CO2-m2-an": 25,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 26,"Max CO2-m2-an": 35,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 36,"Max CO2-m2-an": 55,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 56,"Max CO2-m2-an": 80,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 81,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 15,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 16,"Max CO2-m2-an": 30,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 31,"Max CO2-m2-an": 60,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 61,"Max CO2-m2-an": 100,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 101,"Max CO2-m2-an": 145,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 145,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "bureau"}]'''
    df_ClassOnEmission=pd.read_json(jsonClassOnEmission, orient ='records')
    df_Taxonomy=pd.merge(df1,df_ClassOnEmission, how='inner', on = 'Type_Du_Batiment')
    df_Taxonomy['Emission/m2']=df_Taxonomy['Emission_Ges:_Scope_1_Plus_2']/df_Taxonomy['Floor_Area_(m2)']
    df_Taxonomy=df_Taxonomy[(df_Taxonomy['Min CO2-m2-an']<=df_Taxonomy['Emission/m2']) & (df_Taxonomy['Emission/m2']<=df_Taxonomy['Max CO2-m2-an'])]
    df_Taxonomy['Self_Consumption']=((df_Taxonomy['Solaire_Totale']/df_Taxonomy['Consommation'])*100).astype('double')
    df_Taxonomy.reset_index(inplace=True)
    df_Taxonomy['SC_CC_Adaptation']=''
    df_Taxonomy['DNSH_Adaptation']=''
    for i in range (0,len(df_Taxonomy)):
        if df_Taxonomy['Self_Consumption'][i]<20:
            df_Taxonomy['SC_CC_Adaptation'][i]='SC'
        else:
            df_Taxonomy['SC_CC_Adaptation'][i]=''
        if df_Taxonomy['Self_Consumption'][i]>10:
            df_Taxonomy['DNSH_Adaptation'][i]='DNSH'
        else:
            df_Taxonomy['DNSH_Adaptation'][i]=''
    df_Taxonomy['Taxonomy']=''
    for i in range (0,len(df_Taxonomy)):
        if (df_Taxonomy['SC_CC_Mitigation'][i]=="SC" or df_Taxonomy['SC_CC_Adaptation'][i]=="SC")and df_Taxonomy['DNSH_CC__Mitigation'][i]=='DNSH' :
            df_Taxonomy['Taxonomy'][i]="Eligible and Aligned"
        else:
            df_Taxonomy['Taxonomy'][i]="Eligible and Non Aligned"
    df_Taxonomy['Opex']=df_Taxonomy['Opex'].astype('double').fillna(0)
    df_Taxonomy['Total_Operating_Expenses']=df_Taxonomy['Total_Operating_Expenses'].astype('double').fillna(0)
    df_Taxonomy['Opex_Totale']=df_Taxonomy['Opex']+df_Taxonomy['Total_Operating_Expenses']
    
    

    def data_Global():
        Global=df_Taxonomy.groupby(['Taxonomy'])['Opex_Totale','Absolute_Turnover(M_Euro)','Capex'].sum()
        Global["Pourcentage_Turnover"]=(Global['Absolute_Turnover(M_Euro)']/Global['Absolute_Turnover(M_Euro)'].sum()*100).round(2)
        Global["Pourcentage_Capex"]=(Global['Capex']/Global['Capex'].sum()*100).round(2)
        Global["Pourcentage_Opex"]=(Global['Opex_Totale']/Global['Opex_Totale'].sum()*100).round(2)
        Global.reset_index(inplace=True)
        convert_dict={'Taxonomy':str,'Pourcentage_Turnover':float,'Pourcentage_Capex':float,'Pourcentage_Opex':float}
        arround={'Pourcentage_Turnover':2,'Pourcentage_Capex':2,'Pourcentage_Opex':2}
        Global=Global.astype(convert_dict)
        Global=Global.round(arround)
        json_Global=Global[['Taxonomy','Pourcentage_Turnover','Pourcentage_Capex','Pourcentage_Opex']].to_dict(orient='records')
        return json_Global


    CC={}
    df_Taxonomy['CC_Adaptation']=''
    df_Taxonomy['CC_Mitigation']=''
    for i in range (0, len(df_Taxonomy)):
        if df_Taxonomy['SC_CC_Adaptation'][i]=='SC':
            df_Taxonomy['CC_Adaptation'][i]='SC'
        elif df_Taxonomy['DNSH_Adaptation'][i]=='DNSH':
            df_Taxonomy['CC_Adaptation'][i]='DNSH'
        else:
            df_Taxonomy['CC_Adaptation'][i]='LowPerformance'
        if df_Taxonomy['SC_CC_Mitigation'][i]=='SC':
            df_Taxonomy['CC_Mitigation'][i]='SC'
        elif df_Taxonomy['DNSH_CC__Mitigation'][i]=='DNSH':
            df_Taxonomy['CC_Mitigation'][i]='DNSH'
        else:
            df_Taxonomy['CC_Mitigation'][i]='LowPerformance'
    CC=df_Taxonomy[['CC_Adaptation','CC_Mitigation']]
    CC['record']=''
    for i in range (0,len(CC)):
        CC['record'][i]=i
    
    
    CC_Values=["SC","DNSH","LowPerformance"]
    def data_CCA():
        CCA=CC.groupby(['CC_Adaptation'])['record'].count()
        countCCA=CCA.sum()
        jsonCCA=((CCA/countCCA)*100).round(2)
        jsonCCA=jsonCCA.to_dict()
        for i in CC_Values:
            if i not in jsonCCA:
                jsonCCA[i]=0
        jsonCCA['SC_vs_default']=10
        jsonCCA['DNSH_vs_default']=10
        jsonCCA['LowP_vs_default']=10
        res={}
        for k, v in jsonCCA.items():
            jsonCCA[k] = float(v)
            res[k] = round(jsonCCA[k], 2)
        jsonCCA=res
        return jsonCCA
    def data_CCM():
        CCM=CC.groupby(['CC_Mitigation'])['record'].count()
        countCCM=CCM.sum()
        jsonCCM=((CCM/countCCM)*100).round(2)
        jsonCCM=jsonCCM.to_dict()
        for i in CC_Values:
            if i not in jsonCCM:
                jsonCCM[i]=0
        jsonCCM['SC_vs_default']=10
        jsonCCM['DNSH_vs_default']=10
        jsonCCM['LowP_vs_default']=10
        res={}
        for k, v in jsonCCM.items():
            jsonCCM[k] = float(v)
            res[k] = round(jsonCCM[k], 2)
        jsonCCM=res
        return jsonCCM


    df_Performance=df_Performance[df_Performance['Year']>2020]
    def data_BuildingYear(df_Performance):
            df_Performance=df_Performance[df_Performance['Year']>2020]
            df_Performance.rename(columns={"Energy_Consumption":"Consommation"},inplace=True)
            df_Performance['Consommation']=df_Performance['Consommation'].astype('double').round(2)
            df_Performance['Consommation']=df_Performance['Consommation'].fillna(0)
            df_Performance.reset_index(inplace=True)
            for i in range(0,len(df_Performance)):
                    if df_Performance['Consommation'][i]== 0:
                            df_Performance['Consommation'][i]=df_Performance['Energy_Consumption_predicted'][i]
                    else:
                            df_Performance['Consommation'][i]=df_Performance['Consommation'][i]
            df_BuildingPearYear=df_Performance
            df_BuildingPearYear=pd.merge(df_Input,df_BuildingPearYear, how='left', on = 'Building')
            df_BuildingPearYear=df_BuildingPearYear[["Building","Consommation","Emission_Ges:_Scope_1_Plus_2(Kg)","Year","Month"]]
            df_BuildingPearYear.sort_values(by = ['Building','Year', 'Month'], axis=0, ascending=[True, True,True], inplace=True)
            convert_dict={"Building":str,"Consommation":float,"Emission_Ges:_Scope_1_Plus_2(Kg)":float,"Year":int,"Month":int}
            arround={"Consommation":2,"Emission_Ges:_Scope_1_Plus_2(Kg)":2}
            df_BuildingPearYear=df_BuildingPearYear.astype(convert_dict)
            df_BuildingPearYear=df_BuildingPearYear.round(arround)
            jsonBuildingPerYear=df_BuildingPearYear.to_dict(orient='records')
            return jsonBuildingPerYear

    
    def data_SavingQuarterly():
        df_SavingQuarterly= json_normalize(Input['SavingQuarterly'])
        df_SavingQuarterly=df_SavingQuarterly.groupby(['Year','Quarter'])['Eco_Total_','Montant_Total_'].sum()
        df_SavingQuarterly.reset_index(inplace=True)
        df_SavingQuarterly['Saving_SelfConsumption']=(df_SavingQuarterly['Eco_Total_']/df_SavingQuarterly['Montant_Total_'])*100
        df_SavingQuarterly['Saving_SelfConsumption']=df_SavingQuarterly['Saving_SelfConsumption'].round(2)
        df_SavingQuarterly=df_SavingQuarterly[['Year','Quarter','Saving_SelfConsumption']]
        convert_dict={'Year':int,'Quarter':str,'Saving_SelfConsumption':float}
        arround={'Saving_SelfConsumption':2}
        df_SavingQuarterly=df_SavingQuarterly.astype(convert_dict)
        df_SavingQuarterly=df_SavingQuarterly.round(arround)
        jsonSavingQuarterly=df_SavingQuarterly.to_dict(orient='records')
        return jsonSavingQuarterly
        
                
    Output={'Global':data_Global(),'CCA':data_CCA(),'CCM':data_CCM(),'Energie':data_Energie(),'Building':data_Building(),'Building_Year':data_BuildingYear(df_Performance),'SavingQuarterly':data_SavingQuarterly(),'Conso_vs_Solar':Conso_VS_Solar()}
    return jsonify(Output)
  
if __name__=='__main__':
    app.run(debug=True)
