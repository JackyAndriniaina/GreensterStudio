from flask import Flask,jsonify,request
import json
import pandas as pd
import numpy as np
from pandas import json_normalize
app =   Flask(__name__)

@app.route('/', methods=['GET'])
def hello_world():
    return 'Hello, World!'

@app.route('/test', methods=['POST'])
def upload_json():
    """Input=request.json
    df_input =json_normalize(Input['Input'])
    df_EUTaxonomie = json_normalize(Input['EU_Taxonomy_version1'])
    df_performance=json_normalize(Input['Draft_Performance_Tracking_Global+Predicted'])
    df1=pd.merge(df_input,df_EUTaxonomie, how='right', on = 'Building')
    df1_input=pd.merge(df_input,df_EUTaxonomie, how='right', on = 'Building')
    df2=pd.merge(df_input,df_performance, how='right', on = 'Building')
    
    try:    
        df1['Solaire']=df1['Solaire'].astype('double').fillna(0)
        df1['Solaire_(panneaux_Pv)']=df1['Solaire_(panneaux_Pv)'].astype('double').fillna(0)
        df1['Solaire_Totale']=df1['Solaire']+df1['Solaire_(panneaux_Pv)']
        df1_input['Solaire']=df1_input['Solaire'].astype('double').fillna(0)
        df1_input['Solaire_(panneaux_Pv)']=df1_input['Solaire_(panneaux_Pv)'].astype('double').fillna(0)
        df1_input['Solaire_Totale']=df1_input['Solaire']+df1_input['Solaire_(panneaux_Pv)']
    
        def dataEnergie():
            df1_input['Quotation']=df1_input['Quotation'].astype('double').fillna(0)
            for i in range (0,len(df1_input)):
                if df1_input['Quotation'][i]!=0:
                    df1_input['Quotation'][i]=df1_input['Quotation'][i]
                else:
                    df1_input['Quotation'][i]=0.131   
            df1_input['Saving_Self_Conso']=df1_input['Solaire_Totale']*df1_input['Quotation']
            df1_input['Saving_Self_Conso_Input']=df1_input['Solaire']*df1_input['Quotation']
            df1_input['Solaire']=df1_input['Solaire'].astype('float').fillna(0)
            jsonEnergie={}
            jsonEnergie['Volume_Of_Generated_KWh']=df1_input['Solaire_Totale'].sum().round(2)
            jsonEnergie['Volume_Of_Generated_vs_default']=((df1_input['Solaire'].sum()*100)/df1_input['Solaire_(panneaux_Pv)'].sum()).round(2)
            jsonEnergie['Self-Consumption_Rate']=((df1_input['Solaire_Totale'].sum()/df1_input['Consommation'].sum())*100).round(2)
            jsonEnergie['Self-Conso_vs_default']=((df1_input['Solaire'].sum()/df1_input['Consommation'].sum())*100).round(2)
            jsonEnergie['Saving_Self_Conso']=df1_input['Saving_Self_Conso'].sum().round(2)
            jsonEnergie['Saving_Self_Conso_vs_default']=df1_input['Saving_Self_Conso_Input'].sum().round(2)
            jsonEnergie['Percentage_Saving_Self_Conso_vs_default']=(jsonEnergie['Saving_Self_Conso_vs_default']*100/jsonEnergie['Saving_Self_Conso']).round(2)
            return jsonEnergie

        def data_building():
            df_building=df1
            df_building.sort_values(by='Building', ascending=True,inplace=True)
            df_building['Dep_Kwh_m2_an']=df_building['Dep_Kwh_m2_an'].astype('double').fillna(0).round(2)
            df_building['Solaire_Totale']=df_building['Solaire_Totale'].astype('double').fillna(0).round(2)
            df_building=pd.merge(df_input,df_building, how='left', on = 'Building')
            df_building=df_building[["Building","Dep_Kwh_m2_an","Consommation","Solaire_Totale"]]
            convert_dict={"Building":str,"Dep_Kwh_m2_an":float,"Consommation":float,"Solaire_Totale":float}
            arround={"Building":2,"Dep_Kwh_m2_an":2,"Consommation":2,"Solaire_Totale":2}
            df_building=df_building.astype(convert_dict)
            df_building=df_building.round(arround)
            json_Building=df_building.to_dict(orient='records')
            return json_Building

        def conso_vs_solar():
            df_conso_vs_solar=df1
            df_conso_vs_solar=pd.merge(df_input,df_conso_vs_solar, how='left', on = 'Building')
            df_conso_vs_solar=df_conso_vs_solar[['Building','Solaire_Totale','Consommation']]
            df_conso_vs_solar=df_conso_vs_solar.melt(id_vars='Building', var_name="Label", value_name="Value")
            convert_dict={"Building":str,"Label":str,"Value":float}
            arround={"Value":2}
            df_conso_vs_solar=df_conso_vs_solar.astype(convert_dict)
            df_conso_vs_solar=df_conso_vs_solar.round(arround)
            json_conso_vs_solar=df_conso_vs_solar.to_dict(orient='records')
            return json_conso_vs_solar

        jsonClassOnEmission='''[{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 11,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 11,"Max CO2-m2-an": 30,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 30,"Max CO2-m2-an": 50,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 50,"Max CO2-m2-an": 70,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 70,"Max CO2-m2-an": 100,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 100,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "habitation"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 15,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 16,"Max CO2-m2-an": 25,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 26,"Max CO2-m2-an": 35,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 36,"Max CO2-m2-an": 55,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 56,"Max CO2-m2-an": 80,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 81,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "centre commercial"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": "SC","Class ": "A","Min CO2-m2-an": 0,"Max CO2-m2-an": 6,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "B","Min CO2-m2-an": 6,"Max CO2-m2-an": 15,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": "DNSH","SC_CC_Mitigation": 0,"Class ": "C","Min CO2-m2-an": 16,"Max CO2-m2-an": 30,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "D","Min CO2-m2-an": 31,"Max CO2-m2-an": 60,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "E","Min CO2-m2-an": 61,"Max CO2-m2-an": 100,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "F","Min CO2-m2-an": 101,"Max CO2-m2-an": 145,"Type_Du_Batiment": "bureau"},{"DNSH_CC__Mitigation": 0,"SC_CC_Mitigation": 0,"Class ": "G","Min CO2-m2-an": 145,"Max CO2-m2-an": 500000,"Type_Du_Batiment": "bureau"}]'''
        df_ClassOnEmission=pd.read_json(jsonClassOnEmission, orient ='records')
        df_taxonomy=pd.merge(df1,df_ClassOnEmission, how='inner', on = 'Type_Du_Batiment')
        df_taxonomy['Emission/m2']=df_taxonomy['Emission_Ges:_Scope_1_Plus_2']/df_taxonomy['Floor_Area_(m2)']
        df_taxonomy=df_taxonomy[(df_taxonomy['Min CO2-m2-an']<=df_taxonomy['Emission/m2']) & (df_taxonomy['Emission/m2']<=df_taxonomy['Max CO2-m2-an'])]
        df_taxonomy['Self_Consumption']=((df_taxonomy['Solaire_Totale']/df_taxonomy['Consommation'])*100).astype('double')
        df_taxonomy.reset_index(inplace=True)
        df_taxonomy['SC_CC_Adaptation']=''
        df_taxonomy['DNSH_Adaptation']=''
        for i in range (0,len(df_taxonomy)):
            if df_taxonomy['Self_Consumption'][i]<20:
                df_taxonomy['SC_CC_Adaptation'][i]='SC'
            else:
                df_taxonomy['SC_CC_Adaptation'][i]=''
            if df_taxonomy['Self_Consumption'][i]>10:
                df_taxonomy['DNSH_Adaptation'][i]='DNSH'
            else:
                df_taxonomy['DNSH_Adaptation'][i]=''
        df_taxonomy['Taxonomy']=''
        for i in range (0,len(df_taxonomy)):
            if (df_taxonomy['SC_CC_Mitigation'][i]=="SC" or df_taxonomy['SC_CC_Adaptation'][i]=="SC")and df_taxonomy['DNSH_CC__Mitigation'][i]=='DNSH' :
                df_taxonomy['Taxonomy'][i]="Eligible and Aligned"
            else:
                df_taxonomy['Taxonomy'][i]="Eligible and Non Aligned"
        df_taxonomy['Opex']=df_taxonomy['Opex'].astype('double').fillna(0)
        df_taxonomy['Total_Operating_Expenses']=df_taxonomy['Total_Operating_Expenses'].astype('double').fillna(0)
        df_taxonomy['Opex_Totale']=df_taxonomy['Opex']+df_taxonomy['Total_Operating_Expenses']
        
        

        def data_global():
            Global=df_taxonomy.groupby(['Taxonomy'])['Opex_Totale','Absolute_Turnover(M_Euro)','Capex'].sum()
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
        df_taxonomy['CC_Adaptation']=''
        df_taxonomy['CC_Mitigation']=''
        for i in range (0, len(df_taxonomy)):
            if df_taxonomy['SC_CC_Adaptation'][i]=='SC':
                df_taxonomy['CC_Adaptation'][i]='SC'
            elif df_taxonomy['DNSH_Adaptation'][i]=='DNSH':
                df_taxonomy['CC_Adaptation'][i]='DNSH'
            else:
                df_taxonomy['CC_Adaptation'][i]='LowPerformance'
            if df_taxonomy['SC_CC_Mitigation'][i]=='SC':
                df_taxonomy['CC_Mitigation'][i]='SC'
            elif df_taxonomy['DNSH_CC__Mitigation'][i]=='DNSH':
                df_taxonomy['CC_Mitigation'][i]='DNSH'
            else:
                df_taxonomy['CC_Mitigation'][i]='LowPerformance'
        CC=df_taxonomy[['CC_Adaptation','CC_Mitigation']]
        CC['record']=''
        for i in range (0,len(CC)):
            CC['record'][i]=i
        
        
        ccValues=["SC","DNSH","LowPerformance"]
        def data_cca():
            CCA=CC.groupby(['CC_Adaptation'])['record'].count()
            countCCA=CCA.sum()
            jsonCCA=((CCA/countCCA)*100).round(2)
            jsonCCA=jsonCCA.to_dict()
            for i in ccValues:
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
        def data_ccm():
            CCM=CC.groupby(['CC_Mitigation'])['record'].count()
            countCCM=CCM.sum()
            jsonCCM=((CCM/countCCM)*100).round(2)
            jsonCCM=jsonCCM.to_dict()
            for i in ccValues:
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


        df_performance=df_performance[df_performance['Year']>2020]
        def data_buildingYear(df_performance):
            df_performance=df_performance[df_performance['Year']>2020]
            df_performance.rename(columns={"Energy_Consumption":"Consommation"},inplace=True)
            df_performance['Consommation']=df_performance['Consommation'].astype('double').round(2)
            df_performance['Consommation']=df_performance['Consommation'].fillna(0)
            df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)']=df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'].astype('float').fillna(0)
            df_performance.reset_index(inplace=True)
            for i in range(0,len(df_performance)):
                    if df_performance['Consommation'][i]== 0:
                            df_performance['Consommation'][i]=df_performance['Energy_Consumption_predicted'][i]
                    else:
                            df_performance['Consommation'][i]=df_performance['Consommation'][i]
            for i in range(0,len(df_performance)):
                    if df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'][i]== 0:
                            df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'][i]=df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)_predicted'][i]
                    else:
                            df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'][i]=df_performance['Emission_Ges:_Scope_1_Plus_2(Kg)'][i]
            
            df_buildingPearYear=df_performance
            df_buildingPearYear=pd.merge(df_input,df_buildingPearYear, how='left', on = 'Building')
            df_buildingPearYear=df_buildingPearYear[["Building","Consommation","Emission_Ges:_Scope_1_Plus_2(Kg)","Year","Month"]]
            df_buildingPearYear.sort_values(by = ['Building','Year', 'Month'], axis=0, ascending=[True, True,True], inplace=True)
            convert_dict={"Building":str,"Consommation":float,"Emission_Ges:_Scope_1_Plus_2(Kg)":float,"Year":int,"Month":int}
            arround={"Consommation":2,"Emission_Ges:_Scope_1_Plus_2(Kg)":2}
            df_buildingPearYear=df_buildingPearYear.astype(convert_dict)
            df_buildingPearYear=df_buildingPearYear.round(arround)
            jsonBuildingPerYear=df_buildingPearYear.to_dict(orient='records')
            return jsonBuildingPerYear
        Output={'Global':data_global(),'CCA':data_cca(),'CCM':data_ccm(),'Energie':dataEnergie(),'Building':data_building(),'Building_Year':data_buildingYear(df_performance),'Conso_vs_Solar':conso_vs_solar()}
    


    except:
        listInput=["Building", "Solaire","Quotation","Opex","Target_Opex","Aligned_Opex","Current_Opex","UserID"]
        missingValue=[]
        for i in listInput:
            if i not in Input['Input'][0]:
                print(i)
                missingValue.append(i)
        Output=('input :', missingValue, 'is missing')

    return jsonify(Output) """
    return "haha"
  
if __name__=='__main__':
    app.run(debug=True)
