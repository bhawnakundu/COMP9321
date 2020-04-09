import pandas as pd
from flask import Flask, request, jsonify
from flask_restplus import Resource, Api
import sqlite3
import requests
from pandas.io.json import json_normalize
import time
from datetime import datetime
import re
import json
from werkzeug.routing import BaseConverter, IntegerConverter

app = Flask(__name__)
api = Api(app)

#converting the negative path paramters to signed integer
class SignedIntConverter(IntegerConverter):
    regex = r'-?\d+'

app.url_map.converters['signed_int'] = SignedIntConverter

@api.route('/collections')
class question1and3(Resource):
    @api.param('order_by', 'Eg : +id,+creation_time,+indicator,-id,-creation_time,-indicator')
    ###################################################
    #Question 3
    ###################################################
    def get(self):
        order_type = request.args.get('order_by')
        conn = sqlite3.connect('z5130378.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS DATATABLE (countryiso3code text, date date, value decimal, indicator_id text, indicator_value text, country_id text, country_value text, time text, collection_id integer)')
        
        #if no query parameter is passed, just return the whole collection
        if order_type == None:
            query = "select collection_id, time, indicator_id from DATATABLE"
            c.execute(query)
            indicator_found = c.fetchall()
            
            if indicator_found == []:
                return {'message':'No data to display'}, 404
            
            j = 0
            list = []
            while(j < len(indicator_found)):
                res = {
                    "uri" : "/collections/"+ str(indicator_found[j][0]),
                    "id" : indicator_found[j][0],
                    "creation_time": indicator_found[j][1],
                    "indicator_id" : indicator_found[j][2]
                    }
                list.append(res)
                j += 1
            return list, 200
        else:
            query = "select collection_id, time, indicator_id from DATATABLE ORDER BY "
            result = [x.strip() for x in order_type.split(',')]
            
            i = 0
            while(i < len(result)):
                ascen_desc_how = re.sub(r"[^+|-]", "", result[i])
                ascen_desc_val = re.sub(r"[+|-]", "", result[i])
                
                #changing the names as per the names of columns in my databse
                if ascen_desc_val == "creation_time":
                    ascen_desc_val = "time"
                if ascen_desc_val == "id":
                    ascen_desc_val = "collection_id"
                if ascen_desc_val == "indicator":
                    ascen_desc_val = "indicator_id"
                
                #checking if the input have any invalid item
                if result[i] == '+id' or result[i] == '+indicator' or result[i] == '+creation_time' or result[i] == '-id' or result[i] == '-indicator' or result[i] == '-creation_time' or result[i] == 'id' or result[i] == 'creation_time' or result[i] == 'indicator':
                    pass
                else:
                    result[i] = ""
                
                if result[i] == "":
                    return {'message':'Invalid input type'}, 400
                
                if ascen_desc_how == "+" or ascen_desc_how == "":
                        how_to = " ASC"
                else:
                        how_to = " DESC"
                
                query = str(query) + ascen_desc_val + how_to + ","
                
                if i == len(result) - 1:
                    query = str(query) + ascen_desc_val + how_to
                
                i += 1
                
            c.execute(query)
            indicator_found = c.fetchall()
            
            if indicator_found == []:
                return {'message':'No data to display'}, 404
            
            j = 0
            list = []
            while(j < len(indicator_found)):
                res = {
                    "uri" : "/collections/"+ str(indicator_found[j][0]),
                    "id" : indicator_found[j][0],
                    "creation_time": indicator_found[j][1],
                    "indicator_id" : indicator_found[j][2]
                    }
                list.append(res)
                j += 1
            return list, 200
    
    ###################################################
    #Question 1
    ###################################################
    @api.param('indicator_id', 'Eg : NY.GDP.MKTP.CD', methods=['POST'], type=str, required=True)
    def post(self):
            
            indicator_val = request.args.get('indicator_id')
            url = "http://api.worldbank.org/v2/countries/all/indicators/"+indicator_val+"?date=2012:2017&format=json&per_page=1000"
            r = requests.get(url)
            data = r.json()
            
            conn = sqlite3.connect('z5130378.db')
            c = conn.cursor()
            c.execute('CREATE TABLE IF NOT EXISTS DATATABLE (countryiso3code text, date date, value decimal, indicator_id text, indicator_value text, country_id text, country_value text, time text, collection_id integer)')
            
            c.execute('select max(collection_id) from DATATABLE')
            result = c.fetchall()
            
            for i in result:
                col_id = i[0]
            
            if col_id == None:
                new_id = 1
            else:
                new_id = int(col_id) + 1

            #if the indicator id doesnot exist in the data source
            if 'message' in data[0]:
                return {'message':'indicator id doesn\'t exist in the data source'}, 404
            else:
                #creating dataframe with columns indicator_id, country_id, value, creation_time, indicator_value, country_value, collection_id
                df = pd.DataFrame(data[1])
                #if value exist in data source but returns null
                if df.empty:
                    col = ['collection_id', 'countryiso3code', 'date', 'value', 'indicator_id', 'indicator_value', 'country_id', 'country_value', 'time']
                    df = pd.DataFrame(data[1], columns=col)
                    ts = pd.Timestamp(year = 2011,  month = 11, day = 21,
                    hour = 10, second = 49, tz = 'Europe/Berlin')

                    #creating an empty row in dataframe
                    df.loc[0] = [new_id,"","","",indicator_val,"","","",ts.now()]
                    df['time'] = ts.now()
                    df['collection_id'] = new_id
                    df['indicator_id'] = indicator_val

                    id_to_check = indicator_val
                    c.execute("select indicator_id from DATATABLE where indicator_id=?", (id_to_check,))
                    data = c.fetchall()
                    
                    if data == []:
                        conn.commit()
                        df.to_sql('DATATABLE', conn, if_exists='append', index = False)
                        
                        #query to get the creation time from the database table
                        c.execute("select time from DATATABLE where indicator_id=?",(id_to_check,))
                        create_time = c.fetchall()
                        creation_time = create_time[0][0]

                        #query to get the collection id from the database table
                        c.execute("select collection_id from DATATABLE where indicator_id=?",(id_to_check,))
                        create_id = c.fetchall()
                        creation_id = create_id[0][0]

                        res = {
                           "uri" : "/collections/"+ str(creation_id),
                           "id" : creation_id,
                           "creation_time": creation_time,
                           "indicator_id" : indicator_val
                        }
                        return res, 201
                    else:
                        return {'message':'Collection already exists'}, 409
                #if the dataframe is not empty
                else:
                    #changing the dataframe accordingly
                    df.index.name = 'id'
                    df1 = json_normalize(df['indicator'])
                    df1 = df1.rename({'id': 'indicator_id', 'value': 'indicator_value'}, axis=1)
                    df1.index.name = 'id'
                    df2 = json_normalize(df['country'])
                    df2 = df2.rename({'id': 'country_id', 'value': 'country_value'}, axis=1)
                    df2.index.name = 'id'
                    df3 = pd.merge(df1, df2, on='id')
                    df4 = pd.merge(df, df3, on='id')
                    df4 = df4.drop('indicator', 1)
                    df4 = df4.drop('country', 1)
                    df4 = df4.drop('unit', 1)
                    df4 = df4.drop('obs_status', 1)
                    df4 = df4.drop('decimal', 1)
                    df4['value'] = df4['value'].fillna("Na")
                    df5 = df4[df4.value != 'Na']
                    ts = pd.Timestamp(year = 2011,  month = 11, day = 21,
                    hour = 10, second = 49, tz = 'Europe/Berlin')
                    df5['time'] = ts.now()
                    df5['collection_id'] = new_id

                    #query to get the indicator id from the database table
                    id_to_check = indicator_val
                    c.execute("select indicator_id from DATATABLE where indicator_id=?", (id_to_check,))
                    data = c.fetchall()

                    #if the indicator id doesnot exist in the database table i.e. DATATABLE
                    if data == []:
                        conn.commit()
                        #converting dataframe to database table
                        df5.to_sql('DATATABLE', conn, if_exists='append', index = False)

                        #query to get the creation time from the database table
                        c.execute("select time from DATATABLE where indicator_id=?",(id_to_check,))
                        create_time = c.fetchall()
                        creation_time = create_time[0][0]

                        #query to get the collection id from the database table
                        c.execute("select collection_id from DATATABLE where indicator_id=?",(id_to_check,))
                        create_id = c.fetchall()
                        creation_id = create_id[0][0]

                        res = {
                           "uri" : "/collections/"+ str(creation_id),
                           "id" : creation_id,
                           "creation_time": creation_time,
                           "indicator_id" : indicator_val
                        }
                        return res, 201
                    #if the indicator id already exist then return 409 conflict
                    else:
                        return {'message':'Collection already exists'}, 409

@api.route('/collections/<signed_int:id>', methods=["DELETE","GET"])
class question2and4(Resource):
    ###################################################
    #Question 4
    ###################################################
    def get(self, id):
    
        conn = sqlite3.connect('z5130378.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS DATATABLE (countryiso3code text, date date, value decimal, indicator_id text, indicator_value text, country_id text, country_value text, time text, collection_id integer)')
    
        collection = id
        
        if isinstance(id, int) == False or collection < 0:
            res = {
                "message" : "The collection id is invalid, try id >= 1!",
            }
            return res, 400
        
        #query to get the creation time from the database table
        c.execute("select collection_id from DATATABLE where collection_id=?",(collection,))
        create_id = c.fetchall()
        if create_id == []:
            return {'message':'Collection id not found'}, 404
        else:
            #query to get the creation time from the database table
            c.execute("select time from DATATABLE where collection_id=?",(collection,))
            create_time = c.fetchall()
            creation_time = create_time[0][0]
            
            #query to get the indicator_id from the database table
            c.execute("select indicator_id from DATATABLE where collection_id=?",(collection,))
            indicat_val = c.fetchall()
            indicator_val = indicat_val[0][0]
            
            #query to get the indicator_value from the database table
            c.execute("select indicator_value from DATATABLE where collection_id=?",(collection,))
            cator_val = c.fetchall()
            indicator_value = cator_val[0][0]
            
            #query to get the entries from the database table
            c.execute("select country_value, date, value from DATATABLE where collection_id=?",(collection,))
            entry_value = c.fetchall()

            #query to get the country_value from database for the corresponding colection_id
            c.execute("select country_value from DATATABLE where collection_id=?",(collection,))
            entry_value_country = c.fetchall()
            entry = entry_value_country
            
            #query to get the date from database for the corresponding colection_id
            c.execute("select date from DATATABLE where collection_id=?",(collection,))
            entry_value_date = c.fetchall()
            date = entry_value_date
            
            #query to get the value from database for the corresponding colection_id
            c.execute("select value from DATATABLE where collection_id=?",(collection,))
            entry_value_value = c.fetchall()
            value = entry_value_value
            
            #returning a list of JSON array with all the information
            i = 0
            list = []
            while(i < len(entry_value)):
                res_list = {
                            "country" : entry[i][0],
                            "date" : date[i][0],
                            "value" : value[i][0]
            
                        }
                list.append(res_list)
                i += 1
            res = {
                    "id" : collection,
                    "indicator" : indicator_val,
                    "indicator_value" : indicator_value,
                    "creation_time": creation_time,
                    "entries": list
                    }
        return res, 200
        
    ###################################################
    #Question 2
    ###################################################
    def delete(self, id):
        conn = sqlite3.connect('z5130378.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS DATATABLE (countryiso3code text, date date, value decimal, indicator_id text, indicator_value text, country_id text, country_value text, time text, collection_id integer)')
        
        collection = id
        
        if isinstance(collection, int) == False or collection < 0:
            res = {
                    "message" : "The collection id is invalid, try id >= 1!",
                }
            respon = jsonify(res)
            respon.status_code = 400
            return respon
            
        conn = sqlite3.connect('z5130378.db')
        c = conn.cursor()
        id_to_check = collection
        c.execute("select collection_id from DATATABLE where collection_id=?",(id_to_check,))
        collection_to_delete = c.fetchall()

        #if collection_to_delete exist in source, but is already deleted or not in the database
        if collection_to_delete == []:
             res = {
                     "message" : "The collections doesnot exist in the database!",
                     }
             return res, 404
        else:
             collection_id_to_delete = collection_to_delete[0][0]
             id_to_delete = collection_id_to_delete
             c.execute('DELETE FROM DATATABLE WHERE collection_id = ?', (id_to_delete,))
             result = c.fetchall()
             conn.commit()

             #if collection with creation_id deleted successfully
             if result == []:
                 res = {
                     "message" : "The collections "+ str(collection_to_delete[0][0]) + " was removed from the database!",
                     "id" : collection_to_delete[0][0],
                 }
        return res, 200
 
@api.route('/collections/<signed_int:id>/<signed_int:year>/<string:country>')
class question5(Resource):
    ###################################################
    #Question 5
    ###################################################
    def get(self, id, year, country):
        
        conn = sqlite3.connect('z5130378.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS DATATABLE (countryiso3code text, date date, value decimal, indicator_id text, indicator_value text, country_id text, country_value text, time text, collection_id integer)')
    
        if isinstance(id, int) == False or id < 0 or year < 0:
            res = {
                "message" : "The collection id or year is invalid, try id >= 1!",
            }
            return res, 400
            
        #query to get the date from database for the corresponding colection_id
        c.execute("select indicator_id from DATATABLE where collection_id=? AND country_value=? AND date=?", (id,country,year))
        
        indicator_found = c.fetchall()
        if indicator_found == []:
            res = {
                    "message" : "The collection doesnot exist in the database!",
                    }
            return res, 404
        else:
            entry = indicator_found
            c.execute("select value from DATATABLE where collection_id=? AND country_value=? AND date=?", (id,country,year))
            value_found = c.fetchall()
            entry_value = value_found
            
            #returning a list of JSON array with all the information
            i = 0
            while(i < len(entry_value)):
                res = {
                        "id" : id,
                        "indicator" : entry[0][i],
                        "country" : country,
                        "year":  year,
                        "value": entry_value[0][i]
                        }
                i += 1
            return res, 200

@api.route('/collections/<signed_int:id>/<signed_int:year>')
@api.param('q', '+N or -N')
class question6(Resource):
    ###################################################
    #Question 6
    ###################################################
    def get(self, id, year):
        conn = sqlite3.connect('z5130378.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS DATATABLE (countryiso3code text, date date, value decimal, indicator_id text, indicator_value text, country_id text, country_value text, time text, collection_id integer)')
        
        how_many = request.args.get('q')
        
        if isinstance(id, int) == False or id < 0 or year < 0:
            res = {
                "message" : "The collection id or year is invalid, try id >= 1!",
            }
            return res, 400
        
        #if the input for 'q' is not a valid input
        if how_many is None:
            #query to get the indicator_id from database for the corresponding colection_id
            c.execute("select indicator_id from DATATABLE where collection_id=? AND date=?", (id,year))
            indicator_found = c.fetchall()
            
            if indicator_found == []:
                res = {
                    "message" : "The collection doesnot exist in the database!",
                    }
                return res, 404
            else:
                entry = indicator_found[0][0]
                c.execute("select indicator_value from DATATABLE where collection_id=? AND date=?", (id,year))
                value_found = c.fetchall()
                entry_value = value_found[0][0]
                    
                #query to get the date from database for the corresponding colection_id
                c.execute("select country_value from DATATABLE where collection_id=? AND date=?",(id,year))
                entry_value_date = c.fetchall()
                entry_con = entry_value_date
                    
                #query to get the value from database for the corresponding colection_id
                c.execute("select value from DATATABLE where collection_id=? AND date=?",(id,year))
                entry_value_value = c.fetchall()
                value = entry_value_value
                
                #returning a list of JSON array with all the information
                i = 0
                list = []
                while(i < len(entry_value)):
                    res_list = {
                                "country" : entry_con[i][0],
                                "value" : value[i][0]
                                }
                    list.append(res_list)
                    i += 1
                res = {
                        "indicator" : entry,
                        "indicator_value" : entry_value,
                        "entries": list
                    }
                return res, 200
        else:
                #+N/-N
                pattern = re.compile("(^[\+|\-]([0-9]+)$)")
                pattern_without_sign = re.compile("(^([0-9]+)$)")

                if pattern.match(how_many) or pattern_without_sign.match(how_many):
                    ascen_desc_num = re.sub(r"[\+|\-]", "", how_many)
                    ascen_desc_how = re.sub(r"\d", "", how_many)
                    #putting the range restriction
                    if (int(ascen_desc_num) > 100) or (int(ascen_desc_num) < 0):
                        return {'message':'0 <= N <= 100'}, 400
                    
                    if (ascen_desc_how == "+" or ascen_desc_how == ""):
                        c.execute("select indicator_id from DATATABLE where collection_id=? AND date=?", (id,year))
                        indicator_found = c.fetchall()
                        
                        if indicator_found == []:
                            res = {
                                "message" : "The collection doesnot exist in the database!",
                                }
                            respon = jsonify(res)
                            respon.status_code = 404
                            return respon
                        else:
                            entry = indicator_found[0][0]
                            c.execute("select indicator_value from DATATABLE where collection_id=? AND date=?", (id,year))
                            value_found = c.fetchall()
                            entry_value = value_found[0][0]
                            
                            c.execute("select country_value, value from DATATABLE where collection_id=? AND date=? ORDER BY value DESC limit ?",(id,year,ascen_desc_num))
                            entry_value_date = c.fetchall()
                            
                            i = 0
                            list = []
                            while(i < len(entry_value_date)):
                                res_list = {
                                            "country" : entry_value_date[i][0],
                                            "value" : entry_value_date[i][1]
                                        }
                                list.append(res_list)
                                i += 1
                            res = {
                                    "indicator" : entry,
                                    "indicator_value" : entry_value,
                                    "entries": list
                                    }
                            return res, 200
                    else:
                        how_many = request.args.get('q')
                        ascen_desc_num = re.sub(r"[\+|\-]", "", how_many)
                        ascen_desc_how = re.sub(r"\d", "", how_many)
                    
                        #putting the range restriction
                        if (int(ascen_desc_num) > 100) or (int(ascen_desc_num) < 0):
                            return {'message':'0 <= N <= 100'}, 400
                    
                        c.execute("select indicator_id from DATATABLE where collection_id=? AND date=?", (id,year))
                        indicator_found = c.fetchall()
                        if indicator_found == []:
                            res = {
                                "message" : "The collection doesnot exist in the database!",
                                }
                            return res, 404
                        else:
                            entry = indicator_found[0][0]
                            c.execute("select indicator_value from DATATABLE where collection_id=? AND date=?", (id,year))
                            value_found = c.fetchall()
                            entry_value = value_found[0][0]
                            
                            c.execute("SELECT country_value, value FROM DATATABLE where collection_id = ? and date = ? ORDER BY value ASC limit ?",(id,year,ascen_desc_num))
                            entry_value_date = c.fetchall()
                            #getting the bottom values
                            entry_value_date.reverse()
                            
                            i = 0
                            list = []
                            while(i < len(entry_value_date)):
                                res_list = {
                                        "country" : entry_value_date[i][0],
                                        "value" : entry_value_date[i][1]
                                        }
                                list.append(res_list)
                                i += 1
                            res = {
                                    "indicator" : entry,
                                    "indicator_value" : entry_value,
                                    "entries": list
                                    }
                            return res, 200
                else:
                    return {'message':'Invalid query type, try +N or -N where 0 <= N <= 100'}, 400

if __name__ == '__main__':
    app.run(host='127.0.0.1',port=8000,debug=True)

