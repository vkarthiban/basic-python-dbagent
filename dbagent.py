import psycopg2

class dbagent():

    '''
    Module Name       : DBAgent
    Description       : Class definition for database management. This module acts as a wrapper for the PsycoPG 2 library. 
                        There will be basic functions for 
    '''    

    def __init__(self):
        self.con = psycopg2.connect(database="django",user="django",password="pass123",host="localhost",port=5432)
        self.csr = self.con.cursor()

######################################################################        
    '''
    Function Name : *CreateTable*
    Arguments     : **tableName** Type: string, *Description:* name of the table to create the table
                    **Columns** *Type:* Dictonary, *Description:* Denotes the columns to create the table structure
    Returns       : True if successful, False otherwise
    '''        

    def CreateTable(self,TableName,Columns):
        query ="create table "+TableName+" ("
        lastkey = list(Columns.keys())
        for variable in Columns.keys():
            if variable == lastkey[-1]:
                query += variable +" "+Columns[variable]
            else:
                query +=variable +" "+Columns[variable]+", "
        query += ")"
        self.csr.execute(query)
        print("..............................................",query)
        self.con.commit()

#######################################################################################
    '''
    Function Name : *FetchData*
    Arguments     : **tableName** Type: string, *Description:* name of the table to fetch the  data from the table
                    **NeedLs** *Type:* List, *Description:* Denotes the columns in a table to define wanted column details
                    **condition** *Type:* Dictonary, *Description:* Denotes the condition is applyed the condition for fetching data
    Returns       : True if successful, False otherwise
    '''        

    def FetchData(self,NeedLs,TableName,condition):
        query = "select "
        for ls in NeedLs:
            if ls == NeedLs[-1]:
                query += ls
            else:
                query += ls + " , "
        query += " from "+TableName 
        lastkey = list(condition.keys())
        print(".....................keys",key)
        if condition:
            query += " where "
            for con in condition.keys():
                print(".....con",con)
                if con == lastkey[-1]:
                    query += str(con) + " = '"+ str(condition[con])+"'"
                else:
                    query += str(con) + " = '" + str(condition[con]) +"' and "

        self.csr.execute(query)
        print("..............................................",query)
        obj = self.csr.fetchall()
        print(".............................",obj)

####################################################################################################################
    '''
    Function Name : *insertData*
    Arguments     : **tableName** Type: string, *Description:* name of the table to insert data to
                    **fields** *Type:* List, *Description:* Denotes the columns in a table
                    **values** *Type:* List, *Description:* Denotes the values to be inserted into the table
    Returns       : True if successful, False otherwise
    '''        

    def insert(self,namels,valuels,TableName):
        query = "insert into "+ TableName +"("
        for name in namels:
            if name == namels[-1]:
                query += name
            else:
                query += name +","

        query += " ) values ("
        for value in valuels:
            if value == valuels[-1]:
                query += "'"+value +"'"
            else:
                query += "'"+value + "',"
        query += " )"
        self.csr.execute(query)
        self.con.commit()
##############################################################
    '''
    Function Name : *update*
    Arguments     : **tableName** Type: string, *Description:* name of the table to update particular table
                    **changels** *Type:* Dictonary, *Description:* Denotes the columns in a table to define what changes to be applyed
                    **condition** *Type:* Dictonary, *Description:* Denotes the condition is applyed the condition for update for particular 
    Returns       : True if successful, False otherwise
    '''        
        

    def update(self,TableName,changls,condition):

        query = "update "+ TableName+" set "
        lastkey = list(changls.keys())
        for change in changls:
            if change == lastkey[-1]:
                query += str(change)+" = '"+str(changls[change])+"'"
            else:
                query += str(change)+" = '"+str(changls[change])+"' ,"
        lastkey = list(condition.keys())
        query +="where "
        for con in condition:
            if con == lastkey[-1]:
                query += str(con) +"= '" +str(condition[con])+"'"
            else:
                query += str(con) +"= '" +str(condition[con])+"' and "

        print("query.............................",query)
        self.csr.execute(query)
        self.con.commit()


    def AddColoumn(self,TableName,Columns):

        query = "alter table "+ TableName
        lastkey = list(Columns.keys())

        for key in Columns.keys():
            if key == lastkey[-1]:
                query += " add column"+key + " " +Columns[key]
            else:
                query += " add column"+key + " " + Columns[key] +", "

        print("query..................................",query)
        self.csr.execute(query)
        self.con.commit()

    def ModifyColumn(self,TableName,Columns):
        query = "alter table "+TableName
        lastkey = list(Columns.keys())

        for key in Columns.keys():
            if key == lastkey[-1] :
                query += " alter column "+key +" type "+Columns[key]
            else:
                query += " alter column "+key +" type "+Columns[key]+","
        print("query..................................",query)
        self.csr.execute(query)
        self.con.commit()
         
    def DropColumn(self,TableName,ColumnLs):
        query = "alter table "+TableName

        for key in ColumnLs:
            if key == ColumnLs[-1]:
                query += " drop " +key
            else:
                query += " drop "+ key +","
        print("query.....................................",query)
        self.csr.execute(query)
        self.con.commit()

    def RenameTable(self,PrvName,NexName):
        query = "alter table "+ PrvName +" rename to "+NexName
        print("query.....................................",query)
        self.csr.execute(query)
        self.con.commit()

    def DeleteData(self,TableName,):
        query = "delete from "+TableName +" where "
        lastkey = list(condition.keys())
        for key in condition.keys():
            if key == lastkey:
                query = key +" = "+condition[key]
            else:
                query = key +" = "+condition[key]
        print("query.................................",query)
        self.csr.execute(query)
        self.con.commit()

    def SearchData(self,TableName,ColumnNameLs,ColumnName,SearchText):
        query = "select "
        for Column in ColumnNameLs:
            if Column == ColumnNameLs[-1]:
                query += Column
            else:
                query += Column +" ,"

        query +=" from "+TableName+" where "+ ColumnName+" like '"+ SearchText+"'"
        print("query.................................",query)
        self.csr.execute(query)
        self.con.commit()        
        return self.csr.fetchall()
        

    # def RenameColumn(self,TableName,Columns):
    #     query = "alter table "+ TableName 
    #     lastkey = list(Columns.keys())

    #     for key in Columns.keys():
    #         if key == lastkey[-1]:
    #             query += " rename column "+key +" to "+Columns[key]
    #         else:
    #             query += " rename column "+key +" to "+ Columns[key]+","
    #     print("query.....................................",query)
    #     self.csr.execute(query)
    #     self.con.commit()                

    


        
        




db = dbagent()
# NeedLs = ['imagesrc','title','subtitle']
# condition = {"title":"dubai","id":"4"}
# db.FetchData(NeedLs,"app1_news ",condition)

####################insert###########################

    # namels = ["name","emailid","subject7","message"]
    # valuels = ["thats5","thatsemail5","submail5","thatsmessag5"]
    # db.insert(namels,valuels,"message")
##################################update###############################
# changls = {"name":"karthiban"}
# condition = {"id":"2","message":"erfgthn"}
# db.update("app1_message",changls,condition)

####################################alter add column#############################
# db.AddColoumn("app1_message",{"some":"varchar(100)","next":"varchar(200)"})
##########################modify column#############################

# db.ModifyColumn("app1_message",{"columnsome":"text","next":"varchar(200)"})

######################################Drop coloumn###########################
# db.DropColumn("app1_message",['columnsome','next'])

#################################tablerename#############################
# db.RenameTable("app1_message","message")
#########################################column rename#############################
# db.RenameColumn("message",{"subject":"subject1","emailid":"email"})


###########################################table creation####################
# db.CreateTable("message1",{"name":"varchar(200)","times":"Time"})

##############################################search text##############################
db.SearchData("message",["name","message"],"message","%2")