import pymssql
import os
import json
class DatabaseCreatorMSSQL:
    def __init__(self, server, user, password, db):
        self.conn = pymssql.connect(server, user, password, db)
        self.curs = self.conn.cursor()
    def _factor_cols(self, cols):
        result_string = ""
        for col in cols:
            if "Reference" in col:
                result_string += "\"" + col['Name'] + "\" bigint FOREIGN KEY REFERENCES " + col['Reference'] + "(ID) "
                if "OnDeleteCascade" in col:
                    result_string += ("ON DELETE CASCADE " if col['OnDeleteCascade'] else "ON DELETE SET NULL ")
                result_string += ", "
            else:
                result_string += "\"" + col['Name'] + "\" " + col['Type'] + " "
                if "Default" in col:
                    result_string += "DEFAULT (" + ("N'" + col['Default'] + "'" if "()" not in col['Default'] else col['Default']) + ") "
                if col["Null"]:
                    result_string += "NULL, "
                else:
                    result_string += "NOT NULL, "
        return result_string

    def execute(self, fileName):
        self.conn.commit()
        f=open(fileName, encoding="UTF-8")
        jsonString = f.read()
        jsonobj = json.loads(jsonString)
        print("Lookups started...")
        for table in jsonobj['Lookups']:
            create_string = "CREATE TABLE " + table['Name'] + "("
            create_string += "ID bigint NOT NULL IDENTITY(1,1),"
            create_string += "NAME nvarchar(1000) NOT NULL, "
            if "Cols" in table:
                create_string += self._factor_cols(table['Cols'])
            create_string += "CONSTRAINT " + table['Name'] + "_PK PRIMARY KEY (ID))"
            print(create_string)
            self.curs.execute(create_string)
            self.conn.commit()
        print("Dimensions started...")
        for table in jsonobj['Dimensions']:
            create_string = "CREATE TABLE " + table['Name'] + "("
            create_string += "ID bigint NOT NULL IDENTITY(1,1),"
            create_string += "NAME nvarchar(1000) NOT NULL, "
            create_string += "CREATED_DATE DATE DEFAULT (GETDATE()), "
            create_string += "CREATED_USER nvarchar(100) DEFAULT (N'SYSTEM'), "
            create_string += "LAST_UPDATED_DATE DATE DEFAULT (GETDATE()), "
            create_string += "LAST_UPDATED_USER nvarchar(100) DEFAULT (N'SYSTEM'), "
            if "Cols" in table:
                create_string += self._factor_cols(table['Cols'])
            create_string += "CONSTRAINT " + table['Name'] + "_PK PRIMARY KEY (ID))"
            print(create_string)
            self.curs.execute(create_string)
            self.conn.commit()
        print("Mappings started...")
        for table in jsonobj['Mappings']:
            create_string = "CREATE TABLE " + table['Name'] + "("
            create_string += "ID bigint NOT NULL IDENTITY(1,1), "
            create_string += "SOURCE_ID bigint FOREIGN KEY REFERENCES " + table['Source'] + "(ID), "
            create_string += "TARGET_ID bigint FOREIGN KEY REFERENCES " + table['Target'] + "(ID), "
            create_string += "CREATED_DATE DATE DEFAULT (GETDATE()), "
            create_string += "CREATED_USER nvarchar(100) DEFAULT (N'SYSTEM'), "
            create_string += "LAST_UPDATED_DATE DATE DEFAULT (GETDATE()), "
            create_string += "LAST_UPDATED_USER nvarchar(100) DEFAULT (N'SYSTEM'), "
            if "Cols" in table:
                create_string += self._factor_cols(table['Cols'])
            create_string += "CONSTRAINT " + table['Name'] + "_PK PRIMARY KEY (ID))"
            print(create_string)
            self.curs.execute(create_string)
            self.conn.commit()
        print("Histories started...")
        for table in jsonobj['Histories']:
            create_string = "CREATE TABLE " + table['Name'] + "("
            create_string += "ID bigint NOT NULL IDENTITY(1,1), "
            create_string += "CREATED_DATE DATE DEFAULT (GETDATE()), "
            create_string += "CREATED_USER nvarchar(100) DEFAULT (N'SYSTEM'), "
            create_string += "LAST_UPDATED_DATE DATE DEFAULT (GETDATE()), "
            create_string += "LAST_UPDATED_USER nvarchar(100) DEFAULT (N'SYSTEM'), "
            if "Cols" in table:
                create_string += self._factor_cols(table['Cols'])
            create_string += "CONSTRAINT " + table['Name'] + "_PK PRIMARY KEY (ID))"
            print(create_string)
            self.curs.execute(create_string)
            self.conn.commit()

    def close(self):
        self.conn.close();


