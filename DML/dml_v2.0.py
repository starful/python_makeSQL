import pandas as pd
import xlrd
import mysql.connector
import os
import os.path

# SQL接続情報
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    passwd="pass",
    db="art"
)


def main():

    if os.path.exists("dml"):
        pass
    else:
        os.mkdir("dml")

    ############ コードリスト ############
    # wbCode = xlrd.open_workbook('C:/work/python/DML/ART_コードリスト.xls')
    # sheetCode = wbCode.sheet_by_name("コード一覧(整備中) ")
    # DML:M_CODE_CLASSIFICATION生成
    # mCodeClassSql = codeClass(sheetCode)
    # DML:M_CODE
    # mCodeSql = code(sheetCode)
    # コード値SQL実行
    # codeSql()

    ############ マスタTBL ############
    wb = xlrd.open_workbook('C:/work/python/DML/ART_DBマスタデータ定義書.xlsx')
    # 統合DML
    mstIntegration(wb, mCodeClassSql, mCodeSql)
    # 個別DML
    mstSeparate(wb)
    # SQL実行
    mstSql()
    # 接続を閉じる
    conn.close()


def mstIntegration(wb, mCodeClassSql, mCodeSql):

    filename = 'DML.sql'
    with open('dml/' + filename, mode='w', encoding='UTF-8') as f:
        f.write(mCodeClassSql)
        f.write(mCodeSql)
        for sheetNames in wb.sheet_names()[2:len(wb.sheet_names())]:
            sheet = wb.sheet_by_name(sheetNames)
            columnType = sheet.row_values(1)
            tableName = sheet.row_values(0)[0]

            deleteSQL = "\nDELETE FROM " + tableName + ";"
            f.write(deleteSQL)

            for row in range(4, sheet.nrows):
                rowdata = sheet.row_values(row)
                resultSQL = mstMakeSql(tableName, columnType, rowdata)
                f.write(resultSQL)


def mstSeparate(wb):
    wb = xlrd.open_workbook('C:/work/python/DML/ART_DBマスタデータ定義書.xlsx')
    for sheetNames in wb.sheet_names()[2:len(wb.sheet_names())]:
        with open('dml/' + sheetNames+'.sql', mode='w', encoding='UTF-8') as f:
            sheet = wb.sheet_by_name(sheetNames)
            columnType = sheet.row_values(1)
            tableName = sheet.row_values(0)[0]

            deleteSQL = "\nDELETE FROM " + tableName + ";"
            f.write(deleteSQL)

            for row in range(4, sheet.nrows):
                rowdata = sheet.row_values(row)
                resultSQL = mstMakeSql(tableName, columnType, rowdata)
                f.write(resultSQL)


def mstSql():
    cursor = conn.cursor()
    print("*************** DML実行 ***************")
    for line in open('C:/work/python/DML/dml/DML.sql', encoding='UTF-8'):
        if line != '\n':
            print(line)
            cursor.execute(line)
    conn.commit()


def codeSql():
    cursor = conn.cursor()
    print("*************** M_CODE_CLASSIFICATION ***************")
    for line in open('C:/work/python/DML/dml/M_CODE_CLASSIFICATION.sql', encoding='UTF-8'):
        if line != '\n':
            print(line)
            cursor.execute(line)
    print("*************** M_CODE ***************")
    for line in open('C:/work/python/DML/dml/M_CODE.sql', encoding='UTF-8'):
        if line != '\n':
            print(line)
            cursor.execute(line)
    conn.commit()


def codeClass(sheetCode):
    path_mcode_class = 'M_CODE_CLASSIFICATION.sql'
    mCodeClassSql = ""
    with open("dml/" + path_mcode_class, mode='w', encoding='UTF-8') as f:
        deleteMCodeClassSql = "\nDELETE FROM M_CODE_CLASSIFICATION;"
        f.write(deleteMCodeClassSql)
        for row in range(2, sheetCode.nrows):
            if not sheetCode.row_values(row)[0]:
                pass
            else:
                # コード分類ID
                CODE_CLASSIFICATION_ID = sheetCode.row_values(row)[0]
                # コード分類
                CODE_CLASSIFICATION = sheetCode.row_values(row)[1]
                # コード分類(物理名)
                CODE_CLASSIFICATION_PHYSICS = sheetCode.row_values(row)[2]

                result = "\nINSERT INTO M_CODE_CLASSIFICATION VALUES(" \
                    + '\'' + CODE_CLASSIFICATION_ID + '\',' \
                    + '\'' + CODE_CLASSIFICATION + '\',' \
                    + '\'' + CODE_CLASSIFICATION_PHYSICS \
                    + '\')' + ";"
                mCodeClassSql = mCodeClassSql + result
                f.write(result)
    return deleteMCodeClassSql + mCodeClassSql


def code(sheetCode):
    path_mcode = 'M_CODE.sql'
    mCodeSql = ""
    with open("dml/" + path_mcode, mode='w', encoding='UTF-8') as f:
        deleteMCodeSql = "\nDELETE FROM M_CODE;"
        f.write(deleteMCodeSql)
        for row in range(2, sheetCode.nrows):
            # コード分類ID
            if not sheetCode.row_values(row)[0]:
                pass
            else:
                CODE_CLASSIFICATION_ID = sheetCode.row_values(row)[0]

            # コードID
            CODE_ID = sheetCode.row_values(row)[3]
            # コード名
            CODE_NAME = sheetCode.row_values(row)[4]
            # コード名(物理名)
            CODE_NAME_PHYSICS = sheetCode.row_values(row)[7]
            # コード略称
            CODE_ABB = sheetCode.row_values(row)[5]
            # ソート順
            SORT = sheetCode.row_values(row)[6]
            # コード
            USER_CODE = sheetCode.row_values(row)[8]
            if not USER_CODE:
                USER_CODE = 0

            result = "\nINSERT INTO M_CODE VALUES(" \
                + str(CODE_ID) + ',' \
                + '\'' + str(CODE_NAME_PHYSICS) + '\',' \
                + str(CODE_CLASSIFICATION_ID) + ',' \
                + '\'' + str(CODE_NAME) + '\',' \
                + '\'' + str(CODE_ABB) + '\',' \
                + str(SORT) + ',' \
                + '\'' + str(USER_CODE) \
                + '\')' + ";"
            mCodeSql = mCodeSql + result
            f.write(result)
    return deleteMCodeSql+mCodeSql


def mstMakeSql(tableName, columnType, rowdata):
    resultSQL = "\nINSERT INTO " + tableName + " VALUES("
    for x in range(0, len(rowdata)):
        if columnType[x] == 'BOOLEAN' or columnType[x] == 'TINYINT' or columnType[x] == 'SMALLINT' or columnType[x] == 'MEDIUMINT' or columnType[x] == 'BIGINT':
            if rowdata[x] == 'NULL':
                resultSQL = resultSQL + str(rowdata[x]) + ','
            else:
                resultSQL = resultSQL + str(int(rowdata[x])) + ','
        elif columnType[x] == 'FLOAT' or columnType[x] == 'DOUBLE' or columnType[x] == 'DECIMAL':
            resultSQL = resultSQL + str(rowdata[x]) + ','
        else:
            if rowdata[x] != 'NULL':
                resultSQL = resultSQL + '\'' + str(rowdata[x]) + '\','
            else:
                resultSQL = resultSQL + str(rowdata[x]) + ','

    resultSQL = resultSQL[:-1] + ');'
    return resultSQL


def executeSql(cursor, sql):
    try:
        print('実行SQL:' + sql)
        cursor.execute(sql)
    except Exception as e:
        print('MySQLdb.Error: ', e)
        raise e


if __name__ == '__main__':
    main()
