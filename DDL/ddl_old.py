import pandas as pd
import xlrd

# filename = 'キャッシュフロー+シミュレーション+チャート+ダッシュボード+帳票'
# filename = 'ダッシュボード'
# filename = 'ファミリー'
# filename = 'チャート'
# filename = '帳票'
# filename = 'キャッシュフロー'
# filename = 'シミュレーション'
# filename = '法人'
# filename = 'コード'
filename = "ART_DB項目定義書"

# 読み込みファイル
wb = xlrd.open_workbook('C:/work/python/DDL/' + filename + '.xlsx')
# 出力ファイル
path_w = filename + '.sql'

with open(path_w, mode='w', encoding="UTF-8") as f:

    # FOREIGN KEY DELETE
    foreign_delete = ''

    for sheetNames in wb.sheet_names()[3:len(wb.sheet_names())]:
        sheet = wb.sheet_by_name(sheetNames)

        # テーブル物理名
        tableName = sheet.row_values(2)[19]

        # テーブル日本語名
        tableJapanName = sheet.row_values(2)[30]

        print(tableJapanName + tableName)
        result = '\n\nDROP TABLE IF EXISTS %s CASCADE;\n\n'
        result = result + 'CREATE TABLE %s (\n%s, '
        result = result + 'PRIMARY KEY (%s)\n'

        # UNIQUE KEY
        result = result + '%s'

        # FOREIGN KEY
        result = result + '%s'
        result = result + ') COMMENT \'%s\' ENGINE=INNODB DEFAULT CHARSET=utf8mb4; '

        sql_column = []
        pklist = []
        uklist = []
        foreign_table = []
        foreign_column = []
        local_column = []

        for row in range(7, sheet.nrows):
            rowdata = sheet.row_values(row)
            # print("==============================")
            # print("項目名 : " + str(rowdata[1]))
            # print("PK : " + str(rowdata[7]))
            # print("FK : " + str(rowdata[8]))
            # print("NOTNULL : " + str(rowdata[9]))
            # print("unique key : " + str(rowdata[10]))
            # print("カラム名 : " + str(rowdata[11]))
            # print("型 : " + str(rowdata[16]))
            # print("桁数 : " + str(rowdata[19]))
            # print("初期値 : " + str(rowdata[20]))
            # print("auto increment : " + str(rowdata[22]))
            # print("unsigned : " + str(rowdata[24]))
            # if len(rowdata) > 36 and rowdata[36]:
            # 	print("FK TABLE : " + str(rowdata[36]))
            # print("==============================")

            # FOREIGN KEY
            if len(rowdata) > 36 and rowdata[36]:
                # if rowdata[36].find(',') and rowdata[37]:
                if len(rowdata) > 37 and rowdata[37]:
                    table = rowdata[36].split(',')
                    column = rowdata[37].split(',')
                    for x in range(0, len(table)):
                        foreign_table.append(table[x])
                        foreign_column.append(column[x])
                        local_column.append(str(rowdata[11]))
                else:
                    foreign_table.append(rowdata[36])
                    foreign_column.append(str(rowdata[11]))
                    local_column.append(str(rowdata[11]))
            sql_line = []

            # PK
            if rowdata[7]:
                pklist.append(str(rowdata[11]))
            # UK
            if rowdata[10]:
                uklist.append(str(rowdata[11]))
            # comma
            if row != 7:
                comma = ','
                sql_line.append(comma)
            # カラム名
            sql_line.append(str(rowdata[11]))

            # 型&型数値
            # 型BOOLEANの場合数値は指定しない。
            if rowdata[16] == 'BOOLEAN' or rowdata[16] == 'DATE' or rowdata[16] == 'TEXT':
                sql_line.append(str(rowdata[16]))
            else:
                sql_line.append(
                    str(rowdata[16]) + '(' + str(rowdata[19]).replace(".0", "") + ')')

            # unsigned = ''
            if rowdata[24]:
                sql_line.append('UNSIGNED')

            # AutoIncrement
            if rowdata[22]:
                sql_line.append('AUTO_INCREMENT')

            # 必須チェック
            if rowdata[9]:
                sql_line.append('NOT NULL')
            else:
                sql_line.append('NULL')

            # DEFAULT値
            if rowdata[20]:
                if rowdata[16] == 'BOOLEAN' or rowdata[16] == 'CHAR':
                    sql_line.append(
                        'DEFAULT ' + str(rowdata[20]).replace(".0", ""))
                else:
                    sql_line.append('DEFAULT ' + str(rowdata[20]))

            # 項目名
            sql_line.append('COMMENT \'' + rowdata[1] + '\'\n')

            sql_column.append(' '.join(sql_line))

        # UNIQUE KEY
        unique = ''
        if len(uklist) > 0:
            unique = ', UNIQUE INDEX %s_IDX1(%s)\n' % (
                tableName, ','.join(uklist))

        # FOREIGN KEY
        foreign = ''
        for i in range(len(foreign_table)):
            foreign = foreign + ', CONSTRAINT FK_%s_%s FOREIGN KEY (%s) REFERENCES %s(%s) ON UPDATE CASCADE ON DELETE CASCADE \n' % (
                tableName, foreign_table[i], local_column[i], foreign_table[i], foreign_column[i])
            foreign_delete = foreign_delete + \
                '\n ALTER TABLE %s DROP FOREIGN KEY `FK_%s_%s`;' % (
                    tableName, tableName, foreign_table[i])

        # print(result %(tableName, tableName, ''.join(sql_column), ','.join(pklist), unique, foreign, tableJapanName))
        f.write(result % (tableName, tableName, ''.join(sql_column),
                          ','.join(pklist), unique, foreign, tableJapanName))
    f.write('\n' + foreign_delete)
