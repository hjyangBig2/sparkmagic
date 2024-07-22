import json
import logging
import os
import re
import requests
from typing import Tuple


from sparkmagic.utils.config import *

class SparkSqlAuth:
    def __init__(self, ):
        self.username = os.getenv("JUPYTERUHB_USER", "UnknowUser")
        self.project = os.getenv("JUPYTERUHB_SERVER_NAME", "UnknowProject")
        self.images = IMAGES
        self.h_databases = H_DATABASES
        self.cost_center_timeout = COST_CENTER_TIMEOUT
        self.cost_center_host = CABT_URL
        self.headers = HEADERS


    def create_table_auth(self, table_name: str, tables: list, auth_control: bool , type_str: str):
        if "." not in table_name:
            print(f"创建{type_str}语句中的{type_str}名缺少库名，{type_str}名称为：{table_name}")
            return False
        create_flag = self.create_table_check(table_name)
        if not create_flag:
            return False
        if auth_control:
            auth_flag = self.sql_exec_auth_check("",tables)
            if not auth_flag:
                return False
        return True


    # 查看有没有sql的执行权限
    def sql_exec_auth_check(self, table_name:str , tables: list) -> bool:
        if table_name:
            if len(table_name.split(".")) != 2:
                print(f"当前sql中的表名缺少库名，表名为{table_name}")
                return False
            all_flag = self.table_auth_check(table_name,"2")
            if not all_flag:
                print(f"您没有表【{table_name}】的可写权限，请参考技术支持群群公告中信大脑相关申请流程文档第二章节")
                return False
        for table in tables:
            if table == table_name:
                continue
            if "." not in table:
                print(f"当前sql中的表名缺少库名，表名为：{table}")
                return False
            auth_flag = self.table_auth_check(table,"1")
            if not auth_flag:
                print(f"您没有表【{table}】的可读权限，请参考技术支持群群公告中信大脑相关申请流程文档第二章节")
                return False
        return True

    #检测表是否可以创建
    def create_table_check(self, table_name):
        try:
            url = self.cost_center_host + '/api/service/table_info/add_check/'
            params = {"username": self.username, "table_name": table_name}
            res = requests.post(url=url, headers=self.headers, timeout=self.cost_center_timeout, json=params)
            data = res.json()
            if res.status_code == 400:
                msg = "【kde-notebook】【数据湖创建表检测】【检测失败原因】：{0}".format(data.get("msg", ""))
                print(msg)
                return False
            create_flag = data.get("date", False)
            if not create_flag:
                msg = "【kde-notebook】【数据湖创建表检测】【检测失败原因】：表{0}已经存在".format(table_name)
                print(msg)
                return False
            return True
        except Exception as e:
            msg = "【kde-notebook】【数据湖创建表检测】【检测失败原因】:{0}".format(e)
            print(msg)
            return False


    def sql_auth_check(self,sql,auth_control):
        sql_text = load_sql_txt(sql_text=sql)
        operation_flag,create_flag,auth_sqls = not_operation(self.username,sql_text,self.h_databases)
        if not operation_flag:
            return False
        if not auth_sqls:
            return True
        if create_flag:
            flag = self.record_tables(auth_sqls[0],auth_control)
            return flag
        elif not auth_control:
            return True
        else:
            auth_flag = self.pull_sql_auth(auth_sqls=auth_sqls)
            return auth_flag


   #operation_type:0:无权，1：只读，2：读写，3：可赋权
    def table_auth_check(self, table_name: str, operation_type: str) -> bool:
        try:
            url = self.cost_center_host + "/api/service/table_info/check"
            params = {"username": self.username, "table_name": table_name, "type": operation_type}
            res = requests.post(url=url, params=params, headers=self.headers, timeout=self.cost_center_timeout)
            data = res.json()
            if res.status_code == 400:
                msg = "【kde-notebook】【数据湖操作表鉴权：{0}】【鉴权失败原因】：{1}".format(table_name, data.get("msg", ""))
                print(msg)
                return False
            return True
        except Exception as e:
            msg = "【kde-notebook】【数据湖操操作表鉴权：{0}】【鉴权失败原因】：{1}".format(table_name,str(e))
            print(msg)
            return False

    def create_table_sql_parse(self, sql_line: str, auth_control: bool) -> Tuple[bool, list]:
        """创建sql解析"""
        table_pat = r'create\s+(?:external)?\s*table\s+(?:if\s+not\s+exists)?\s*[`]?(\w+\,?\w+)[`]?\s*'
        view_pat = r'create\s+view\s+(?:if\s+not\s+exists)?\s*[`]?(\w*\.?\w+)[`]?\s+'
        sql_line = sql_line.strip()
        s = sql_line.lower()
        table_res = re.search(table_pat, s)
        view_res = re.search(view_pat, s)
        sql_new = re.sub(r'\s+view\s+', " table ", s)
        ts = get_sql_table(sql_new)
        if table_res:
            table_name = table_res.group(1)
            c_flag = self.create_table_auth(table_name,ts,auth_control,"表")
            if not c_flag:
                return False,[]
            return True, ["table", table_name, sql_line, "create"]
        else:
            view_name = view_res.group(1)
            c_flag = self.create_table_auth(view_name,ts,auth_control,"视图")
            if not c_flag:
                return False,[]
            return True,["table", view_name, sql_line,"create"]

    def record_tables(self, sql_line: str, auth_control: bool) -> bool:
        """查看权限，并且将表插入到库中"""
        c_flag, tables = self.create_table_sql_parse(sql_line,auth_control)
        if not c_flag:
            return False
        if not tables:
            return True
        self.send_batch_tables(tables)
        return True

    def send_batch_tables(self, tables: list) -> None:
        # 创建的表维护到大脑数据库中
        self.create_or_update_tables_status(tables[1],tables[0],tables[2],tables[3])
        message = f"用户【{self.username}】在项目【{self.project}】中创建了一张{tables[0]}名：【{tables[1]}】,创建{tables[0]}的sql语句为：\n{tables[2]}"
        print(message)
        logging.info(message)

    def create_or_update_tables_status(self, table_name: str, table_type: str, hsql: str, status: str) -> bool:
        try:
            url = self.cost_center_host + "/api/service/table_info/add/"
            params = {"creator": self.username,
                      "table_name": table_name,
                      "table_type": table_type,
                      "sql": hsql,
                      "image": self.images,
                      "state": status}
            res = requests.post(url=url, headers=self.headers, timeout=self.cost_center_timeout, json=params)
            data = res.json()
            if res.status_code == 400:
                msg = "[kde-notebook]【数据湖创建表维护到数据库失败原因】：{0}".format(data.get("msg", ""))
                logging.warn(msg)
                return False
            return True
        except Exception as e :
            msg = "【kds-notebook】【数据湖创建记录或者更新状态】【记录失败原因】：{0}".format(str(e))
            logging.error(msg)
            return False

    def pull_sql_auth(self, auth_sqls: list):
        for sql in auth_sqls:
            break_flag,flag = self.exec_auth_check(sql_type='alter',table_type="table",sql=sql)
            if not flag:
                return False
            if break_flag:
               continue
            break_flag, flag = self.exec_auth_check(sql_type='drop', table_type="table", sql=sql)
            if not flag:
                return False
            if break_flag:
                continue
            break_flag, flag = self.exec_auth_check(sql_type='drop', table_type="view", sql=sql)
            if not flag:
                return False
            if break_flag:
                continue
            break_flag, flag = self.exec_auth_check(sql_type='insert', table_type="view", sql=sql)
            if not flag:
                return False
            if break_flag:
                continue
            tables = parser_query_tables(sql_line=sql)
            flag = self.sql_exec_auth_check(table_name="",tables=sql)
            if not flag:
                return False
        return True
#加载sql 去除注释部分
def load_sql_txt(sql_text:str) -> str:
    """
       将--注释和空串，以及多行注释的sql语句去除
    Parameters
    ----------
    sql_text

    Returns
    -------
    """
    lines = sql_text.split("\n")
    flag = False
    for line in lines :
        sql_line = line.strip()
        if not (sql_line.startswith("--") or sql_line == ""):
            if sql_line.startswith("/*"):
                flag = True
            elif sql_line.endswith("*/"):
                flag = False
            elif not flag:
                if sql_text:
                    sql_text = "{0}\n{1}".format(sql_text,sql_line)
                else:
                    sql_text = sql_line

    return sql_text


def not_operation(username:str,sql_text:str,h_database:list) -> Tuple[bool,bool,list]:
    """
    过滤某些操作
    Parameters
    ----------
    username
    sql_text
    h_database

    Returns
    -------

    """
    create_flag = False
    temp_pat = r'reate\s+(?:global)?\s*temporary\s+'
    if re.search(temp_pat,sql_text):
        print("创建临时表请参考：create sdms_ads_cabt.tmp_表名")
        return False, create_flag, []
    pat = r'alter\s+view\s+[`]?(\w*\.?\w+)[`]?\s*'
    res = re.search(pat,sql_text)
    if res:
        print("暂时不支持修改视图的操作,请删除重新创建")
        return False, create_flag, []
    table_pat = r'create\s+(?:external)?\s*table\s+(?:if\s+not\s+exists)\s*[`]?(\w*\.?\w+)[`]?\s*'
    s = re.sub(r'\s+view+\s',"table ",sql_text)
    res = re.search(table_pat,s)
    if res:
        database = res.group(1).split(".")[0]
        if database not in h_database:
            print(f"您没有{database}库的创建表权限")
            return False ,create_flag ,[]
        create_flag = True
    lines = sql_text.split(";")
    auth_sqls = []
    for sql_line in lines:
        s = sql_line.lower()
        if not s:
            continue
        if (s.startswith("desc ") or s.startswith("desc\n") or s.startswith("desc\t")
               or s.startswith("describe ") or s.startswith("describe\n") or s.startswith("describe\t")
                or s.startswith("show ") or s.startswith("show\n") or s.startswith("show\t")
                or s.startswith("explain ") or s.startswith("explain\n") or s.startswith("explain\t")
                or s.startswith("set ") or s.startswith("set\n") or s.startswith("set\t")):
            continue
        if not (s.startswith("create ") or s.startswith("create\n") or s.startswith("create\t")
                or s.startswith("insert ") or s.startswith("insert\n") or s.startswith("insert\t")
                or s.startswith("select ") or s.startswith("select\n") or s.startswith("select\t")
                or s.startswith("with ") or s.startswith("with\n") or s.startswith("with\t")
                or s.startswith("alter ") or s.startswith("alter\n") or s.startswith("alter\t")
                or s.startswith("drop ") or s.startswith("drop\n") or s.startswith("drop\t")
               ):
            print("当前操作只支持【create,insert,select,set,alter,drop】操作")
            return False,create_flag,[]
        auth_sqls.append(s)

    if create_flag and len(auth_sqls) > 1:
        print("创建表语句请单独一个cell操作，一个cell暂时只支持一个创建表语句！！！！！")
        return False, create_flag, []

    return True, create_flag, auth_sqls








def get_sql_table(sql_line: str) -> list:
    sql_str = re.sub(r'stored\s+as\s+\w+'," ", sql_line)
    str_list = re.split(pattern=r'\s+as\s+', String=sql_str, maxsplit=1)
    if len(str_list) < 2:
        return []
    sql_lines = str_list[1]
    tables = parser_query_tables(sql_lines)
    return tables


#从sql中提取表名
def parser_query_tables(sql_line: str) ->list:
    try:
        tables = get_sql_table(sql_line)
    except Exception as  e:
        print("[程序报错了，报错原因为：]", e)
        tables = []
    return tables
