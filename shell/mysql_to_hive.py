# -*- coding: utf-8 -*-
import os

mysql_config = {
    "url": "jdbc:mysql://cdh03:3306/",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "root"
}

hive_config = {
    "metastore_uri": "thrift://cdh01:9083",
    "database": "tms",
    "hive_conf_dir": "/etc/hive/conf.cloudera.hive"
}

target_dir = "/opt/soft/apache-seatunnel-2.3.10/job/"

tables = [
    "a_template_city_distance", "base_complex", "base_dic", "base_organ",
    "base_region_info", "employee_info", "express_courier", "express_courier_complex",
    "express_task_collect", "express_task_delivery", "line_base_info", "line_base_shift",
    "order_cargo", "order_info", "order_org_bound", "order_trace_log", "sorter_info",
    "transport_plan_line_detail", "transport_task", "transport_task_detail",
    "transport_task_process", "truck_driver", "truck_info", "truck_model", "truck_team",
    "user_address", "user_info"
]  # 要传输的表名列表

for table in tables:
    config_content = (
        "env {\n"
        "    execution.parallelism = 1\n"
        "    job.mode = \"BATCH\"\n"
        "}\n\n"
        "source {\n"
        "    Jdbc {\n"
        "        url = \"{0}\"\n"
        "        driver = \"{1}\"\n"
        "        user = \"{2}\"\n"
        "        password = \"{3}\"\n"
        "        query = \"SELECT * FROM {4}\"\n"
        "    }\n"
        "}\n\n"
        "transform {\n"
        "    # 可按需添加数据转换操作\n"
        "}\n\n"
        "sink {\n"
        "    Hive {\n"
        "        metastore_uri = \"{5}\"\n"
        "        database = \"{6}\"\n"
        "        table = \"{4}\"\n"
        "        hive_conf_dir = \"{7}\"\n"
        "    }\n"
        "}\n".format(mysql_config['url'], mysql_config['driver'], mysql_config['user'], mysql_config['password'], table, hive_config['metastore_uri'], hive_config['database'], hive_config['hive_conf_dir'])
    )
    config_file_name = os.path.join(target_dir, "seatunnel_{0}.conf".format(table))
    with open(config_file_name, "w") as f:
        f.write(config_content)
    # 可选择直接执行生成的脚本
    # os.system("./bin/start-seatunnel.sh --config {0} --mode standalone".format(config_file_name))