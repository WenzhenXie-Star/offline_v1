# -*- coding: utf-8 -*-
import os

mysql_config = {
    "url": "jdbc:mysql://cdh03:3306/tms",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "root"
}

hive_config = {
    "metastore_uri": "thrift://cdh01:9083",
    "database": "tms",
    "hive_hadoop_conf_path": "/etc/hadoop/conf",
    "save_mode": "overwrite"
}

target_dir = "/opt/soft/seatunnel/seatunnel/job/"

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
        "    parallelism = 2\n"
        "    job.mode = \"BATCH\"\n"
        "}\n\n"
        "source {\n"
        "    Jdbc {\n"
        f"        url = \"{mysql_config['url']}\"\n"
        f"        driver = \"{mysql_config['driver']}\"\n"
        f"        user = \"{mysql_config['user']}\"\n"
        f"        password = \"{mysql_config['password']}\"\n"
        f"        table_path = \"{hive_config['database']}.{table}\"\n"
        f"        query = \"SELECT * FROM {hive_config['database']}.{table}\"\n"
        "    }\n"
        "}\n\n"
        "transform {\n"
        "    # 可按需添加数据转换操作\n"
        "}\n\n"
        "sink {\n"
        "    Hive {\n"
        f"        metastore_uri = \"{hive_config['metastore_uri']}\"\n"
        f"        table_name = \"{hive_config['database']}.{table}\"\n"
        f"        hive.hadoop.conf-path = \"{hive_config['hive_hadoop_conf_path']}\"\n"
        f"        save_mode = \"{hive_config['save_mode']}\"\n"
        "    }\n"
        "}\n"
    )
    config_file_name = os.path.join(target_dir, f"seatunnel_{table}.conf")
    with open(config_file_name, "w") as f:
        f.write(config_content)
    # 可选择直接执行生成的脚本
    # os.system(f"./bin/start-seatunnel.sh --config {config_file_name} --mode standalone")