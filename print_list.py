
list_c = ["sql_config",
"td_s_menu",
"td_s_operator_role",
"td_s_role_menu",
"td_s_task",
"td_s_task_exec_host",
"td_s_task_host_reource",
"td_s_task_log",
"td_s_task_log_detail",
"td_s_task_redo_log",
"td_s_task_release",
"td_s_task_sub",
"user_info",
"user_role"]

list_d = ["oc_active_mobile_change_request",
"oc_active_mobile_chg_realname_check",
"oc_order_kplane_verify_vst",
"sql_config",
"td_s_task",
"td_s_task_exec_host",
"td_s_task_host_reource",
"td_s_task_log",
"td_s_task_log_detail",
"td_s_task_redo_log",
"td_s_task_release",
"td_s_task_sub",
"tl_kplan_sign_index",
"wait_gzt_check"]

list = ["sql_config",
"td_s_menu",
"td_s_operator_role",
"td_s_role_menu",
"td_s_task",
"td_s_task_exec_host",
"td_s_task_host_reource",
"td_s_task_log",
"td_s_task_log_detail",
"td_s_task_redo_log",
"td_s_task_release",
"td_s_task_sub",
"user_info",
"user_role"]

for a in list:
    print('''{"table": "%s", "column": []},'''% a)
# b = [a for a in list_c if a not in list_d]
# for i in b:
#     print(i)