#!/bin/bash
DATAX_HOME=/opt/module/datax
# 如果传入日期则do_date等于传入的日期，否则等于前一天日期
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

#处理目标路径，此处的处理逻辑是，如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
handle_targetdir() {
  hadoop fs -test -e $1
  if [[ $? -eq 1 ]]; then
    echo "路径$1不存在，正在创建......"
    hadoop fs -mkdir -p $1
  else
    echo "路径$1已经存在"
    fs_count=$(hadoop fs -count $1)
    content_size=$(echo $fs_count | awk '{print $3}')
    if [[ $content_size -eq 0 ]]; then
      echo "路径$1为空"
    else
      echo "路径$1不为空，正在清空......"
      hadoop fs -rm -r -f $1/*
    fi
  fi
}

#数据同步
import_data() {
  datax_config=$1
  target_dir=$2
  handle_targetdir $target_dir
  python $DATAX_HOME/bin/datax.py -p" -Dtargetdir=$target_dir" $datax_config
}

case $1 in
"order_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.order_info.json /2207A/wenzhen_xie/order_info/$do_date
  ;;
"base_category1")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_category1.json /2207A/wenzhen_xie/base_category1/$do_date
  ;;
"base_category2")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_category2.json /2207A/wenzhen_xie/base_category2/$do_date
  ;;
"base_category3")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_category3.json /2207A/wenzhen_xie/base_category3/$do_date
  ;;
"order_detail")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.order_detail.json /2207A/wenzhen_xie/order_detail/$do_date
  ;;
"sku_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.sku_info.json /2207A/wenzhen_xie/sku_info/$do_date
  ;;
"user_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.user_info.json /2207A/wenzhen_xie/user_info/$do_date
  ;;
"payment_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.payment_info.json /2207A/wenzhen_xie/payment_info/$do_date
  ;;
"base_province")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_province.json /2207A/wenzhen_xie/base_province/$do_date
  ;;
"base_region")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_region.json /2207A/wenzhen_xie/base_region/$do_date
  ;;
"base_trademark")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_trademark.json /2207A/wenzhen_xie/base_trademark/$do_date
  ;;
"activity_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.activity_info.json /2207A/wenzhen_xie/activity_info/$do_date
  ;;
"activity_order")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.activity_order.json /2207A/wenzhen_xie/activity_order/$do_date
  ;;
"cart_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.cart_info.json /2207A/wenzhen_xie/cart_info/$do_date
  ;;
"comment_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.comment_info.json/2207A/wenzhen_xie/comment_info/$do_date
  ;;
"coupon_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.coupon_info.json /2207A/wenzhen_xie/coupon_info/$do_date
  ;;
"coupon_use")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.coupon_use.json /2207A/wenzhen_xie/coupon_use/$do_date
  ;;
"favor_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.favor_info.json /2207A/wenzhen_xie/favor_info/$do_date
  ;;
"order_refund_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.order_refund_info.json /2207A/wenzhen_xie/order_refund_info/$do_date
  ;;
"order_status_log")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.order_status_log.json /2207A/wenzhen_xie/order_status_log/$do_date
  ;;
"spu_info")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.spu_info.json /2207A/wenzhen_xie/spu_info/$do_date
  ;;
"activity_rule")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.activity_rule.json /2207A/wenzhen_xieactivity_rule/$do_date
  ;;
"base_dic")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_dic.json /2207A/wenzhen_xie/base_dic/$do_date
  ;;
"all")
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.order_info.json /2207A/wenzhen_xie/order_info/$do_date
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_category1.json /2207A/wenzhen_xie/base_category1/$do_date
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_category2.json /2207A/wenzhen_xie/base_category2/$do_date
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.base_category3.json /2207A/wenzhen_xie/base_category3/$do_date
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.order_detail.json /2207A/wenzhen_xie/order_detail/$do_date
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.sku_info.json /2207A/wenzhen_xie/sku_info/$do_date
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.user_info.json /2207A/wenzhen_xie/user_info/$do_date
  import_data /opt/soft/datax/job/2207A/wenzhen_xie/import/gmall_v_Wenzhen_Xie.payment_info.json /2207A/wenzhen_xie/payment_info/$do_date
  ;;
esac