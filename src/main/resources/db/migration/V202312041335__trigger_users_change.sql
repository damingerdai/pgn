CREATE OR REPLACE FUNCTION "public"."notify_global_data_change"()
  RETURNS "pg_catalog"."trigger" AS $BODY$
BEGIN
  -- 表发生数据变化时，把表名、操作名以JSON形式通知到`change_data_capture`频道
  -- 其中`tg_table_name`是表名，`tg_op`是`INSERT`或`UPDATE`或`DELETE`
  PERFORM pg_notify('change_data_capture','{"table":"'||tg_table_name||'","operation":"'||tg_op||'"}');
  RETURN null;
END
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;


CREATE TRIGGER "trigger_table_name" AFTER INSERT OR UPDATE OR DELETE ON "public"."users"
FOR EACH ROW
EXECUTE PROCEDURE "public"."notify_global_data_change"();
