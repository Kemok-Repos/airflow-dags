from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

CONN = 'bago_caricam_postgres'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=10)    
}
with DAG(
  dag_id="transformacion-bago-caricam",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso asincrono
  schedule_interval='0 6 * * *', #UTC
  catchup=False,
  tags=['bago-caricam','transformacion'],
) as dag:
    # Start Task Group Definition
    path = 'bago-caricam-sql/sql/0_maestros/'
    with TaskGroup(group_id='0_maestros') as tg1:
        a0 = PostgresOperator(task_id="upsert_articulo_medidor", postgres_conn_id=CONN,
                                 sql=path+"tm002_upsert_articulo_medidor.sql")
        a1 = PostgresOperator(task_id="upsert_articulo_ventas_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm003_upsert_articulo_ventas_por_distribuidor.sql")
        a2 = PostgresOperator(task_id="upsert_articulo_devoluciones_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm004_upsert_articulo_devoluciones_por_distribuidor.sql")
        a3 = PostgresOperator(task_id="upsert_articulo_agente_comercial", postgres_conn_id=CONN,
                                 sql=path+"tm005_upsert_articulo_agente_comercial.sql")
        a4 = PostgresOperator(task_id="upsert_articulo_presupuesto_costo_precio", postgres_conn_id=CONN,
                                 sql=path+"tm006_upsert_articulo_presupuesto_costo_precio.sql")
        a5 = PostgresOperator(task_id="upsert_articulo_presupuesto_de_venta", postgres_conn_id=CONN,
                                 sql=path+"tm007_upsert_articulo_presupuesto_de_venta.sql")
        a6 = PostgresOperator(task_id="upsert_articulo_ventas_por_distribuidor_precios", postgres_conn_id=CONN,
                                 sql=path+"tm008_upsert_articulo_ventas_por_distribuidor_precios.sql")
        a7 = PostgresOperator(task_id="upsert_articulo_on_archivo_medidor", postgres_conn_id=CONN,
                                 sql=path+"tm010_upsert_articulo_on_archivo_medidor.sql")
        a8 = PostgresOperator(task_id="upsert_articulos_on_archivo_ventas_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm011_upsert_articulos_on_archivo_ventas_por_distribuidor.sql")
        a9 = PostgresOperator(task_id="upsert_articulos_on_archivo_devoluciones_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm012_upsert_articulos_on_archivo_devoluciones_por_distribuidor.sql")
        a10 = PostgresOperator(task_id="upsert_articulo_on_archivo_agente_comercial", postgres_conn_id=CONN,
                                 sql=path+"tm013_upsert_articulo_on_archivo_agente_comercial.sql")
        a11 = PostgresOperator(task_id="upsert_articulo_on_archivo_presupuesto_costo_precio", postgres_conn_id=CONN,
                                 sql=path+"tm014_upsert_articulo_on_archivo_presupuesto_costo_precio.sql")
        a12 = PostgresOperator(task_id="upsert_articulo_on_archivo_presupuesto_de_venta", postgres_conn_id=CONN,
                                 sql=path+"tm015_upsert_articulo_on_archivo_presupuesto_de_venta.sql")
        a13 = PostgresOperator(task_id="upsert_articulo_on_archivo_ventas_por_distribuidor_precios", postgres_conn_id=CONN,
                                 sql=path+"tm016_upsert_articulo_on_archivo_ventas_por_distribuidor_precios.sql")
        a14 = PostgresOperator(task_id="upsert_locaciones_medidor", postgres_conn_id=CONN,
                                 sql=path+"tm020_upsert_locaciones_medidor.sql")
        a15 = PostgresOperator(task_id="upsert_locaciones_ventas_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm021_upsert_locaciones_ventas_por_distribuidor.sql")
        a16 = PostgresOperator(task_id="upsert_locaciones_devoluciones_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm022_upsert_locaciones_devoluciones_por_distribuidor.sql")
        a17 = PostgresOperator(task_id="upsert_locaciones_agente_comercial", postgres_conn_id=CONN,
                                 sql=path+"tm023_upsert_locaciones_agente_comercial.sql")
        a18 = PostgresOperator(task_id="upsert_locaciones_presupuesto_costo_precio", postgres_conn_id=CONN,
                                 sql=path+"tm024_upsert_locaciones_presupuesto_costo_precio.sql")
        a19 = PostgresOperator(task_id="upsert_locaciones_presupuesto_de_ventas", postgres_conn_id=CONN,
                                 sql=path+"tm025_upsert_locaciones_presupuesto_de_ventas.sql")
        a20 = PostgresOperator(task_id="upsert_locaciones_test_de_producto", postgres_conn_id=CONN,
                                 sql=path+"tm026_upsert_locaciones_test_de_producto.sql")
        a21 = PostgresOperator(task_id="upsert_locaciones_ventas_por_distribuidor_precios", postgres_conn_id=CONN,
                                 sql=path+"tm027_upsert_locaciones_ventas_por_distribuidor_precios.sql")
        a22 = PostgresOperator(task_id="upsert_locacion_on_archivo_medidor", postgres_conn_id=CONN,
                                 sql=path+"tm030_upsert_locacion_on_archivo_medidor.sql")
        a23 = PostgresOperator(task_id="upsert_locacion_on_archivo_ventas_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm031_upsert_locacion_on_archivo_ventas_por_distribuidor.sql")
        a24 = PostgresOperator(task_id="upsert_locacion_on_archivo_devoluciones_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm032_upsert_locacion_on_archivo_devoluciones_por_distribuidor.sql")
        a25 = PostgresOperator(task_id="upsert_locacion_on_archivo_agente_comercial", postgres_conn_id=CONN,
                                 sql=path+"tm033_upsert_locacion_on_archivo_agente_comercial.sql")
        a26 = PostgresOperator(task_id="upsert_locacion_on_archivo_presupuesto_costo_precio", postgres_conn_id=CONN,
                                 sql=path+"tm034_upsert_locacion_on_archivo_presupuesto_costo_precio.sql")
        a27 = PostgresOperator(task_id="upsert_locacion_on_archivo_presupuesto_de_venta", postgres_conn_id=CONN,
                                 sql=path+"tm035_upsert_locacion_on_archivo_presupuesto_de_venta.sql")
        a28 = PostgresOperator(task_id="upsert_locacion_on_archivo_test_de_producto", postgres_conn_id=CONN,
                                 sql=path+"tm036_upsert_locacion_on_archivo_test_de_producto.sql")
        a29 = PostgresOperator(task_id="upsert_locacion_on_archivo_ventas_por_distribuidor_precios", postgres_conn_id=CONN,
                                 sql=path+"tm037_upsert_locacion_on_archivo_ventas_por_distribuidor_precios.sql")
        a30 = PostgresOperator(task_id="upsert_clientes_ventas_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm040_upsert_clientes_ventas_por_distribuidor.sql")
        a31 = PostgresOperator(task_id="upsert_clientes_devoluciones_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm041_upsert_clientes_devoluciones_por_distribuidor.sql")
        a32 = PostgresOperator(task_id="upsert_clientes_medidor", postgres_conn_id=CONN,
                                 sql=path+"tm042_upsert_clientes_medidor.sql")
        a33 = PostgresOperator(task_id="upsert_clientes_agente_comercial", postgres_conn_id=CONN,
                                 sql=path+"tm043_upsert_clientes_agente_comercial.sql")
        a34 = PostgresOperator(task_id="upsert_clientes_presupuesto_costo_precio", postgres_conn_id=CONN,
                                 sql=path+"tm044_upsert_clientes_presupuesto_costo_precio.sql")
        a35 = PostgresOperator(task_id="upsert_clientes_presupuesto_de_ventas", postgres_conn_id=CONN,
                                 sql=path+"tm045_upsert_clientes_presupuesto_de_ventas.sql")
        a36 = PostgresOperator(task_id="upsert_clientes_test_de_producto", postgres_conn_id=CONN,
                                 sql=path+"tm046_upsert_clientes_test_de_producto.sql")
        a37 = PostgresOperator(task_id="upsert_clientes_ventas_por_distribuidor_precios", postgres_conn_id=CONN,
                                 sql=path+"tm047_upsert_clientes_ventas_por_distribuidor_precios.sql")
        a38 = PostgresOperator(task_id="upsert_clientes_on_archivo_medidor", postgres_conn_id=CONN,
                                 sql=path+"tm050_upsert_clientes_on_archivo_medidor.sql")
        a39 = PostgresOperator(task_id="upsert_clientes_on_archivo_ventas_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm051_upsert_clientes_on_archivo_ventas_por_distribuidor.sql")
        a40 = PostgresOperator(task_id="upsert_clientes_on_archivo_devoluciones_por_distribuidor", postgres_conn_id=CONN,
                                 sql=path+"tm052_upsert_clientes_on_archivo_devoluciones_por_distribuidor.sql")
        a41 = PostgresOperator(task_id="upsert_clientes_on_archivo_agente_comercial", postgres_conn_id=CONN,
                                 sql=path+"tm053_upsert_clientes_on_archivo_agente_comercial.sql")
        a42 = PostgresOperator(task_id="upsert_clientes_on_archivo_presupuesto_costo_precio", postgres_conn_id=CONN,
                                 sql=path+"tm054_upsert_clientes_on_archivo_presupuesto_costo_precio.sql")
        a43 = PostgresOperator(task_id="upsert_clientes_on_archivo_presupuesto_de_ventas", postgres_conn_id=CONN,
                                 sql=path+"tm055_upsert_clientes_on_archivo_presupuesto_de_ventas.sql")
        a44 = PostgresOperator(task_id="upsert_clientes_on_archivo_test_de_producto", postgres_conn_id=CONN,
                                 sql=path+"tm056_upsert_clientes_on_archivo_test_de_producto.sql")
        a45 = PostgresOperator(task_id="upsert_clientes_on_archivo_ventas_por_distribuidor_precios", postgres_conn_id=CONN,
                                 sql=path+"tm057_upsert_clientes_on_archivo_ventas_por_distribuidor_precios.sql")
        a46 = PostgresOperator(task_id="upsert_cliente_historico", postgres_conn_id=CONN,
                                 sql=path+"tm060_upsert_cliente_historico.sql")
        a47 = PostgresOperator(task_id="upsert_articulo_historico", postgres_conn_id=CONN,
                                 sql=path+"tm061_upsert_articulo_historico.sql")
        a48 = PostgresOperator(task_id="upsert_alias_articulo_historico", postgres_conn_id=CONN,
                                 sql=path+"tm062_upsert_alias_articulo_historico.sql")
        a49 = PostgresOperator(task_id="update_agrupacion_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm063_update_agrupacion_articulos.sql")
        a50 = PostgresOperator(task_id="update_auxiliar_costos", postgres_conn_id=CONN,
                                 sql=path+"tm064_update_auxiliar_costos.sql")

        a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> a6 >> a7 >> a8 >> a9
        a9 >> a10 >> a11 >> a12 >> a13 >> a14 >> a15 >> a16 >> a17 >> a18
        a18 >> a19 >> a20 >> a21 >> a22 >> a23 >> a24 >> a25 >> a26 >> a27
        a27 >> a28 >> a29 >> a30 >> a31 >> a32 >> a33 >> a34 >> a35 >> a36
        a36 >> a37 >> a38 >> a39 >> a40 >> a41 >> a42 >> a43 >> a44 >> a45
        a45 >> a46 >> a47 >> a48 >> a49 >> a50
   # End Task Group definition

    # Start Task Group Definition
    path = 'bago-caricam-sql/sql/1_tablas_transaccionales/'
    with TaskGroup(group_id='1_tablas_transaccionales') as tg2:
        b0 = PostgresOperator(task_id="transacciones_de_venta_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt001_transacciones_de_venta_truncate.sql")
        b1 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_sell_in", postgres_conn_id=CONN,
                                 sql=path+"tt002_transacciones_de_venta_insert_ventas_sell_in.sql")
        b2 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_sell_in_archivo", postgres_conn_id=CONN,
                                 sql=path+"tt003_transacciones_de_venta_insert_ventas_sell_in_archivo.sql")
        b3 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_sell_out", postgres_conn_id=CONN,
                                 sql=path+"tt004_transacciones_de_venta_insert_ventas_sell_out.sql")
        b4 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_sell_out_archivo", postgres_conn_id=CONN,
                                 sql=path+"tt005_transacciones_de_venta_insert_ventas_sell_out_archivo.sql")
        b5 = PostgresOperator(task_id="transacciones_de_venta_insert_devoluciones_sell_out_archivo", postgres_conn_id=CONN,
                                 sql=path+"tt006_transacciones_de_venta_insert_devoluciones_sell_out_archivo.sql")
        b6 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_agentes_comerciales", postgres_conn_id=CONN,
                                 sql=path+"tt007_transacciones_de_venta_insert_ventas_agentes_comerciales.sql")
        b7 = PostgresOperator(task_id="transacciones_de_venta_insert_devoluciones", postgres_conn_id=CONN,
                                 sql=path+"tt008_transacciones_de_venta_insert_devoluciones.sql")
        b8 = PostgresOperator(task_id="transacciones_de_venta_insert_devoluciones_archivo", postgres_conn_id=CONN,
                                 sql=path+"tt009_transacciones_de_venta_insert_devoluciones_archivo.sql")
        b9 = PostgresOperator(task_id="transacciones_de_venta_insert_conciliacion", postgres_conn_id=CONN,
                                 sql=path+"tt010_transacciones_de_venta_insert_conciliacion.sql")
        b10 = PostgresOperator(task_id="transacciones_de_venta_update_venta_final", postgres_conn_id=CONN,
                                 sql=path+"tt011_transacciones_de_venta_update_venta_final.sql")
        b11 = PostgresOperator(task_id="transacciones_de_venta_update_articulo_activos", postgres_conn_id=CONN,
                                 sql=path+"tt012_transacciones_de_venta_update_articulo_activos.sql")
        b12 = PostgresOperator(task_id="auxiliar_metas", postgres_conn_id=CONN,
                                 sql=path+"tt013_auxiliar_metas.sql")

        b0 >> b1 >> b2 >> b3 >> b4 >> b5 >> b6 >> b7 >> b8 >> b9
        b9 >> b10 >> b11 >> b12
   # End Task Group definition

    # Start Task Group Definition
    path = 'bago-caricam-sql/sql/2_tablas_resumidas/'
    with TaskGroup(group_id='2_tablas_resumidas') as tg3:
        c0 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr007_resumen_de_ventas_mensuales_sell_out_truncate.sql")
        c1 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_insert_inicial_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr008_resumen_de_ventas_mensuales_sell_out_insert_inicial_ventas.sql")
        c2 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr009_resumen_de_ventas_mensuales_sell_out_update_ventas.sql")
        c3 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_insert_metas_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr010_resumen_de_ventas_mensuales_sell_out_insert_metas_inicial.sql")
        c4 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_metas", postgres_conn_id=CONN,
                                 sql=path+"tr011_resumen_de_ventas_mensuales_sell_out_update_metas.sql")
        c5 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_ventas_ytd", postgres_conn_id=CONN,
                                 sql=path+"tr012_resumen_de_ventas_mensuales_sell_out_update_ventas_ytd.sql")
        c6 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr013_resumen_de_ventas_mensuales_sell_out_update_year_to_year.sql")
        c7 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr014_resumen_de_ventas_mensuales_sell_out_delete_registros_cero.sql")
        c8 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr015_resumen_de_ventas_mensuales_sell_in_truncate.sql")
        c9 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_insert_inicial_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr016_resumen_de_ventas_mensuales_sell_in_insert_inicial_ventas.sql")
        c10 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr017_resumen_de_ventas_mensuales_sell_in_update_ventas.sql")
        c11 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_insert_metas_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr018_resumen_de_ventas_mensuales_sell_in_insert_metas_inicial.sql")
        c12 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_metas", postgres_conn_id=CONN,
                                 sql=path+"tr019_resumen_de_ventas_mensuales_sell_in_update_metas.sql")
        c13 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_ventas_ytd", postgres_conn_id=CONN,
                                 sql=path+"tr020_resumen_de_ventas_mensuales_sell_in_update_ventas_ytd.sql")
        c14 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr021_resumen_de_ventas_mensuales_sell_in_update_year_to_year.sql")
        c15 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr022_resumen_de_ventas_mensuales_sell_in_delete_registros_cero.sql")

        c0 >> c1 >> c2 >> c3 >> c4 >> c5 >> c6 >> c7 >> c8 >> c9
        c9 >> c10 >> c11 >> c12 >> c13 >> c14 >> c15
   # End Task Group definition

    # Start Task Group Definition
    path = 'bago-caricam-sql/sql/3_vistas_materializadas/'
    with TaskGroup(group_id='3_vistas_materializadas') as tg4:
        d0 = PostgresOperator(task_id="update_views", postgres_conn_id=CONN,
                                 sql=path+"vm001_update_views.sql")
        d1 = PostgresOperator(task_id="ultima_actualizacion", postgres_conn_id=CONN,
                                 sql=path+"vm002_ultima_actualizacion.sql")

        d0 >> d1
   # End Task Group definition

    tg1 >> tg2 >> tg3 >> tg4