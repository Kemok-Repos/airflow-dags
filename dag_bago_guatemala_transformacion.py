from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

CONN = 'bago_guatemala_postgres'

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
  dag_id="transformacion-bago-guatemala",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso normal a las 3:00 AM
  schedule_interval='30 5 * * *', #UTC
  catchup=False,
  tags=['bago-guatemala','transformacion'],
) as dag:
    # Start Task Group Definition
    path = 'bago-guatemala-sql/sql/0_tablas_maestro/'
    with TaskGroup(group_id='0_tablas_maestro') as tg1:
        a0 = PostgresOperator(task_id="upsert_vendedores_sae", postgres_conn_id=CONN,
                                 sql=path+"tm001_upsert_vendedores_sae.sql")
        a1 = PostgresOperator(task_id="update_vendedores_g2s_clasificacion_de_vendedores", postgres_conn_id=CONN,
                                 sql=path+"tm002_update_vendedores_g2s_clasificacion_de_vendedores.sql")
        a2 = PostgresOperator(task_id="alias_de_articulos_update_pendientes", postgres_conn_id=CONN,
                                 sql=path+"tm003_alias_de_articulos_update_pendientes.sql")
        a3 = PostgresOperator(task_id="upsert_articulos_sae", postgres_conn_id=CONN,
                                 sql=path+"tm004_upsert_articulos_sae.sql")
        a4 = PostgresOperator(task_id="upsert_articulos_cendis", postgres_conn_id=CONN,
                                 sql=path+"tm005_upsert_articulos_cendis.sql")
        a5 = PostgresOperator(task_id="upsert_articulos_resco", postgres_conn_id=CONN,
                                 sql=path+"tm006_upsert_articulos_resco.sql")
        a6 = PostgresOperator(task_id="upsert_articulos_cruz_verde", postgres_conn_id=CONN,
                                 sql=path+"tm007_upsert_articulos_cruz_verde.sql")
        a7 = PostgresOperator(task_id="upsert_articulos_batres", postgres_conn_id=CONN,
                                 sql=path+"tm008_upsert_articulos_batres.sql")
        a8 = PostgresOperator(task_id="upsert_articulos_galeno", postgres_conn_id=CONN,
                                 sql=path+"tm009_upsert_articulos_galeno.sql")
        a9 = PostgresOperator(task_id="upsert_articulos_mexgua", postgres_conn_id=CONN,
                                 sql=path+"tm010_upsert_articulos_mexgua.sql")
        a10 = PostgresOperator(task_id="upsert_articulos_kielsa", postgres_conn_id=CONN,
                                 sql=path+"tm011_upsert_articulos_kielsa.sql")
        a11 = PostgresOperator(task_id="upsert_articulos_presupuesto", postgres_conn_id=CONN,
                                 sql=path+"tm012_upsert_articulos_presupuesto.sql")
        a12 = PostgresOperator(task_id="upsert_articulos_precios", postgres_conn_id=CONN,
                                 sql=path+"tm013_upsert_articulos_precios.sql")
        a13 = PostgresOperator(task_id="upsert_articulo_on_archivo_cendis", postgres_conn_id=CONN,
                                 sql=path+"tm014_upsert_articulo_on_archivo_cendis.sql")
        a14 = PostgresOperator(task_id="upsert_articulo_on_archivo_resco", postgres_conn_id=CONN,
                                 sql=path+"tm015_upsert_articulo_on_archivo_resco.sql")
        a15 = PostgresOperator(task_id="upsert_articulo_on_archivo_cruz_verde", postgres_conn_id=CONN,
                                 sql=path+"tm016_upsert_articulo_on_archivo_cruz_verde.sql")
        a16 = PostgresOperator(task_id="upsert_articulo_on_archivo_batres", postgres_conn_id=CONN,
                                 sql=path+"tm017_upsert_articulo_on_archivo_batres.sql")
        a17 = PostgresOperator(task_id="upsert_articulo_on_archivo_galeno", postgres_conn_id=CONN,
                                 sql=path+"tm018_upsert_articulo_on_archivo_galeno.sql")
        a18 = PostgresOperator(task_id="upsert_articulo_on_archivo_mexgua", postgres_conn_id=CONN,
                                 sql=path+"tm019_upsert_articulo_on_archivo_mexgua.sql")
        a19 = PostgresOperator(task_id="upsert_articulo_on_archivo_kielsa", postgres_conn_id=CONN,
                                 sql=path+"tm020_upsert_articulo_on_archivo_kielsa.sql")
        a20 = PostgresOperator(task_id="upsert_articulo_on_archivo_presupuesto", postgres_conn_id=CONN,
                                 sql=path+"tm021_upsert_articulo_on_archivo_presupuesto.sql")
        a21 = PostgresOperator(task_id="upsert_articulo_on_archivo_precios", postgres_conn_id=CONN,
                                 sql=path+"tm022_upsert_articulo_on_archivo_precios.sql")
        a22 = PostgresOperator(task_id="upsert_articulo_on_g2s_devoluciones", postgres_conn_id=CONN,
                                 sql=path+"tm023_upsert_articulo_on_g2s_devoluciones.sql")
        a23 = PostgresOperator(task_id="upsert_articulo_on_g2s_ventas_privadas_adicionales", postgres_conn_id=CONN,
                                 sql=path+"tm024_upsert_articulo_on_g2s_ventas_privadas_adicionales.sql")
        a24 = PostgresOperator(task_id="upsert_locacion_on_archivo_cendis", postgres_conn_id=CONN,
                                 sql=path+"tm025_upsert_locacion_on_archivo_cendis.sql")
        a25 = PostgresOperator(task_id="upsert_locacion_on_archivo_resco", postgres_conn_id=CONN,
                                 sql=path+"tm026_upsert_locacion_on_archivo_resco.sql")
        a26 = PostgresOperator(task_id="upsert_clientes_sae", postgres_conn_id=CONN,
                                 sql=path+"tm027_upsert_clientes_sae.sql")
        a27 = PostgresOperator(task_id="upsert_clientes_cendis", postgres_conn_id=CONN,
                                 sql=path+"tm028_upsert_clientes_cendis.sql")
        a28 = PostgresOperator(task_id="upsert_clientes_resco", postgres_conn_id=CONN,
                                 sql=path+"tm029_upsert_clientes_resco.sql")
        a29 = PostgresOperator(task_id="upsert_clientes_cruz_verde", postgres_conn_id=CONN,
                                 sql=path+"tm030_upsert_clientes_cruz_verde.sql")
        a30 = PostgresOperator(task_id="upsert_clientes_batres", postgres_conn_id=CONN,
                                 sql=path+"tm031_upsert_clientes_batres.sql")
        a31 = PostgresOperator(task_id="upsert_clientes_galeno", postgres_conn_id=CONN,
                                 sql=path+"tm032_upsert_clientes_galeno.sql")
        a32 = PostgresOperator(task_id="upsert_clientes_mexgua", postgres_conn_id=CONN,
                                 sql=path+"tm033_upsert_clientes_mexgua.sql")
        a33 = PostgresOperator(task_id="upsert_clientes_kielsa", postgres_conn_id=CONN,
                                 sql=path+"tm034_upsert_clientes_kielsa.sql")
        a34 = PostgresOperator(task_id="upsert_clientes_sae_anexos", postgres_conn_id=CONN,
                                 sql=path+"tm035_upsert_clientes_sae_anexos.sql")
        a35 = PostgresOperator(task_id="upsert_cliente_on_archivo_cendis", postgres_conn_id=CONN,
                                 sql=path+"tm036_upsert_cliente_on_archivo_cendis.sql")
        a36 = PostgresOperator(task_id="upsert_cliente_on_archivo_resco", postgres_conn_id=CONN,
                                 sql=path+"tm037_upsert_cliente_on_archivo_resco.sql")
        a37 = PostgresOperator(task_id="upsert_cliente_on_archivo_cruz_verde", postgres_conn_id=CONN,
                                 sql=path+"tm038_upsert_cliente_on_archivo_cruz_verde.sql")
        a38 = PostgresOperator(task_id="upsert_cliente_on_archivo_batres", postgres_conn_id=CONN,
                                 sql=path+"tm039_upsert_cliente_on_archivo_batres.sql")
        a39 = PostgresOperator(task_id="upsert_cliente_on_archivo_galeno", postgres_conn_id=CONN,
                                 sql=path+"tm040_upsert_cliente_on_archivo_galeno.sql")
        a40 = PostgresOperator(task_id="upsert_cliente_on_archivo_mexgua", postgres_conn_id=CONN,
                                 sql=path+"tm041_upsert_cliente_on_archivo_mexgua.sql")
        a41 = PostgresOperator(task_id="upsert_cliente_on_archivo_kielsa", postgres_conn_id=CONN,
                                 sql=path+"tm042_upsert_cliente_on_archivo_kielsa.sql")
        a42 = PostgresOperator(task_id="upsert_almacenes", postgres_conn_id=CONN,
                                 sql=path+"tm043_upsert_almacenes.sql")
        a43 = PostgresOperator(task_id="upsert_tipos_de_movimiento_comentarios_de_factura", postgres_conn_id=CONN,
                                 sql=path+"tm044_upsert_tipos_de_movimiento_comentarios_de_factura.sql")
        a44 = PostgresOperator(task_id="upsert_tipos_de_movimiento_comentarios_de_devolucion", postgres_conn_id=CONN,
                                 sql=path+"tm045_upsert_tipos_de_movimiento_comentarios_de_devolucion.sql")
        a45 = PostgresOperator(task_id="update_clientes_sin_mostrador", postgres_conn_id=CONN,
                                 sql=path+"tm046_update_clientes_sin_mostrador.sql")
        a46 = PostgresOperator(task_id="update_agrupacion_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm047_update_agrupacion_articulos.sql")
        a47 = PostgresOperator(task_id="update_agrupacion_clientes", postgres_conn_id=CONN,
                                 sql=path+"tm048_update_agrupacion_clientes.sql")
        a48 = PostgresOperator(task_id="update_agrupacion_de_locaciones", postgres_conn_id=CONN,
                                 sql=path+"tm049_update_agrupacion_de_locaciones.sql")
        a49 = PostgresOperator(task_id="update_agrupacion_kits", postgres_conn_id=CONN,
                                 sql=path+"tm050_update_agrupacion_kits.sql")
        a50 = PostgresOperator(task_id="maestro_de_clientes_update_ubicaciones_pendientes", postgres_conn_id=CONN,
                                 sql=path+"tm051_maestro_de_clientes_update_ubicaciones_pendientes.sql")
        a51 = PostgresOperator(task_id="auxiliar_precios_update_articulos_externos", postgres_conn_id=CONN,
                                 sql=path+"tm052_auxiliar_precios_update_articulos_externos.sql")
        a52 = PostgresOperator(task_id="update_vendedor_g2s_metricas_crm", postgres_conn_id=CONN,
                                 sql=path+"tm053_update_vendedor_g2s_metricas_crm.sql")
        a53 = PostgresOperator(task_id="update_vendedor_g2_asignacion", postgres_conn_id=CONN,
                                 sql=path+"tm054_update_vendedor_g2_asignacion.sql")
        a54 = PostgresOperator(task_id="auxiliar_cobertura_de_gira_upsert", postgres_conn_id=CONN,
                                 sql=path+"tm055_auxiliar_cobertura_de_gira_upsert.sql")
        a55 = PostgresOperator(task_id="auxiliar_cobertura_de_capital_upsert", postgres_conn_id=CONN,
                                 sql=path+"tm056_auxiliar_cobertura_de_capital_upsert.sql")
        a56 = PostgresOperator(task_id="auxiliar_asignacion_actual", postgres_conn_id=CONN,
                                 sql=path+"tm057_auxiliar_asignacion_actual.sql")

        a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> a6 >> a7 >> a8 >> a9
        a9 >> a10 >> a11 >> a12 >> a13 >> a14 >> a15 >> a16 >> a17 >> a18
        a18 >> a19 >> a20 >> a21 >> a22 >> a23 >> a24 >> a25 >> a26 >> a27
        a27 >> a28 >> a29 >> a30 >> a31 >> a32 >> a33 >> a34 >> a35 >> a36
        a36 >> a37 >> a38 >> a39 >> a40 >> a41 >> a42 >> a43 >> a44 >> a45
        a45 >> a46 >> a47 >> a48 >> a49 >> a50 >> a51 >> a52 >> a53 >> a54
        a54 >> a55 >> a56
   # End Task Group definition

    # Start Task Group Definition
    path = 'bago-guatemala-sql/sql/1_tablas_transaccionales/'
    with TaskGroup(group_id='1_tablas_transaccionales') as tg2:
        b0 = PostgresOperator(task_id="transacciones_de_venta_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt001_transacciones_de_venta_truncate.sql")
        b1 = PostgresOperator(task_id="transacciones_de_venta_insert_facturas", postgres_conn_id=CONN,
                                 sql=path+"tt002_transacciones_de_venta_insert_facturas.sql")
        b2 = PostgresOperator(task_id="transacciones_de_venta_insert_facturas_canceladas", postgres_conn_id=CONN,
                                 sql=path+"tt003_transacciones_de_venta_insert_facturas_canceladas.sql")
        b3 = PostgresOperator(task_id="transacciones_de_venta_insert_devoluciones_sin_detalle", postgres_conn_id=CONN,
                                 sql=path+"tt004_transacciones_de_venta_insert_devoluciones_sin_detalle.sql")
        b4 = PostgresOperator(task_id="transacciones_de_venta_insert_devoluciones_con_detalle", postgres_conn_id=CONN,
                                 sql=path+"tt005_transacciones_de_venta_insert_devoluciones_con_detalle.sql")
        b5 = PostgresOperator(task_id="transacciones_de_venta_insert_pronto_pago", postgres_conn_id=CONN,
                                 sql=path+"tt006_transacciones_de_venta_insert_pronto_pago.sql")
        b6 = PostgresOperator(task_id="transacciones_de_venta_update_movimientos_enlazados", postgres_conn_id=CONN,
                                 sql=path+"tt007_transacciones_de_venta_update_movimientos_enlazados.sql")
        b7 = PostgresOperator(task_id="transacciones_de_venta_update_cambio_de_cliente_ventas", postgres_conn_id=CONN,
                                 sql=path+"tt008_transacciones_de_venta_update_cambio_de_cliente_ventas.sql")
        b8 = PostgresOperator(task_id="transacciones_de_venta_update_cambio_de_cliente_devoluciones", postgres_conn_id=CONN,
                                 sql=path+"tt009_transacciones_de_venta_update_cambio_de_cliente_devoluciones.sql")
        b9 = PostgresOperator(task_id="transacciones_de_venta_update_cambio_de_cliente_g2s", postgres_conn_id=CONN,
                                 sql=path+"tt010_transacciones_de_venta_update_cambio_de_cliente_g2s.sql")
        b10 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_bagogeneral", postgres_conn_id=CONN,
                                 sql=path+"tt013_transacciones_de_venta_insert_ventas_de_bago - general.sql")
        b11 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_bago", postgres_conn_id=CONN,
                                 sql=path+"tt014_transacciones_de_venta_insert_ventas_de_bago.sql")
        b12 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_cendis", postgres_conn_id=CONN,
                                 sql=path+"tt015_transacciones_de_venta_insert_ventas_de_cendis.sql")
        b13 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_resco", postgres_conn_id=CONN,
                                 sql=path+"tt016_transacciones_de_venta_insert_ventas_de_resco.sql")
        b14 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_cruz_verde", postgres_conn_id=CONN,
                                 sql=path+"tt017_transacciones_de_venta_insert_ventas_de_cruz_verde.sql")
        b15 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_batres", postgres_conn_id=CONN,
                                 sql=path+"tt018_transacciones_de_venta_insert_ventas_de_batres.sql")
        b16 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_galeno", postgres_conn_id=CONN,
                                 sql=path+"tt019_transacciones_de_venta_insert_ventas_de_galeno.sql")
        b17 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_mexgua", postgres_conn_id=CONN,
                                 sql=path+"tt020_transacciones_de_venta_insert_ventas_de_mexgua.sql")
        b18 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_kielsa", postgres_conn_id=CONN,
                                 sql=path+"tt021_transacciones_de_venta_insert_ventas_de_kielsa.sql")
        b19 = PostgresOperator(task_id="transacciones_de_venta_insert_ventas_de_herdez", postgres_conn_id=CONN,
                                 sql=path+"tt022_transacciones_de_venta_insert_ventas_de_herdez.sql")
        b20 = PostgresOperator(task_id="transacciones_de_venta_update_venta_final", postgres_conn_id=CONN,
                                 sql=path+"tt023_transacciones_de_venta_update_venta_final.sql")
        b21 = PostgresOperator(task_id="transacciones_de_venta_update_clientes_activos", postgres_conn_id=CONN,
                                 sql=path+"tt024_transacciones_de_venta_update_clientes_activos.sql")
        b22 = PostgresOperator(task_id="auxiliar_division_de_canales_de_suministro_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt025_auxiliar_division_de_canales_de_suministro_truncate.sql")
        b23 = PostgresOperator(task_id="auxiliar_division_de_canales_de_suministro_insert_id", postgres_conn_id=CONN,
                                 sql=path+"tt026_auxiliar_division_de_canales_de_suministro_insert_id.sql")
        b24 = PostgresOperator(task_id="auxiliar_division_de_canales_de_suministro_update_especifico_individual", postgres_conn_id=CONN,
                                 sql=path+"tt027_auxiliar_division_de_canales_de_suministro_update_especifico_individual.sql")
        b25 = PostgresOperator(task_id="auxiliar_division_de_canales_de_suministro_update_especifico_grupal", postgres_conn_id=CONN,
                                 sql=path+"tt028_auxiliar_division_de_canales_de_suministro_update_especifico_grupal.sql")
        b26 = PostgresOperator(task_id="auxiliar_division_de_canales_de_suministro_update_ventana", postgres_conn_id=CONN,
                                 sql=path+"tt029_auxiliar_division_de_canales_de_suministro_update_ventana.sql")
        b27 = PostgresOperator(task_id="auxiliar_division_de_canales_de_suministro_update_proporcion", postgres_conn_id=CONN,
                                 sql=path+"tt030_auxiliar_division_de_canales_de_suministro_update_proporcion.sql")
        b28 = PostgresOperator(task_id="auxiliar_division_de_canales_de_suministro_delete_proporcion_cero", postgres_conn_id=CONN,
                                 sql=path+"tt031_auxiliar_division_de_canales_de_suministro_delete_proporcion_cero.sql")
        b29 = PostgresOperator(task_id="maestro_de_cliente_update_activos", postgres_conn_id=CONN,
                                 sql=path+"tt032_maestro_de_cliente_update_activos.sql")
        b30 = PostgresOperator(task_id="maestro_de_clientes_update_rfm", postgres_conn_id=CONN,
                                 sql=path+"tt033_maestro_de_clientes_update_rfm.sql")
        b31 = PostgresOperator(task_id="maestro_de_clientes_update_calificacion_crediticia", postgres_conn_id=CONN,
                                 sql=path+"tt034_maestro_de_clientes_update_calificacion_crediticia.sql")
        b32 = PostgresOperator(task_id="maestro_de_articulos_update_pareto", postgres_conn_id=CONN,
                                 sql=path+"tt035_maestro_de_articulos_update_pareto.sql")
        b33 = PostgresOperator(task_id="auxiliar_precios_temp", postgres_conn_id=CONN,
                                 sql=path+"tt036_auxiliar_precios_temp.sql")
        b34 = PostgresOperator(task_id="auxiliar_ventas_privadas_adicionales", postgres_conn_id=CONN,
                                 sql=path+"tt037_auxiliar_ventas_privadas_adicionales.sql")

        b0 >> b1 >> b2 >> b3 >> b4 >> b5 >> b6 >> b7 >> b8 >> b9
        b9 >> b10 >> b11 >> b12 >> b13 >> b14 >> b15 >> b16 >> b17 >> b18
        b18 >> b19 >> b20 >> b21 >> b22 >> b23 >> b24 >> b25 >> b26 >> b27
        b27 >> b28 >> b29 >> b30 >> b31 >> b32 >> b33 >> b34
   # End Task Group definition

    # Start Task Group Definition
    path = 'bago-guatemala-sql/sql/2_tablas_resumidas/'
    with TaskGroup(group_id='2_tablas_resumidas') as tg3:
        c0 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr001_resumen_de_ventas_mensuales_sell_out_truncate.sql")
        c1 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_insert_ventas_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr002_resumen_de_ventas_mensuales_sell_out_insert_ventas_inicial.sql")
        c2 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr003_resumen_de_ventas_mensuales_sell_out_update_ventas.sql")
        c3 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_insert_metas_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr004_resumen_de_ventas_mensuales_sell_out_insert_metas_inicial.sql")
        c4 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_metas", postgres_conn_id=CONN,
                                 sql=path+"tr005_resumen_de_ventas_mensuales_sell_out_update_metas.sql")
        c5 = PostgresOperator(task_id="auxiliar_alcances_sell_out", postgres_conn_id=CONN,
                                 sql=path+"tr006_auxiliar_alcances_sell_out.sql")
        c6 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_metas_especificas", postgres_conn_id=CONN,
                                 sql=path+"tr007_resumen_de_ventas_mensuales_sell_out_update_metas_especificas.sql")
        c7 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_metas_generales", postgres_conn_id=CONN,
                                 sql=path+"tr008_resumen_de_ventas_mensuales_sell_out_update_metas_generales.sql")
        c8 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_ventas_ytd", postgres_conn_id=CONN,
                                 sql=path+"tr009_resumen_de_ventas_mensuales_sell_out_update_ventas_ytd.sql")
        c9 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr010_resumen_de_ventas_mensuales_sell_out_update_year_to_year.sql")
        c10 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr012_resumen_de_ventas_mensuales_sell_in_truncate.sql")
        c11 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_insert_ventas_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr013_resumen_de_ventas_mensuales_sell_in_insert_ventas_inicial.sql")
        c12 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr014_resumen_de_ventas_mensuales_sell_in_update_ventas.sql")
        c13 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_metas", postgres_conn_id=CONN,
                                 sql=path+"tr015_resumen_de_ventas_mensuales_sell_in_update_metas.sql")
        c14 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_metas_especificas", postgres_conn_id=CONN,
                                 sql=path+"tr016_resumen_de_ventas_mensuales_sell_in_update_metas_especificas.sql")
        c15 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_metas_cadenas", postgres_conn_id=CONN,
                                 sql=path+"tr017_resumen_de_ventas_mensuales_sell_in_update_metas_cadenas.sql")
        c16 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_ventas_ytd", postgres_conn_id=CONN,
                                 sql=path+"tr018_resumen_de_ventas_mensuales_sell_in_update_ventas_ytd.sql")
        c17 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr019_resumen_de_ventas_mensuales_sell_in_update_year_to_year.sql")
        c18 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_out_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr020_resumen_de_ventas_mensuales_sell_out_delete_registros_cero.sql")
        c19 = PostgresOperator(task_id="resumen_de_ventas_mensuales_sell_in_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr021_resumen_de_ventas_mensuales_sell_in_delete_registros_cero.sql")

        c0 >> c1 >> c2 >> c3 >> c4 >> c5 >> c6 >> c7 >> c8 >> c9
        c9 >> c10 >> c11 >> c12 >> c13 >> c14 >> c15 >> c16 >> c17 >> c18
        c18 >> c19
   # End Task Group definition

    # Start Task Group Definition
    path = 'bago-guatemala-sql/sql/3_vistas_materializadas/'
    with TaskGroup(group_id='3_vistas_materializadas') as tg4:
        d0 = PostgresOperator(task_id="refresh_alias_de_locaciones", postgres_conn_id=CONN,
                                 sql=path+"vm001_refresh_alias_de_locaciones.sql")
        d1 = PostgresOperator(task_id="refresh_maestro_de_locaciones", postgres_conn_id=CONN,
                                 sql=path+"vm002_refresh_maestro_de_locaciones.sql")
        d2 = PostgresOperator(task_id="refresh_tabla_de_venta_unificada", postgres_conn_id=CONN,
                                 sql=path+"vm003_refresh_tabla_de_venta_unificada.sql")
        d3 = PostgresOperator(task_id="refresh_tabla_sell_in", postgres_conn_id=CONN,
                                 sql=path+"vm004_refresh_tabla_sell_in.sql")
        d4 = PostgresOperator(task_id="refresh_tabla_sell_out", postgres_conn_id=CONN,
                                 sql=path+"vm005_refresh_tabla_sell_out.sql")
        d5 = PostgresOperator(task_id="refresh_vista_de_presupuesto", postgres_conn_id=CONN,
                                 sql=path+"vm006_refresh_vista_de_presupuesto.sql")
        d6 = PostgresOperator(task_id="refresh_vista_sell_in", postgres_conn_id=CONN,
                                 sql=path+"vm007_refresh_vista_sell_in.sql")
        d7 = PostgresOperator(task_id="refresh_vista_sell_out", postgres_conn_id=CONN,
                                 sql=path+"vm008_refresh_vista_sell_out.sql")
        d8 = PostgresOperator(task_id="refresh_vista_de_venta_unificada", postgres_conn_id=CONN,
                                 sql=path+"vm009_refresh_vista_de_venta_unificada.sql")
        d9 = PostgresOperator(task_id="refresh_notificaciones", postgres_conn_id=CONN,
                                 sql=path+"vm010_refresh_notificaciones.sql")
        d10 = PostgresOperator(task_id="update_auxiliar_actualizacion", postgres_conn_id=CONN,
                                 sql=path+"vm011_update_auxiliar_actualizacion.sql")
        d11 = PostgresOperator(task_id="refresh_vista_venta_360", postgres_conn_id=CONN,
                                 sql=path+"vm012_refresh_vista_venta_360.sql")
        d12 = PostgresOperator(task_id="refresh_vista_reporte_360", postgres_conn_id=CONN,
                                 sql=path+"vm013_refresh_vista_reporte_360.sql")
        d13 = PostgresOperator(task_id="refresh_vista_visitador_medico", postgres_conn_id=CONN,
                                 sql=path+"vm014_refresh_vista_visitador_medico.sql")

        d0 >> d1 >> d2 >> d3 >> d4 >> d5 >> d6 >> d7 >> d8 >> d9
        d9 >> d10 >> d11 >> d12 >> d13
   # End Task Group definition

    tg1 >> tg2 >> tg3 >> tg4