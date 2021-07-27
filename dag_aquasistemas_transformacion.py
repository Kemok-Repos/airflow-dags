from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

CONN = 'aquasistemas_postgres'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=120)    
}
with DAG(
  dag_id="transformacion-aquasistemas",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso manual
  schedule_interval=None, #UTC
  max_active_runs=1,
  catchup=False,
  tags=['aquasistemas','transformacion'],
) as dag:
    # Start Task Group Definition
    path = 'aquasistemas-sql/sql/0_tablas_maestras/'
    with TaskGroup(group_id='0_tablas_maestras') as tg1:
        a0 = PostgresOperator(task_id="update_maestro_id", postgres_conn_id=CONN,
                                 sql=path+"tm001_update_maestro_id.sql")
        a1 = PostgresOperator(task_id="upsert_tipos_de_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tm002_upsert_tipos_de_movimiento.sql")
        a2 = PostgresOperator(task_id="upsert_canal_de_venta", postgres_conn_id=CONN,
                                 sql=path+"tm003_upsert_canal_de_venta.sql")
        a3 = PostgresOperator(task_id="update_canal_de_venta_on_g2s_series_de_documento", postgres_conn_id=CONN,
                                 sql=path+"tm004_update_canal_de_venta_on_g2s_series_de_documento.sql")
        a4 = PostgresOperator(task_id="update_canal_de_venta_on_g2s_metas_vendedor", postgres_conn_id=CONN,
                                 sql=path+"tm005_update_canal_de_venta_on_g2s_metas_vendedor.sql")
        a5 = PostgresOperator(task_id="update_canal_de_venta_on_g2s_bonificacion_por_tipo_de_meta", postgres_conn_id=CONN,
                                 sql=path+"tm006_update_canal_de_venta_on_g2s_bonificacion_por_tipo_de_meta.sql")
        a6 = PostgresOperator(task_id="update_canal_de_venta_on_g2s_metas_por_area", postgres_conn_id=CONN,
                                 sql=path+"tm007_update_canal_de_venta_on_g2s_metas_por_area.sql")
        a7 = PostgresOperator(task_id="update_canal_de_venta_on_g2s_parametros_de_comisiones", postgres_conn_id=CONN,
                                 sql=path+"tm008_update_canal_de_venta_on_g2s_parametros_de_comisiones.sql")
        a8 = PostgresOperator(task_id="upsert_clientes", postgres_conn_id=CONN,
                                 sql=path+"tm010_upsert_clientes.sql")
        a9 = PostgresOperator(task_id="update_clientes_correccion_de_locacion", postgres_conn_id=CONN,
                                 sql=path+"tm011_update_clientes_correccion_de_locacion.sql")
        a10 = PostgresOperator(task_id="update_cliente_on_g2s_ubicaciones_de_clientes", postgres_conn_id=CONN,
                                 sql=path+"tm012_update_cliente_on_g2s_ubicaciones_de_clientes.sql")
        a11 = PostgresOperator(task_id="update_clientes_ubicaciones_confirmadas", postgres_conn_id=CONN,
                                 sql=path+"tm013_update_clientes_ubicaciones_confirmadas.sql")
        a12 = PostgresOperator(task_id="upsert_almacenes", postgres_conn_id=CONN,
                                 sql=path+"tm014_upsert_almacenes.sql")
        a13 = PostgresOperator(task_id="update_almacen_on_g2s_series_de_documentos", postgres_conn_id=CONN,
                                 sql=path+"tm015_update_almacen_on_g2s_series_de_documentos.sql")
        a14 = PostgresOperator(task_id="update_almacen_on_g2s_metas_vendedor", postgres_conn_id=CONN,
                                 sql=path+"tm016_update_almacen_on_g2s_metas_vendedor.sql")
        a15 = PostgresOperator(task_id="upsert_vendedores", postgres_conn_id=CONN,
                                 sql=path+"tm017_upsert_vendedores.sql")
        a16 = PostgresOperator(task_id="update_vendedor_on_g2s_metas_vendedor", postgres_conn_id=CONN,
                                 sql=path+"tm018_update_vendedor_on_g2s_metas_vendedor.sql")
        a17 = PostgresOperator(task_id="update_vendedor_on_g2s_ubicaciones_de_clientes", postgres_conn_id=CONN,
                                 sql=path+"tm019_update_vendedor_on_g2s_ubicaciones_de_clientes.sql")
        a18 = PostgresOperator(task_id="update_vendedor_on_g2s_metricas_de_ruta", postgres_conn_id=CONN,
                                 sql=path+"tm020_update_vendedor_on_g2s_metricas_de_ruta.sql")
        a19 = PostgresOperator(task_id="upsert_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm021_upsert_articulos.sql")
        a20 = PostgresOperator(task_id="upsert_kits", postgres_conn_id=CONN,
                                 sql=path+"tm022_upsert_kits.sql")
        a21 = PostgresOperator(task_id="upsert_proveedores", postgres_conn_id=CONN,
                                 sql=path+"tm023_upsert_proveedores.sql")
        a22 = PostgresOperator(task_id="update_agrupacion_vendedores", postgres_conn_id=CONN,
                                 sql=path+"tm024_update_agrupacion_vendedores.sql")
        a23 = PostgresOperator(task_id="update_agrupacion_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm025_update_agrupacion_articulos.sql")
        a24 = PostgresOperator(task_id="update_agrupacion_proveedores", postgres_conn_id=CONN,
                                 sql=path+"tm026_update_agrupacion_proveedores.sql")
        a25 = PostgresOperator(task_id="update_auxiliar_proveedores", postgres_conn_id=CONN,
                                 sql=path+"tm027_update_auxiliar_proveedores.sql")
        a26 = PostgresOperator(task_id="update_cartera", postgres_conn_id=CONN,
                                 sql=path+"tm028_update_cartera.sql")
        a27 = PostgresOperator(task_id="update_canal_de_venta_with_g2s_series_de_documentos", postgres_conn_id=CONN,
                                 sql=path+"tm029_update_canal_de_venta_with_g2s_series_de_documentos.sql")
        a28 = PostgresOperator(task_id="auxiliar_bonificacion", postgres_conn_id=CONN,
                                 sql=path+"tm030_auxiliar_bonificacion.sql")
        a29 = PostgresOperator(task_id="auxiliar_comisiones_de_ruta", postgres_conn_id=CONN,
                                 sql=path+"tm032_auxiliar_comisiones_de_ruta.sql")
        a30 = PostgresOperator(task_id="update_tiempo_de_surtido", postgres_conn_id=CONN,
                                 sql=path+"tm100_update_tiempo_de_surtido.sql")

        a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> a6 >> a7 >> a8 >> a9
        a9 >> a10 >> a11 >> a12 >> a13 >> a14 >> a15 >> a16 >> a17 >> a18
        a18 >> a19 >> a20 >> a21 >> a22 >> a23 >> a24 >> a25 >> a26 >> a27
        a27 >> a28 >> a29 >> a30
   # End Task Group definition

    # Start Task Group Definition
    path = 'aquasistemas-sql/sql/1_tablas_transaccionales/'
    with TaskGroup(group_id='1_tablas_transaccionales') as tg2:
        b0 = PostgresOperator(task_id="transacciones_de_venta_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt001_transacciones_de_venta_truncate.sql")
        b1 = PostgresOperator(task_id="transacciones_de_venta_insert_facturas", postgres_conn_id=CONN,
                                 sql=path+"tt002_transacciones_de_venta_insert_facturas.sql")
        b2 = PostgresOperator(task_id="transacciones_de_venta_insert_facturas_canceladas", postgres_conn_id=CONN,
                                 sql=path+"tt003_transacciones_de_venta_insert_facturas_canceladas.sql")
        b3 = PostgresOperator(task_id="transacciones_de_venta_insert_devoluciones", postgres_conn_id=CONN,
                                 sql=path+"tt004_transacciones_de_venta_insert_devoluciones.sql")
        b4 = PostgresOperator(task_id="transacciones_de_venta_insert_devoluciones_canceladas", postgres_conn_id=CONN,
                                 sql=path+"tt005_transacciones_de_venta_insert_devoluciones_canceladas.sql")
        b5 = PostgresOperator(task_id="transacciones_de_venta_update_movimientos_enlazados", postgres_conn_id=CONN,
                                 sql=path+"tt006_transacciones_de_venta_update_movimientos_enlazados.sql")
        b6 = PostgresOperator(task_id="transacciones_de_venta_update_facturas_kit", postgres_conn_id=CONN,
                                 sql=path+"tt007_transacciones_de_venta_update_facturas_kit.sql")
        b7 = PostgresOperator(task_id="transacciones_de_venta_update_devoluciones_kit", postgres_conn_id=CONN,
                                 sql=path+"tt008_transacciones_de_venta_update_devoluciones_kit.sql")
        b8 = PostgresOperator(task_id="maestro_de_articulos_update_pareto", postgres_conn_id=CONN,
                                 sql=path+"tt009_maestro_de_articulos_update_pareto.sql")
        b9 = PostgresOperator(task_id="transacciones_de_inventario_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt010_transacciones_de_inventario_truncate.sql")
        b10 = PostgresOperator(task_id="transacciones_de_inventario_insert_dimensiones", postgres_conn_id=CONN,
                                 sql=path+"tt011_transacciones_de_inventario_insert_dimensiones.sql")
        b11 = PostgresOperator(task_id="transacciones_de_inventario_update_orden_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tt012_transacciones_de_inventario_update_orden_movimiento.sql")
        b12 = PostgresOperator(task_id="transacciones_de_inventario_update_rastreo_movimientos", postgres_conn_id=CONN,
                                 sql=path+"tt013_transacciones_de_inventario_update_rastreo_movimientos.sql")
        b13 = PostgresOperator(task_id="transacciones_de_inventario_update_existencias_actuales", postgres_conn_id=CONN,
                                 sql=path+"tt014_transacciones_de_inventario_update_existencias_actuales.sql")
        b14 = PostgresOperator(task_id="transacciones_de_inventario_update_existencias_historicas", postgres_conn_id=CONN,
                                 sql=path+"tt015_transacciones_de_inventario_update_existencias_historicas.sql")
        b15 = PostgresOperator(task_id="transacciones_de_inventario_update_monto_en_inventario", postgres_conn_id=CONN,
                                 sql=path+"tt016_transacciones_de_inventario_update_monto_en_inventario.sql")
        b16 = PostgresOperator(task_id="transacciones_de_inventario_update_dias_sin_existencias", postgres_conn_id=CONN,
                                 sql=path+"tt017_transacciones_de_inventario_update_dias_sin_existencias.sql")
        b17 = PostgresOperator(task_id="maestro_de_clientes_update_rfm", postgres_conn_id=CONN,
                                 sql=path+"tt020_maestro_de_clientes_update_rfm.sql")
        b18 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt030_transacciones_de_cuentas_por_cobrar_truncate.sql")
        b19 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_insert_cuentas", postgres_conn_id=CONN,
                                 sql=path+"tt031_transacciones_de_cuentas_por_cobrar_insert_cuentas.sql")
        b20 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_insert_abonos", postgres_conn_id=CONN,
                                 sql=path+"tt032_transacciones_de_cuentas_por_cobrar_insert_abonos.sql")
        b21 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_update_estatus", postgres_conn_id=CONN,
                                 sql=path+"tt033_transacciones_de_cuentas_por_cobrar_update_estatus.sql")
        b22 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_update_ventana", postgres_conn_id=CONN,
                                 sql=path+"tt034_transacciones_de_cuentas_por_cobrar_update_ventana.sql")
        b23 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_update_orden", postgres_conn_id=CONN,
                                 sql=path+"tt035_transacciones_de_cuentas_por_cobrar_update_orden.sql")
        b24 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_update_fecha_de_vencimiento", postgres_conn_id=CONN,
                                 sql=path+"tt036_transacciones_de_cuentas_por_cobrar_update_fecha_de_vencimiento.sql")
        b25 = PostgresOperator(task_id="transacciones_de_cuentas_por_cobrar_update_balance", postgres_conn_id=CONN,
                                 sql=path+"tt037_transacciones_de_cuentas_por_cobrar_update_balance.sql")
        b26 = PostgresOperator(task_id="metas_de_vendedor", postgres_conn_id=CONN,
                                 sql=path+"tt040_metas_de_vendedor.sql")
        b27 = PostgresOperator(task_id="metas_de_area", postgres_conn_id=CONN,
                                 sql=path+"tt041_metas_de_area.sql")
        b28 = PostgresOperator(task_id="transacciones_de_compra_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt114_transacciones_de_compra_truncate.sql")
        b29 = PostgresOperator(task_id="transacciones_de_compra_insert_compras", postgres_conn_id=CONN,
                                 sql=path+"tt115_transacciones_de_compra_insert_compras.sql")
        b30 = PostgresOperator(task_id="transacciones_de_compra_insert_cancelaciones_de_compras", postgres_conn_id=CONN,
                                 sql=path+"tt116_transacciones_de_compra_insert_cancelaciones_de_compras.sql")
        b31 = PostgresOperator(task_id="transacciones_de_compra_insert_recepciones", postgres_conn_id=CONN,
                                 sql=path+"tt117_transacciones_de_compra_insert_recepciones.sql")
        b32 = PostgresOperator(task_id="transacciones_de_compra_insert_cancelaciones_de_recepciones", postgres_conn_id=CONN,
                                 sql=path+"tt118_transacciones_de_compra_insert_cancelaciones_de_recepciones.sql")
        b33 = PostgresOperator(task_id="transacciones_de_compra_insert_devoluciones", postgres_conn_id=CONN,
                                 sql=path+"tt119_transacciones_de_compra_insert_devoluciones.sql")
        b34 = PostgresOperator(task_id="transacciones_de_compra_insert_cancelaciones_de_devoluciones", postgres_conn_id=CONN,
                                 sql=path+"tt120_transacciones_de_compra_insert_cancelaciones_de_devoluciones.sql")
        b35 = PostgresOperator(task_id="transacciones_de_cotizacion_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt140_transacciones_de_cotizacion_truncate.sql")
        b36 = PostgresOperator(task_id="transacciones_de_cotizacion_insert", postgres_conn_id=CONN,
                                 sql=path+"tt141_transacciones_de_cotizacion_insert.sql")
        b37 = PostgresOperator(task_id="transacciones_de_cotizacion_update_cotizacion", postgres_conn_id=CONN,
                                 sql=path+"tt142_transacciones_de_cotizacion_update_cotizacion.sql")
        b38 = PostgresOperator(task_id="transacciones_de_cotizacion_update_id_cliente", postgres_conn_id=CONN,
                                 sql=path+"tt143_transacciones_de_cotizacion_update_id_cliente.sql")
        b39 = PostgresOperator(task_id="transacciones_de_cotizacion_update_id_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt144_transacciones_de_cotizacion_update_id_almacen.sql")
        b40 = PostgresOperator(task_id="transacciones_de_cotizacion_update_id_vendedor", postgres_conn_id=CONN,
                                 sql=path+"tt145_transacciones_de_cotizacion_update_id_vendedor.sql")
        b41 = PostgresOperator(task_id="transacciones_de_cotizacion_update_id_articulo", postgres_conn_id=CONN,
                                 sql=path+"tt146_transacciones_de_cotizacion_update_id_articulo.sql")

        b0 >> b1 >> b2 >> b3 >> b4 >> b5 >> b6 >> b7 >> b8 >> b9
        b9 >> b10 >> b11 >> b12 >> b13 >> b14 >> b15 >> b16 >> b17 >> b18
        b18 >> b19 >> b20 >> b21 >> b22 >> b23 >> b24 >> b25 >> b26 >> b27
        b27 >> b28 >> b29 >> b30 >> b31 >> b32 >> b33 >> b34 >> b35 >> b36
        b36 >> b37 >> b38 >> b39 >> b40 >> b41
   # End Task Group definition

    # Start Task Group Definition
    path = 'aquasistemas-sql/sql/2_tablas_resumidas/'
    with TaskGroup(group_id='2_tablas_resumidas') as tg3:
        c0 = PostgresOperator(task_id="resumen_de_ventas_mensuales_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr001_resumen_de_ventas_mensuales_truncate.sql")
        c1 = PostgresOperator(task_id="resumen_de_ventas_mensuales_insert_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr002_resumen_de_ventas_mensuales_insert_inicial.sql")
        c2 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_ventas_1", postgres_conn_id=CONN,
                                 sql=path+"tr003_resumen_de_ventas_mensuales_update_ventas_1.sql")
        c3 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_ventas_2", postgres_conn_id=CONN,
                                 sql=path+"tr004_resumen_de_ventas_mensuales_update_ventas_2.sql")
        c4 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_ventas_3", postgres_conn_id=CONN,
                                 sql=path+"tr005_resumen_de_ventas_mensuales_update_ventas_3.sql")
        c5 = PostgresOperator(task_id="resumen_de_ventas_mensuales_meta_vendedor", postgres_conn_id=CONN,
                                 sql=path+"tr006_resumen_de_ventas_mensuales_meta_vendedor.sql")
        c6 = PostgresOperator(task_id="resumen_de_ventas_mensuales_meta_canal_de_venta", postgres_conn_id=CONN,
                                 sql=path+"tr007_resumen_de_ventas_mensuales_meta_canal_de_venta.sql")
        c7 = PostgresOperator(task_id="resumen_de_ventas_mensuales_cotizaciones_insert_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr008_resumen_de_ventas_mensuales_cotizaciones_insert_inicial.sql")
        c8 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_cotizaciones", postgres_conn_id=CONN,
                                 sql=path+"tr009_resumen_de_ventas_mensuales_update_cotizaciones.sql")
        c9 = PostgresOperator(task_id="resumen_de_ventas_mensuales_cotizaciones_insert_balanceador", postgres_conn_id=CONN,
                                 sql=path+"tr010_resumen_de_ventas_mensuales_cotizaciones_insert_balanceador.sql")
        c10 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_year_to_date_1", postgres_conn_id=CONN,
                                 sql=path+"tr011_resumen_de_ventas_mensuales_update_year_to_date_1.sql")
        c11 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_year_to_date_2", postgres_conn_id=CONN,
                                 sql=path+"tr012_resumen_de_ventas_mensuales_update_year_to_date_2.sql")
        c12 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_year_to_date_3", postgres_conn_id=CONN,
                                 sql=path+"tr013_resumen_de_ventas_mensuales_update_year_to_date_3.sql")
        c13 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr014_resumen_de_ventas_mensuales_update_year_to_year.sql")
        c14 = PostgresOperator(task_id="resumen_de_ventas_mensuales_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr015_resumen_de_ventas_mensuales_delete_registros_cero.sql")
        c15 = PostgresOperator(task_id="resumen_de_inventario_mensual_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr030_resumen_de_inventario_mensual_truncate.sql")
        c16 = PostgresOperator(task_id="resumen_de_inventario_mensual_insert", postgres_conn_id=CONN,
                                 sql=path+"tr031_resumen_de_inventario_mensual_insert.sql")
        c17 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_cierre", postgres_conn_id=CONN,
                                 sql=path+"tr032_resumen_de_inventario_mensual_update_cierre.sql")
        c18 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_ventana", postgres_conn_id=CONN,
                                 sql=path+"tr033_resumen_de_inventario_mensual_update_ventana.sql")
        c19 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_calculos_de_ventana", postgres_conn_id=CONN,
                                 sql=path+"tr034_resumen_de_inventario_mensual_update_calculos_de_ventana.sql")
        c20 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_metricas", postgres_conn_id=CONN,
                                 sql=path+"tr035_resumen_de_inventario_mensual_update_metricas.sql")
        c21 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_anterior", postgres_conn_id=CONN,
                                 sql=path+"tr036_resumen_de_inventario_mensuales_update_anterior.sql")
        c22 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_dias_sin_inventario", postgres_conn_id=CONN,
                                 sql=path+"tr037_resumen_de_inventario_mensuales_update_dias_sin_inventario.sql")
        c23 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_metricas_a_la_fecha", postgres_conn_id=CONN,
                                 sql=path+"tr038_resumen_de_inventario_mensuales_update_metricas_a_la_fecha.sql")
        c24 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_productos_sin_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tr039_resumen_de_inventario_mensuales_update_productos_sin_movimiento.sql")
        c25 = PostgresOperator(task_id="resumen_de_cuentas_por_cobrar_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr040_resumen_de_cuentas_por_cobrar_truncate.sql")
        c26 = PostgresOperator(task_id="resumen_de_cuentas_por_cobrar_insert_saldo_a_favor", postgres_conn_id=CONN,
                                 sql=path+"tr041_resumen_de_cuentas_por_cobrar_insert_saldo_a_favor.sql")
        c27 = PostgresOperator(task_id="resumen_de_cuentas_por_cobrar_insert_cobros", postgres_conn_id=CONN,
                                 sql=path+"tr042_resumen_de_cuentas_por_cobrar_insert_cobros.sql")
        c28 = PostgresOperator(task_id="resumen_de_cuentas_por_cobrar_update_orden", postgres_conn_id=CONN,
                                 sql=path+"tr043_resumen_de_cuentas_por_cobrar_update_orden.sql")
        c29 = PostgresOperator(task_id="resumen_de_cuentas_por_cobrar_update_calculos", postgres_conn_id=CONN,
                                 sql=path+"tr044_resumen_de_cuentas_por_cobrar_update_calculos.sql")

        c0 >> c1 >> c2 >> c3 >> c4 >> c5 >> c6 >> c7 >> c8 >> c9
        c9 >> c10 >> c11 >> c12 >> c13 >> c14 >> c15 >> c16 >> c17 >> c18
        c18 >> c19 >> c20 >> c21 >> c22 >> c23 >> c24 >> c25 >> c26 >> c27
        c27 >> c28 >> c29
   # End Task Group definition

    # Start Task Group Definition
    path = 'aquasistemas-sql/sql/3_vistas_materializadas/'
    with TaskGroup(group_id='3_vistas_materializadas') as tg4:
        d0 = PostgresOperator(task_id="refresh_gv_articulo", postgres_conn_id=CONN,
                                 sql=path+"vm001_refresh_gv_articulo.sql")
        d1 = PostgresOperator(task_id="refresh_gv_inventario", postgres_conn_id=CONN,
                                 sql=path+"vm003_refresh_gv_inventario.sql")
        d2 = PostgresOperator(task_id="refresh_gv_ventas", postgres_conn_id=CONN,
                                 sql=path+"vm004_refresh_gv_ventas.sql")
        d3 = PostgresOperator(task_id="refresh_gv_cuentas_por_cobrar", postgres_conn_id=CONN,
                                 sql=path+"vm005_refresh_gv_cuentas_por_cobrar.sql")
        d4 = PostgresOperator(task_id="transacciones_de_venta", postgres_conn_id=CONN,
                                 sql=path+"vm005_transacciones_de_venta.sql")
        d5 = PostgresOperator(task_id="transacciones_de_compra_detallada", postgres_conn_id=CONN,
                                 sql=path+"vm006_transacciones_de_compra_detallada.sql")
        d6 = PostgresOperator(task_id="transacciones_de_inventario_detallado", postgres_conn_id=CONN,
                                 sql=path+"vm007_transacciones_de_inventario_detallado.sql")
        d7 = PostgresOperator(task_id="refresh__notificacion__", postgres_conn_id=CONN,
                                 sql=path+"vm008_refresh__notificacion__.sql")
        d8 = PostgresOperator(task_id="refresh__tabla_resumida_de_ventas", postgres_conn_id=CONN,
                                 sql=path+"vm009_refresh__tabla_resumida_de_ventas.sql")
        d9 = PostgresOperator(task_id="refresh_tabla_de_rotaciones_para_marketing", postgres_conn_id=CONN,
                                 sql=path+"vm010_refresh_tabla_de_rotaciones_para_marketing.sql")
        d10 = PostgresOperator(task_id="refresh_tabla_de_metas_por_vendedor", postgres_conn_id=CONN,
                                 sql=path+"vm011_refresh_tabla_de_metas_por_vendedor.sql")
        d11 = PostgresOperator(task_id="refresh_tabla_de_distribuidores", postgres_conn_id=CONN,
                                 sql=path+"vm012_refresh_tabla_de_distribuidores.sql")
        d12 = PostgresOperator(task_id="refresh_vista_fuerza_de_venta", postgres_conn_id=CONN,
                                 sql=path+"vm013_refresh_vista_fuerza_de_venta.sql")
        d13 = PostgresOperator(task_id="refresh_vista_fuerza_de_venta_detalle", postgres_conn_id=CONN,
                                 sql=path+"vm014_refresh_vista_fuerza_de_venta_detalle.sql")

        d0 >> d1 >> d2 >> d3 >> d4 >> d5 >> d6 >> d7 >> d8 >> d9
        d9 >> d10 >> d11 >> d12 >> d13
   # End Task Group definition

    tg1 >> tg2 >> tg3 >> tg4