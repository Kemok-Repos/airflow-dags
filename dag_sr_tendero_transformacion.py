from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

CONN = 'sr_tendero_postgres'

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
  dag_id="transformacion-sr-tendero",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso asincrono
  schedule_interval='0 6 * * *', #UTC
  catchup=False,
  tags=['sr-tendero','transformacion'],
) as dag:
    # Start Task Group Definition
    path = 'sr-tendero-sql/sql/0_tablas_maestras/'
    with TaskGroup(group_id='0_tablas_maestras') as tg1:
        a0 = PostgresOperator(task_id="upsert_tipos_de_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tm001_upsert_tipos_de_movimiento.sql")
        a1 = PostgresOperator(task_id="upsert_almacenes", postgres_conn_id=CONN,
                                 sql=path+"tm002_upsert_almacenes.sql")
        a2 = PostgresOperator(task_id="upsert_almacenesdb", postgres_conn_id=CONN,
                                 sql=path+"tm003_upsert_almacenes - db.sql")
        a3 = PostgresOperator(task_id="upsert_bodegas", postgres_conn_id=CONN,
                                 sql=path+"tm004_upsert_bodegas.sql")
        a4 = PostgresOperator(task_id="upsert_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm005_upsert_articulos.sql")
        a5 = PostgresOperator(task_id="update_agrupacion_almacenes", postgres_conn_id=CONN,
                                 sql=path+"tm011_update_agrupacion_almacenes.sql")
        a6 = PostgresOperator(task_id="update_agrupacion_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm012_update_agrupacion_articulos.sql")
        a7 = PostgresOperator(task_id="update_auxiliar_categorizacion", postgres_conn_id=CONN,
                                 sql=path+"tm013_update_auxiliar_categorizacion.sql")
        a8 = PostgresOperator(task_id="update_tiempo_de_surtido", postgres_conn_id=CONN,
                                 sql=path+"tm014_update_tiempo_de_surtido.sql")
        a9 = PostgresOperator(task_id="update_agrupacion_proveedores", postgres_conn_id=CONN,
                                 sql=path+"tm015_update_agrupacion_proveedores.sql")
        a10 = PostgresOperator(task_id="update_auxiliar_fabricantes", postgres_conn_id=CONN,
                                 sql=path+"tm016_update_auxiliar_fabricantes.sql")
        a11 = PostgresOperator(task_id="update_auxiliar_costo_unitario", postgres_conn_id=CONN,
                                 sql=path+"tm017_update_auxiliar_costo_unitario.sql")
        a12 = PostgresOperator(task_id="update_auxiliar_costo_unitario_promedio", postgres_conn_id=CONN,
                                 sql=path+"tm018_update_auxiliar_costo_unitario_promedio.sql")
        a13 = PostgresOperator(task_id="auxiliar_precios_truncate", postgres_conn_id=CONN,
                                 sql=path+"tm020_auxiliar_precios_truncate.sql")
        a14 = PostgresOperator(task_id="auxiliar_precios_update_precios", postgres_conn_id=CONN,
                                 sql=path+"tm021_auxiliar_precios_update_precios.sql")
        a15 = PostgresOperator(task_id="auxiliar_precios_upsert_almacenes", postgres_conn_id=CONN,
                                 sql=path+"tm022_auxiliar_precios_upsert_almacenes.sql")
        a16 = PostgresOperator(task_id="auxiliar_precios_upsert_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm023_auxiliar_precios_upsert_articulos.sql")
        a17 = PostgresOperator(task_id="upsert_clientes", postgres_conn_id=CONN,
                                 sql=path+"tm100_upsert_clientes.sql")
        a18 = PostgresOperator(task_id="upsert_proveedores", postgres_conn_id=CONN,
                                 sql=path+"tm100_upsert_proveedores.sql")
        a19 = PostgresOperator(task_id="upsert_vendedores", postgres_conn_id=CONN,
                                 sql=path+"tm100_upsert_vendedores.sql")

        a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> a6 >> a7 >> a8 >> a9
        a9 >> a10 >> a11 >> a12 >> a13 >> a14 >> a15 >> a16 >> a17 >> a18
        a18 >> a19
   # End Task Group definition

    # Start Task Group Definition
    path = 'sr-tendero-sql/sql/1_tablas_transaccionales/'
    with TaskGroup(group_id='1_tablas_transaccionales') as tg2:
        b0 = PostgresOperator(task_id="auxiliar_venta_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt001_auxiliar_venta_truncate.sql")
        b1 = PostgresOperator(task_id="auxiliar_venta_insert_facturas", postgres_conn_id=CONN,
                                 sql=path+"tt002_auxiliar_venta_insert_facturas.sql")
        b2 = PostgresOperator(task_id="auxiliar_venta_update_id_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt005_auxiliar_venta_update_id_almacen.sql")
        b3 = PostgresOperator(task_id="auxiliar_venta_update_productomedida", postgres_conn_id=CONN,
                                 sql=path+"tt006_auxiliar_venta_update_productomedida.sql")
        b4 = PostgresOperator(task_id="auxiliar_venta_update_id_articulo", postgres_conn_id=CONN,
                                 sql=path+"tt007_auxiliar_venta_update_id_articulo.sql")
        b5 = PostgresOperator(task_id="transacciones_de_venta_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt009_transacciones_de_venta_truncate.sql")
        b6 = PostgresOperator(task_id="transacciones_de_venta_insert_venta1", postgres_conn_id=CONN,
                                 sql=path+"tt010_transacciones_de_venta_insert_venta - 1.sql")
        b7 = PostgresOperator(task_id="transacciones_de_venta_insert_venta2", postgres_conn_id=CONN,
                                 sql=path+"tt011_transacciones_de_venta_insert_venta - 2.sql")
        b8 = PostgresOperator(task_id="transacciones_de_venta_update_demografico", postgres_conn_id=CONN,
                                 sql=path+"tt012_transacciones_de_venta_update_demografico.sql")
        b9 = PostgresOperator(task_id="auxiliar_metas", postgres_conn_id=CONN,
                                 sql=path+"tt013_auxiliar_metas.sql")
        b10 = PostgresOperator(task_id="auxiliar_inventario_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt016_auxiliar_inventario_truncate.sql")
        b11 = PostgresOperator(task_id="auxiliar_inventario_insert_inventario", postgres_conn_id=CONN,
                                 sql=path+"tt017_auxiliar_inventario_insert_inventario.sql")
        b12 = PostgresOperator(task_id="auxiliar_inventario_update_stock", postgres_conn_id=CONN,
                                 sql=path+"tt018_auxiliar_inventario_update_stock.sql")
        b13 = PostgresOperator(task_id="auxiliar_inventario_update_lote", postgres_conn_id=CONN,
                                 sql=path+"tt019_auxiliar_inventario_update_lote.sql")
        b14 = PostgresOperator(task_id="auxiliar_inventario_update_inventariotipo", postgres_conn_id=CONN,
                                 sql=path+"tt020_auxiliar_inventario_update_inventariotipo.sql")
        b15 = PostgresOperator(task_id="auxiliar_inventario_update_articulo", postgres_conn_id=CONN,
                                 sql=path+"tt021_auxiliar_inventario_update_articulo.sql")
        b16 = PostgresOperator(task_id="auxiliar_inventario_update_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt022_auxiliar_inventario_update_almacen.sql")
        b17 = PostgresOperator(task_id="auxiliar_inventario_update_tipo_de_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tt023_auxiliar_inventario_update_tipo_de_movimiento.sql")
        b18 = PostgresOperator(task_id="transacciones_de_inventario_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt025_transacciones_de_inventario_truncate.sql")
        b19 = PostgresOperator(task_id="transacciones_de_venta_insert_inventario", postgres_conn_id=CONN,
                                 sql=path+"tt026_transacciones_de_venta_insert_inventario.sql")
        b20 = PostgresOperator(task_id="transacciones_de_inventario_update_orden_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tt027_transacciones_de_inventario_update_orden_movimiento.sql")
        b21 = PostgresOperator(task_id="transacciones_de_inventario_update_rastreo_movimientos", postgres_conn_id=CONN,
                                 sql=path+"tt028_transacciones_de_inventario_update_rastreo_movimientos.sql")
        b22 = PostgresOperator(task_id="transacciones_de_inventario_update_existencias_actuales", postgres_conn_id=CONN,
                                 sql=path+"tt029_transacciones_de_inventario_update_existencias_actuales.sql")
        b23 = PostgresOperator(task_id="transacciones_de_inventario_update_existencias_historicas", postgres_conn_id=CONN,
                                 sql=path+"tt030_transacciones_de_inventario_update_existencias_historicas.sql")
        b24 = PostgresOperator(task_id="transacciones_de_inventario_update_dias_sin_existencias", postgres_conn_id=CONN,
                                 sql=path+"tt031_transacciones_de_inventario_update_dias_sin_existencias.sql")
        b25 = PostgresOperator(task_id="auxiliar_compras_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt035_auxiliar_compras_truncate.sql")
        b26 = PostgresOperator(task_id="auxiliar_compras_insert_inventario", postgres_conn_id=CONN,
                                 sql=path+"tt036_auxiliar_compras_insert_inventario.sql")
        b27 = PostgresOperator(task_id="auxiliar_compras_update_stock", postgres_conn_id=CONN,
                                 sql=path+"tt037_auxiliar_compras_update_stock.sql")
        b28 = PostgresOperator(task_id="auxiliar_compras_update_lote", postgres_conn_id=CONN,
                                 sql=path+"tt038_auxiliar_compras_update_lote.sql")
        b29 = PostgresOperator(task_id="auxiliar_compras_update_articulo", postgres_conn_id=CONN,
                                 sql=path+"tt039_auxiliar_compras_update_articulo.sql")
        b30 = PostgresOperator(task_id="auxiliar_compras_update_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt040_auxiliar_compras_update_almacen.sql")
        b31 = PostgresOperator(task_id="auxiliar_compras_update_proveedor", postgres_conn_id=CONN,
                                 sql=path+"tt041_auxiliar_compras_update_proveedor.sql")
        b32 = PostgresOperator(task_id="transacciones_de_compra_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt042_transacciones_de_compra_truncate.sql")
        b33 = PostgresOperator(task_id="transacciones_de_compra_insert_compras", postgres_conn_id=CONN,
                                 sql=path+"tt043_transacciones_de_compra_insert_compras.sql")
        b34 = PostgresOperator(task_id="insert_auxiliar_homologacion_de_articulos", postgres_conn_id=CONN,
                                 sql=path+"tt044_insert_auxiliar_homologacion_de_articulos.sql")
        b35 = PostgresOperator(task_id="insert_auxiliar_clasificacion_abc", postgres_conn_id=CONN,
                                 sql=path+"tt045_insert_auxiliar_clasificacion_abc.sql")
        b36 = PostgresOperator(task_id="update_auxiliar_clasificacion_abc", postgres_conn_id=CONN,
                                 sql=path+"tt046_update_auxiliar_clasificacion_abc.sql")
        b37 = PostgresOperator(task_id="auxiliar_venta_insert_devoluciones", postgres_conn_id=CONN,
                                 sql=path+"tt100_auxiliar_venta_insert_devoluciones.sql")
        b38 = PostgresOperator(task_id="auxiliar_venta_update_id_categorizacion", postgres_conn_id=CONN,
                                 sql=path+"tt100_auxiliar_venta_update_id_categorizacion.sql")
        b39 = PostgresOperator(task_id="auxiliar_venta_update_id_cliente", postgres_conn_id=CONN,
                                 sql=path+"tt100_auxiliar_venta_update_id_cliente.sql")
        b40 = PostgresOperator(task_id="transacciones_de_venta_insert_anulaciones", postgres_conn_id=CONN,
                                 sql=path+"tt100_transacciones_de_venta_insert_anulaciones.sql")

        b0 >> b1 >> b2 >> b3 >> b4 >> b5 >> b6 >> b7 >> b8 >> b9
        b9 >> b10 >> b11 >> b12 >> b13 >> b14 >> b15 >> b16 >> b17 >> b18
        b18 >> b19 >> b20 >> b21 >> b22 >> b23 >> b24 >> b25 >> b26 >> b27
        b27 >> b28 >> b29 >> b30 >> b31 >> b32 >> b33 >> b34 >> b35 >> b36
        b36 >> b37 >> b38 >> b39 >> b40
   # End Task Group definition

    # Start Task Group Definition
    path = 'sr-tendero-sql/sql/2_tablas_resumidas/'
    with TaskGroup(group_id='2_tablas_resumidas') as tg3:
        c0 = PostgresOperator(task_id="resumen_de_ventas_mensuales_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr001_resumen_de_ventas_mensuales_truncate.sql")
        c1 = PostgresOperator(task_id="resumen_de_ventas_mensuales_insert_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr002_resumen_de_ventas_mensuales_insert_inicial.sql")
        c2 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr003_resumen_de_ventas_mensuales_update_ventas.sql")
        c3 = PostgresOperator(task_id="resumen_de_ventas_mensuales_insert_inicial_metas_y_balanceador", postgres_conn_id=CONN,
                                 sql=path+"tr004_resumen_de_ventas_mensuales_insert_inicial_metas_y_balanceador.sql")
        c4 = PostgresOperator(task_id="resumen_de_ventas_mensuales_insert_balanceador", postgres_conn_id=CONN,
                                 sql=path+"tr005_resumen_de_ventas_mensuales_insert_balanceador.sql")
        c5 = PostgresOperator(task_id="resumen_de_ventas_mensuales_insert_metas", postgres_conn_id=CONN,
                                 sql=path+"tr006_resumen_de_ventas_mensuales_insert_metas.sql")
        c6 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_year_to_date", postgres_conn_id=CONN,
                                 sql=path+"tr007_resumen_de_ventas_mensuales_update_year_to_date.sql")
        c7 = PostgresOperator(task_id="resumen_de_ventas_mensuales_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr008_resumen_de_ventas_mensuales_update_year_to_year.sql")
        c8 = PostgresOperator(task_id="resumen_de_ventas_mensuales_anterior", postgres_conn_id=CONN,
                                 sql=path+"tr009_resumen_de_ventas_mensuales_anterior.sql")
        c9 = PostgresOperator(task_id="resumen_de_ventas_mensuales_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr010_resumen_de_ventas_mensuales_delete_registros_cero.sql")
        c10 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr011_resumen_de_ventas_mensuales_por_tienda_truncate.sql")
        c11 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_insert_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr012_resumen_de_ventas_mensuales_por_tienda_insert_inicial.sql")
        c12 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_update_ventas", postgres_conn_id=CONN,
                                 sql=path+"tr013_resumen_de_ventas_mensuales_por_tienda_update_ventas.sql")
        c13 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_update_metas", postgres_conn_id=CONN,
                                 sql=path+"tr014_resumen_de_ventas_mensuales_por_tienda_update_metas.sql")
        c14 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_update_turno", postgres_conn_id=CONN,
                                 sql=path+"tr015_resumen_de_ventas_mensuales_por_tienda_update_turno.sql")
        c15 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_update_comparativo", postgres_conn_id=CONN,
                                 sql=path+"tr016_resumen_de_ventas_mensuales_por_tienda_update_comparativo.sql")
        c16 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_update_year_to_date", postgres_conn_id=CONN,
                                 sql=path+"tr017_resumen_de_ventas_mensuales_por_tienda_update_year_to_date.sql")
        c17 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_update_anterior", postgres_conn_id=CONN,
                                 sql=path+"tr018_resumen_de_ventas_mensuales_por_tienda_update_anterior.sql")
        c18 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr019_resumen_de_ventas_mensuales_por_tienda_update_year_to_year.sql")
        c19 = PostgresOperator(task_id="resumen_de_ventas_mensuales_por_tienda_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr020_resumen_de_ventas_mensuales_por_tienda_delete_registros_cero.sql")
        c20 = PostgresOperator(task_id="resumen_de_inventario_mensual_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr030_resumen_de_inventario_mensual_truncate.sql")
        c21 = PostgresOperator(task_id="resumen_de_inventario_mensual_insert", postgres_conn_id=CONN,
                                 sql=path+"tr031_resumen_de_inventario_mensual_insert.sql")
        c22 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_cierre", postgres_conn_id=CONN,
                                 sql=path+"tr032_resumen_de_inventario_mensual_update_cierre.sql")
        c23 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_ventana", postgres_conn_id=CONN,
                                 sql=path+"tr033_resumen_de_inventario_mensual_update_ventana.sql")
        c24 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_calculos_de_ventana", postgres_conn_id=CONN,
                                 sql=path+"tr034_resumen_de_inventario_mensual_update_calculos_de_ventana.sql")
        c25 = PostgresOperator(task_id="resumen_de_inventario_mensual_update_metricas", postgres_conn_id=CONN,
                                 sql=path+"tr035_resumen_de_inventario_mensual_update_metricas.sql")
        c26 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_anterior", postgres_conn_id=CONN,
                                 sql=path+"tr036_resumen_de_inventario_mensuales_update_anterior.sql")
        c27 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_dias_sin_inventario", postgres_conn_id=CONN,
                                 sql=path+"tr037_resumen_de_inventario_mensuales_update_dias_sin_inventario.sql")
        c28 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_articulos_sin_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tr038_resumen_de_inventario_mensuales_update_articulos_sin_movimiento.sql")
        c29 = PostgresOperator(task_id="resumen_de_inventario_mensuales_update_ultima_ventana_movil", postgres_conn_id=CONN,
                                 sql=path+"tr039_resumen_de_inventario_mensuales_update_ultima_ventana_movil.sql")
        c30 = PostgresOperator(task_id="resumen_de_compras_mensuales_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr040_resumen_de_compras_mensuales_truncate.sql")
        c31 = PostgresOperator(task_id="resumen_de_compras_mensuales_insert_inicial", postgres_conn_id=CONN,
                                 sql=path+"tr041_resumen_de_compras_mensuales_insert_inicial.sql")
        c32 = PostgresOperator(task_id="resumen_de_compras_mensuales_update_compras", postgres_conn_id=CONN,
                                 sql=path+"tr042_resumen_de_compras_mensuales_update_compras.sql")
        c33 = PostgresOperator(task_id="resumen_de_compras_mensuales_insert_ventas_e_inventario", postgres_conn_id=CONN,
                                 sql=path+"tr043_resumen_de_compras_mensuales_insert_ventas_e_inventario.sql")
        c34 = PostgresOperator(task_id="resumen_de_compras_mensuales_update_year_to_date", postgres_conn_id=CONN,
                                 sql=path+"tr044_resumen_de_compras_mensuales_update_year_to_date.sql")
        c35 = PostgresOperator(task_id="resumen_de_compras_mensuales_update_year_to_year", postgres_conn_id=CONN,
                                 sql=path+"tr045_resumen_de_compras_mensuales_update_year_to_year.sql")
        c36 = PostgresOperator(task_id="resumen_de_compras_mensuales_anterior", postgres_conn_id=CONN,
                                 sql=path+"tr046_resumen_de_compras_mensuales_anterior.sql")
        c37 = PostgresOperator(task_id="resumen_de_compras_mensuales_delete_registros_cero", postgres_conn_id=CONN,
                                 sql=path+"tr047_resumen_de_compras_mensuales_delete_registros_cero.sql")

        c0 >> c1 >> c2 >> c3 >> c4 >> c5 >> c6 >> c7 >> c8 >> c9
        c9 >> c10 >> c11 >> c12 >> c13 >> c14 >> c15 >> c16 >> c17 >> c18
        c18 >> c19 >> c20 >> c21 >> c22 >> c23 >> c24 >> c25 >> c26 >> c27
        c27 >> c28 >> c29 >> c30 >> c31 >> c32 >> c33 >> c34 >> c35 >> c36
        c36 >> c37
   # End Task Group definition

    # Start Task Group Definition
    path = 'sr-tendero-sql/sql/3_vistas_materializadas/'
    with TaskGroup(group_id='3_vistas_materializadas') as tg4:
        d0 = PostgresOperator(task_id="gv_ticket", postgres_conn_id=CONN,
                                 sql=path+"vm001_gv_ticket.sql")
        d1 = PostgresOperator(task_id="gv_basket_analysis", postgres_conn_id=CONN,
                                 sql=path+"vm002_gv_basket_analysis.sql")
        d2 = PostgresOperator(task_id="gv_ventas", postgres_conn_id=CONN,
                                 sql=path+"vm003_gv_ventas.sql")
        d3 = PostgresOperator(task_id="vista_reporte_diario", postgres_conn_id=CONN,
                                 sql=path+"vm004_vista_reporte_diario.sql")
        d4 = PostgresOperator(task_id="vista_reporte_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm005_vista_reporte_mensual.sql")
        d5 = PostgresOperator(task_id="vista_reporte_semanal", postgres_conn_id=CONN,
                                 sql=path+"vm006_vista_reporte_semanal.sql")
        d6 = PostgresOperator(task_id="vista_reporte_canasto_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm007_vista_reporte_canasto_mensual.sql")
        d7 = PostgresOperator(task_id="vista_de_ventas_diarias", postgres_conn_id=CONN,
                                 sql=path+"vm008_vista_de_ventas_diarias.sql")
        d8 = PostgresOperator(task_id="vista_venta_por_producto_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm009_vista_venta_por_producto_mensual.sql")
        d9 = PostgresOperator(task_id="notificaciones", postgres_conn_id=CONN,
                                 sql=path+"vm010_notificaciones.sql")
        d10 = PostgresOperator(task_id="vista_inventario_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm011_vista_inventario_mensual.sql")
        d11 = PostgresOperator(task_id="vista_compras_mensuales", postgres_conn_id=CONN,
                                 sql=path+"vm012_vista_compras_mensuales.sql")
        d12 = PostgresOperator(task_id="vista_gerencial", postgres_conn_id=CONN,
                                 sql=path+"vm013_vista_gerencial.sql")
        d13 = PostgresOperator(task_id="vista_venta_por_producto_mensual_por_hora", postgres_conn_id=CONN,
                                 sql=path+"vm014_vista_venta_por_producto_mensual_por_hora.sql")
        d14 = PostgresOperator(task_id="vista_venta_por_producto_mensual_por_dia", postgres_conn_id=CONN,
                                 sql=path+"vm015_vista_venta_por_producto_mensual_por_dia.sql")
        d15 = PostgresOperator(task_id="vista_proyecciones_de_compra", postgres_conn_id=CONN,
                                 sql=path+"vm016_vista_proyecciones_de_compra.sql")
        d16 = PostgresOperator(task_id="vista_transacciones_de_venta_detallada", postgres_conn_id=CONN,
                                 sql=path+"vm017_vista_transacciones_de_venta_detallada.sql")
        d17 = PostgresOperator(task_id="vista_transacciones_de_venta_detallada_demografico", postgres_conn_id=CONN,
                                 sql=path+"vm018_vista_transacciones_de_venta_detallada_demografico.sql")
        d18 = PostgresOperator(task_id="vista_analisis_de_tiendas", postgres_conn_id=CONN,
                                 sql=path+"vm019_vista_analisis_de_tiendas.sql")
        d19 = PostgresOperator(task_id="vista_compras_de_tiendas_diario", postgres_conn_id=CONN,
                                 sql=path+"vm020_vista_compras_de_tiendas_diario.sql")
        d20 = PostgresOperator(task_id="vista_analisis_de_tiendas_diario", postgres_conn_id=CONN,
                                 sql=path+"vm021_vista_analisis_de_tiendas_diario.sql")
        d21 = PostgresOperator(task_id="vista_horario_de_tiendas", postgres_conn_id=CONN,
                                 sql=path+"vm022_vista_horario_de_tiendas.sql")
        d22 = PostgresOperator(task_id="vista_analisis_de_precio", postgres_conn_id=CONN,
                                 sql=path+"vm023_vista_analisis_de_precio.sql")

        d0 >> d1 >> d2 >> d3 >> d4 >> d5 >> d6 >> d7 >> d8 >> d9
        d9 >> d10 >> d11 >> d12 >> d13 >> d14 >> d15 >> d16 >> d17 >> d18
        d18 >> d19 >> d20 >> d21 >> d22
   # End Task Group definition

    tg1 >> tg2 >> tg3 >> tg4