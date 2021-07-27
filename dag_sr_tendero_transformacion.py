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
    'sla': timedelta(minutes=120)    
}
with DAG(
  dag_id="transformacion-sr-tendero",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso manual
  schedule_interval=None, #UTC
  max_active_runs=1,
  catchup=False,
  tags=['sr-tendero','transformacion'],
) as dag:
    # Start Task Group Definition
    path = 'sr-tendero-sql/sql/0_tablas_maestras/'
    with TaskGroup(group_id='0_tablas_maestras') as tg1:
        a0 = PostgresOperator(task_id="upsert_tipos_de_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tm001_upsert_tipos_de_movimiento.sql")
        a1 = PostgresOperator(task_id="maestro_de_almacenes_update_gom", postgres_conn_id=CONN,
                                 sql=path+"tm002_maestro_de_almacenes_update_gom.sql")
        a2 = PostgresOperator(task_id="maestro_de_almacenes_update_pos", postgres_conn_id=CONN,
                                 sql=path+"tm003_maestro_de_almacenes_update_pos.sql")
        a3 = PostgresOperator(task_id="maestro_de_almacenes_update_g2s", postgres_conn_id=CONN,
                                 sql=path+"tm004_maestro_de_almacenes_update_g2s.sql")
        a4 = PostgresOperator(task_id="alias_de_almacenes_update_bodegas_gom", postgres_conn_id=CONN,
                                 sql=path+"tm005_alias_de_almacenes_update_bodegas_gom.sql")
        a5 = PostgresOperator(task_id="maestro_de_articulos_update_gom", postgres_conn_id=CONN,
                                 sql=path+"tm006_maestro_de_articulos_update_gom.sql")
        a6 = PostgresOperator(task_id="maestro_de_articulos_insert_gom", postgres_conn_id=CONN,
                                 sql=path+"tm007_maestro_de_articulos_insert_gom.sql")
        a7 = PostgresOperator(task_id="maestro_de_articulos_update_pos", postgres_conn_id=CONN,
                                 sql=path+"tm008_maestro_de_articulos_update_pos.sql")
        a8 = PostgresOperator(task_id="maestro_de_articulos_insert_pos", postgres_conn_id=CONN,
                                 sql=path+"tm009_maestro_de_articulos_insert_pos.sql")
        a9 = PostgresOperator(task_id="maestro_de_proveedores_update_gom", postgres_conn_id=CONN,
                                 sql=path+"tm010_maestro_de_proveedores_update_gom.sql")
        a10 = PostgresOperator(task_id="maestro_de_proveedores_update_pos", postgres_conn_id=CONN,
                                 sql=path+"tm011_maestro_de_proveedores_update_pos.sql")
        a11 = PostgresOperator(task_id="auxiliar_agrupacion_de_almacenes_truncate", postgres_conn_id=CONN,
                                 sql=path+"tm012_auxiliar_agrupacion_de_almacenes_truncate.sql")
        a12 = PostgresOperator(task_id="auxiliar_agrupacion_de_almacenes_insert_gom", postgres_conn_id=CONN,
                                 sql=path+"tm013_auxiliar_agrupacion_de_almacenes_insert_gom.sql")
        a13 = PostgresOperator(task_id="auxiliar_agrupacion_de_almacenes_insert_pos", postgres_conn_id=CONN,
                                 sql=path+"tm014_auxiliar_agrupacion_de_almacenes_insert_pos.sql")
        a14 = PostgresOperator(task_id="auxiliar_agrupacion_de_almacenes_prepara_maestro", postgres_conn_id=CONN,
                                 sql=path+"tm015_auxiliar_agrupacion_de_almacenes_prepara_maestro.sql")
        a15 = PostgresOperator(task_id="auxiliar_agrupacion_de_almacenes_update_maestro", postgres_conn_id=CONN,
                                 sql=path+"tm016_auxiliar_agrupacion_de_almacenes_update_maestro.sql")
        a16 = PostgresOperator(task_id="auxiliar_agrupacion_de_articulos_truncate", postgres_conn_id=CONN,
                                 sql=path+"tm017_auxiliar_agrupacion_de_articulos_truncate.sql")
        a17 = PostgresOperator(task_id="auxiliar_agrupacion_de_articulos_insert_gom", postgres_conn_id=CONN,
                                 sql=path+"tm018_auxiliar_agrupacion_de_articulos_insert_gom.sql")
        a18 = PostgresOperator(task_id="auxiliar_agrupacion_de_articulos_insert_pos", postgres_conn_id=CONN,
                                 sql=path+"tm019_auxiliar_agrupacion_de_articulos_insert_pos.sql")
        a19 = PostgresOperator(task_id="auxiliar_agrupacion_de_articulos_prepara_maestro", postgres_conn_id=CONN,
                                 sql=path+"tm020_auxiliar_agrupacion_de_articulos_prepara_maestro.sql")
        a20 = PostgresOperator(task_id="auxiliar_agrupacion_de_articulos_update_maestro", postgres_conn_id=CONN,
                                 sql=path+"tm021_auxiliar_agrupacion_de_articulos_update_maestro.sql")
        a21 = PostgresOperator(task_id="auxiliar_agrupacion_de_locaciones_update_pos", postgres_conn_id=CONN,
                                 sql=path+"tm022_auxiliar_agrupacion_de_locaciones_update_pos.sql")
        a22 = PostgresOperator(task_id="alias_de_almacenes_homologacion", postgres_conn_id=CONN,
                                 sql=path+"tm023_alias_de_almacenes_homologacion.sql")
        a23 = PostgresOperator(task_id="auxiliar_fabricantes_insert", postgres_conn_id=CONN,
                                 sql=path+"tm030_auxiliar_fabricantes_insert.sql")
        a24 = PostgresOperator(task_id="auxiliar_costo_unitario_update_gom", postgres_conn_id=CONN,
                                 sql=path+"tm031_auxiliar_costo_unitario_update_gom.sql")
        a25 = PostgresOperator(task_id="auxiliar_costo_unitario_update_promedio", postgres_conn_id=CONN,
                                 sql=path+"tm033_auxiliar_costo_unitario_update_promedio.sql")
        a26 = PostgresOperator(task_id="auxiliar_precios_truncate", postgres_conn_id=CONN,
                                 sql=path+"tm040_auxiliar_precios_truncate.sql")
        a27 = PostgresOperator(task_id="auxiliar_precios_update_precios", postgres_conn_id=CONN,
                                 sql=path+"tm041_auxiliar_precios_update_precios.sql")
        a28 = PostgresOperator(task_id="auxiliar_precios_upsert_almacenes", postgres_conn_id=CONN,
                                 sql=path+"tm042_auxiliar_precios_upsert_almacenes.sql")
        a29 = PostgresOperator(task_id="auxiliar_precios_upsert_articulos", postgres_conn_id=CONN,
                                 sql=path+"tm043_auxiliar_precios_upsert_articulos.sql")

        a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> a6 >> a7 >> a8 >> a9
        a9 >> a10 >> a11 >> a12 >> a13 >> a14 >> a15 >> a16 >> a17 >> a18
        a18 >> a19 >> a20 >> a21 >> a22 >> a23 >> a24 >> a25 >> a26 >> a27
        a27 >> a28 >> a29
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
        b5 = PostgresOperator(task_id="auxiliar_venta_pos_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt008_auxiliar_venta_pos_truncate.sql")
        b6 = PostgresOperator(task_id="auxiliar_venta_pos_insert", postgres_conn_id=CONN,
                                 sql=path+"tt009_auxiliar_venta_pos_insert.sql")
        b7 = PostgresOperator(task_id="auxiliar_venta_pos_update_presenciones", postgres_conn_id=CONN,
                                 sql=path+"tt010_auxiliar_venta_pos_update_presenciones.sql")
        b8 = PostgresOperator(task_id="auxiliar_venta_pos_update_id_articulos", postgres_conn_id=CONN,
                                 sql=path+"tt011_auxiliar_venta_pos_update_id_articulos.sql")
        b9 = PostgresOperator(task_id="auxiliar_venta_pos_update_id_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt012_auxiliar_venta_pos_update_id_almacen.sql")
        b10 = PostgresOperator(task_id="auxiliar_venta_pos_update_cost", postgres_conn_id=CONN,
                                 sql=path+"tt013_auxiliar_venta_pos_update_cost.sql")
        b11 = PostgresOperator(task_id="auxiliar_venta_pos_update_stock_movement_id", postgres_conn_id=CONN,
                                 sql=path+"tt013_auxiliar_venta_pos_update_stock_movement_id.sql")
        b12 = PostgresOperator(task_id="transacciones_de_venta_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt018_transacciones_de_venta_truncate.sql")
        b13 = PostgresOperator(task_id="transacciones_de_venta_insert_venta_gom_anterior", postgres_conn_id=CONN,
                                 sql=path+"tt019_transacciones_de_venta_insert_venta_gom_anterior.sql")
        b14 = PostgresOperator(task_id="transacciones_de_venta_insert_venta_gom_actual", postgres_conn_id=CONN,
                                 sql=path+"tt020_transacciones_de_venta_insert_venta_gom_actual.sql")
        b15 = PostgresOperator(task_id="transacciones_de_venta_insert_venta_pos_anterior", postgres_conn_id=CONN,
                                 sql=path+"tt021_transacciones_de_venta_insert_venta_pos_anterior.sql")
        b16 = PostgresOperator(task_id="transacciones_de_venta_insert_venta_pos_actual", postgres_conn_id=CONN,
                                 sql=path+"tt022_transacciones_de_venta_insert_venta_pos_actual.sql")
        b17 = PostgresOperator(task_id="transacciones_de_venta_update_demografico", postgres_conn_id=CONN,
                                 sql=path+"tt023_transacciones_de_venta_update_demografico.sql")
        b18 = PostgresOperator(task_id="auxiliar_metas_gom", postgres_conn_id=CONN,
                                 sql=path+"tt024_auxiliar_metas_gom.sql")
        b19 = PostgresOperator(task_id="auxiliar_metas_pos", postgres_conn_id=CONN,
                                 sql=path+"tt025_auxiliar_metas_pos.sql")
        b20 = PostgresOperator(task_id="auxiliar_inventario_gom_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt026_auxiliar_inventario_gom_truncate.sql")
        b21 = PostgresOperator(task_id="auxiliar_inventario_gom_insert_inventario", postgres_conn_id=CONN,
                                 sql=path+"tt027_auxiliar_inventario_gom_insert_inventario.sql")
        b22 = PostgresOperator(task_id="auxiliar_inventario_gom_update_stock", postgres_conn_id=CONN,
                                 sql=path+"tt028_auxiliar_inventario_gom_update_stock.sql")
        b23 = PostgresOperator(task_id="auxiliar_inventario_gom_update_lote", postgres_conn_id=CONN,
                                 sql=path+"tt029_auxiliar_inventario_gom_update_lote.sql")
        b24 = PostgresOperator(task_id="auxiliar_inventario_gom_update_inventariotipo", postgres_conn_id=CONN,
                                 sql=path+"tt030_auxiliar_inventario_gom_update_inventariotipo.sql")
        b25 = PostgresOperator(task_id="auxiliar_inventario_gom_update_articulo", postgres_conn_id=CONN,
                                 sql=path+"tt031_auxiliar_inventario_gom_update_articulo.sql")
        b26 = PostgresOperator(task_id="auxiliar_inventario_gom_update_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt032_auxiliar_inventario_gom_update_almacen.sql")
        b27 = PostgresOperator(task_id="auxiliar_inventario_gom_update_tipo_de_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tt033_auxiliar_inventario_gom_update_tipo_de_movimiento.sql")
        b28 = PostgresOperator(task_id="auxiliar_inventario_gom_update_ultima_transaccion", postgres_conn_id=CONN,
                                 sql=path+"tt034_auxiliar_inventario_gom_update_ultima_transaccion.sql")
        b29 = PostgresOperator(task_id="auxiliar_inventario_gom_actual_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt035_auxiliar_inventario_gom_actual_truncate.sql")
        b30 = PostgresOperator(task_id="auxiliar_inventario_gom_actual_insert_stock", postgres_conn_id=CONN,
                                 sql=path+"tt036_auxiliar_inventario_gom_actual_insert_stock.sql")
        b31 = PostgresOperator(task_id="auxiliar_inventario_gom_actual_update_ubicacion", postgres_conn_id=CONN,
                                 sql=path+"tt037_auxiliar_inventario_gom_actual_update_ubicacion.sql")
        b32 = PostgresOperator(task_id="auxiliar_inventario_gom_actual_update_lote", postgres_conn_id=CONN,
                                 sql=path+"tt038_auxiliar_inventario_gom_actual_update_lote.sql")
        b33 = PostgresOperator(task_id="auxiliar_inventario_gom_actual_update_producto", postgres_conn_id=CONN,
                                 sql=path+"tt039_auxiliar_inventario_gom_actual_update_producto.sql")
        b34 = PostgresOperator(task_id="auxiliar_inventario_gom_actual_update_alias_almacenes", postgres_conn_id=CONN,
                                 sql=path+"tt040_auxiliar_inventario_gom_actual_update_alias_almacenes.sql")
        b35 = PostgresOperator(task_id="auxiliar_inventario_gom_actual_update_alias_articulos", postgres_conn_id=CONN,
                                 sql=path+"tt041_auxiliar_inventario_gom_actual_update_alias_articulos.sql")
        b36 = PostgresOperator(task_id="auxiliar_inventario_pos_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt042_auxiliar_inventario_pos_truncate.sql")
        b37 = PostgresOperator(task_id="auxiliar_inventario_pos_insert", postgres_conn_id=CONN,
                                 sql=path+"tt043_auxiliar_inventario_pos_insert.sql")
        b38 = PostgresOperator(task_id="auxiliar_inventario_pos_update_articulo", postgres_conn_id=CONN,
                                 sql=path+"tt044_auxiliar_inventario_pos_update_articulo.sql")
        b39 = PostgresOperator(task_id="auxiliar_inventario_pos_update_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt045_auxiliar_inventario_pos_update_almacen.sql")
        b40 = PostgresOperator(task_id="auxiliar_inventario_pos_update_tipo_de_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tt046_auxiliar_inventario_pos_update_tipo_de_movimiento.sql")
        b41 = PostgresOperator(task_id="auxiliar_inventario_pos_update_ultima_transaccion", postgres_conn_id=CONN,
                                 sql=path+"tt047_auxiliar_inventario_pos_update_ultima_transaccion.sql")
        b42 = PostgresOperator(task_id="transacciones_de_inventario_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt050_transacciones_de_inventario_truncate.sql")
        b43 = PostgresOperator(task_id="transacciones_de_inventario_drop_index", postgres_conn_id=CONN,
                                 sql=path+"tt051_transacciones_de_inventario_drop_index.sql")
        b44 = PostgresOperator(task_id="transacciones_de_inventario_insert_inventario_gom_transacciones_pasadas", postgres_conn_id=CONN,
                                 sql=path+"tt052_transacciones_de_inventario_insert_inventario_gom_transacciones_pasadas.sql")
        b45 = PostgresOperator(task_id="transacciones_de_inventario_insert_inventario_gom_ultimas_transacciones", postgres_conn_id=CONN,
                                 sql=path+"tt053_transacciones_de_inventario_insert_inventario_gom_ultimas_transacciones.sql")
        b46 = PostgresOperator(task_id="transacciones_de_inventario_insert_inventario_pos_transacciones_pasadas", postgres_conn_id=CONN,
                                 sql=path+"tt054_transacciones_de_inventario_insert_inventario_pos_transacciones_pasadas.sql")
        b47 = PostgresOperator(task_id="transacciones_de_inventario_insert_inventario_pos_ultimas_transacciones", postgres_conn_id=CONN,
                                 sql=path+"tt055_transacciones_de_inventario_insert_inventario_pos_ultimas_transacciones.sql")
        b48 = PostgresOperator(task_id="transacciones_de_inventario_update_orden_movimiento", postgres_conn_id=CONN,
                                 sql=path+"tt056_transacciones_de_inventario_update_orden_movimiento.sql")
        b49 = PostgresOperator(task_id="transacciones_de_inventario_create_index", postgres_conn_id=CONN,
                                 sql=path+"tt057_transacciones_de_inventario_create_index.sql")
        b50 = PostgresOperator(task_id="transacciones_de_inventario_update_rastreo_movimientos", postgres_conn_id=CONN,
                                 sql=path+"tt058_transacciones_de_inventario_update_rastreo_movimientos.sql")
        b51 = PostgresOperator(task_id="transacciones_de_inventario_update_primer_costo", postgres_conn_id=CONN,
                                 sql=path+"tt059_transacciones_de_inventario_update_primer_costo.sql")
        b52 = PostgresOperator(task_id="transacciones_de_inventario_update_costo_ventana", postgres_conn_id=CONN,
                                 sql=path+"tt060_transacciones_de_inventario_update_costo_ventana.sql")
        b53 = PostgresOperator(task_id="transacciones_de_inventario_update_existencias_actuales_gom", postgres_conn_id=CONN,
                                 sql=path+"tt061_transacciones_de_inventario_update_existencias_actuales_gom.sql")
        b54 = PostgresOperator(task_id="transacciones_de_inventario_update_existencias_actuales_pos", postgres_conn_id=CONN,
                                 sql=path+"tt062_transacciones_de_inventario_update_existencias_actuales_pos.sql")
        b55 = PostgresOperator(task_id="transacciones_de_inventario_update_existencias_historicas", postgres_conn_id=CONN,
                                 sql=path+"tt063_transacciones_de_inventario_update_existencias_historicas.sql")
        b56 = PostgresOperator(task_id="transacciones_de_inventario_update_dias_sin_existencias", postgres_conn_id=CONN,
                                 sql=path+"tt064_transacciones_de_inventario_update_dias_sin_existencias.sql")
        b57 = PostgresOperator(task_id="auxiliar_compras_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt065_auxiliar_compras_truncate.sql")
        b58 = PostgresOperator(task_id="auxiliar_compras_insert_inventario", postgres_conn_id=CONN,
                                 sql=path+"tt066_auxiliar_compras_insert_inventario.sql")
        b59 = PostgresOperator(task_id="auxiliar_compras_update_stock", postgres_conn_id=CONN,
                                 sql=path+"tt067_auxiliar_compras_update_stock.sql")
        b60 = PostgresOperator(task_id="auxiliar_compras_update_lote", postgres_conn_id=CONN,
                                 sql=path+"tt068_auxiliar_compras_update_lote.sql")
        b61 = PostgresOperator(task_id="auxiliar_compras_update_articulo", postgres_conn_id=CONN,
                                 sql=path+"tt069_auxiliar_compras_update_articulo.sql")
        b62 = PostgresOperator(task_id="auxiliar_compras_update_almacen", postgres_conn_id=CONN,
                                 sql=path+"tt070_auxiliar_compras_update_almacen.sql")
        b63 = PostgresOperator(task_id="auxiliar_compras_update_proveedor", postgres_conn_id=CONN,
                                 sql=path+"tt071_auxiliar_compras_update_proveedor.sql")
        b64 = PostgresOperator(task_id="transacciones_de_compra_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt072_transacciones_de_compra_truncate.sql")
        b65 = PostgresOperator(task_id="transacciones_de_compra_insert_compras_gom", postgres_conn_id=CONN,
                                 sql=path+"tt073_transacciones_de_compra_insert_compras_gom.sql")
        b66 = PostgresOperator(task_id="transacciones_de_compra_insert_compras_pos", postgres_conn_id=CONN,
                                 sql=path+"tt074_transacciones_de_compra_insert_compras_pos.sql")
        b67 = PostgresOperator(task_id="auxiliar_homologacion_de_articulos_insert", postgres_conn_id=CONN,
                                 sql=path+"tt080_auxiliar_homologacion_de_articulos_insert.sql")
        b68 = PostgresOperator(task_id="auxiliar_clasificacion_abc_insert", postgres_conn_id=CONN,
                                 sql=path+"tt081_auxiliar_clasificacion_abc_insert.sql")
        b69 = PostgresOperator(task_id="auxiliar_clasificacion_abc_update", postgres_conn_id=CONN,
                                 sql=path+"tt082_auxiliar_clasificacion_abc_update.sql")

        b0 >> b1 >> b2 >> b3 >> b4 >> b5 >> b6 >> b7 >> b8 >> b9
        b9 >> b10 >> b11 >> b12 >> b13 >> b14 >> b15 >> b16 >> b17 >> b18
        b18 >> b19 >> b20 >> b21 >> b22 >> b23 >> b24 >> b25 >> b26 >> b27
        b27 >> b28 >> b29 >> b30 >> b31 >> b32 >> b33 >> b34 >> b35 >> b36
        b36 >> b37 >> b38 >> b39 >> b40 >> b41 >> b42 >> b43 >> b44 >> b45
        b45 >> b46 >> b47 >> b48 >> b49 >> b50 >> b51 >> b52 >> b53 >> b54
        b54 >> b55 >> b56 >> b57 >> b58 >> b59 >> b60 >> b61 >> b62 >> b63
        b63 >> b64 >> b65 >> b66 >> b67 >> b68 >> b69
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
        d0 = PostgresOperator(task_id="gv_ventas", postgres_conn_id=CONN,
                                 sql=path+"vm003_gv_ventas.sql")
        d1 = PostgresOperator(task_id="vista_reporte_diario", postgres_conn_id=CONN,
                                 sql=path+"vm004_vista_reporte_diario.sql")
        d2 = PostgresOperator(task_id="vista_reporte_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm005_vista_reporte_mensual.sql")
        d3 = PostgresOperator(task_id="vista_reporte_semanal", postgres_conn_id=CONN,
                                 sql=path+"vm006_vista_reporte_semanal.sql")
        d4 = PostgresOperator(task_id="vista_reporte_canasto_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm007_vista_reporte_canasto_mensual.sql")
        d5 = PostgresOperator(task_id="vista_de_ventas_diarias", postgres_conn_id=CONN,
                                 sql=path+"vm008_vista_de_ventas_diarias.sql")
        d6 = PostgresOperator(task_id="vista_venta_por_producto_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm009_vista_venta_por_producto_mensual.sql")
        d7 = PostgresOperator(task_id="notificaciones", postgres_conn_id=CONN,
                                 sql=path+"vm010_notificaciones.sql")
        d8 = PostgresOperator(task_id="vista_inventario_mensual", postgres_conn_id=CONN,
                                 sql=path+"vm011_vista_inventario_mensual.sql")
        d9 = PostgresOperator(task_id="vista_compras_mensuales", postgres_conn_id=CONN,
                                 sql=path+"vm012_vista_compras_mensuales.sql")
        d10 = PostgresOperator(task_id="vista_gerencial", postgres_conn_id=CONN,
                                 sql=path+"vm013_vista_gerencial.sql")
        d11 = PostgresOperator(task_id="vista_venta_por_producto_mensual_por_hora", postgres_conn_id=CONN,
                                 sql=path+"vm014_vista_venta_por_producto_mensual_por_hora.sql")
        d12 = PostgresOperator(task_id="vista_venta_por_producto_mensual_por_dia", postgres_conn_id=CONN,
                                 sql=path+"vm015_vista_venta_por_producto_mensual_por_dia.sql")
        d13 = PostgresOperator(task_id="vista_proyecciones_de_compra", postgres_conn_id=CONN,
                                 sql=path+"vm016_vista_proyecciones_de_compra.sql")
        d14 = PostgresOperator(task_id="vista_transacciones_de_venta_detallada", postgres_conn_id=CONN,
                                 sql=path+"vm017_vista_transacciones_de_venta_detallada.sql")
        d15 = PostgresOperator(task_id="vista_transacciones_de_venta_detallada_demografico", postgres_conn_id=CONN,
                                 sql=path+"vm018_vista_transacciones_de_venta_detallada_demografico.sql")
        d16 = PostgresOperator(task_id="vista_analisis_de_tiendas", postgres_conn_id=CONN,
                                 sql=path+"vm019_vista_analisis_de_tiendas.sql")
        d17 = PostgresOperator(task_id="vista_compras_de_tiendas_diario", postgres_conn_id=CONN,
                                 sql=path+"vm020_vista_compras_de_tiendas_diario.sql")
        d18 = PostgresOperator(task_id="vista_analisis_de_tiendas_diario", postgres_conn_id=CONN,
                                 sql=path+"vm021_vista_analisis_de_tiendas_diario.sql")
        d19 = PostgresOperator(task_id="vista_horario_de_tiendas", postgres_conn_id=CONN,
                                 sql=path+"vm022_vista_horario_de_tiendas.sql")
        d20 = PostgresOperator(task_id="vista_analisis_de_precio", postgres_conn_id=CONN,
                                 sql=path+"vm023_vista_analisis_de_precio.sql")

        d0 >> d1 >> d2 >> d3 >> d4 >> d5 >> d6 >> d7 >> d8 >> d9
        d9 >> d10 >> d11 >> d12 >> d13 >> d14 >> d15 >> d16 >> d17 >> d18
        d18 >> d19 >> d20
   # End Task Group definition

    tg1 >> tg2 >> tg3 >> tg4