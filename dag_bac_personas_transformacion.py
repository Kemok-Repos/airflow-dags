from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

CONN = 'bac_personas_postgres'

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
  dag_id="transformacion-bac-personas",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso manual
  schedule_interval=None, #UTC
  max_active_runs=1,
  catchup=False,
  tags=['bac-personas','transformacion'],
) as dag:
    # Start Task Group Definition
    path = 'bac-personas-sql/sql/0_tablas_maestras/'
    with TaskGroup(group_id='0_tablas_maestras') as tg1:
        a0 = PostgresOperator(task_id="update_maestro_id", postgres_conn_id=CONN,
                                 sql=path+"tm001_update_maestro_id.sql")
        a1 = PostgresOperator(task_id="upsert_canal_de_venta_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tm002_upsert_canal_de_venta_gestor_general.sql")
        a2 = PostgresOperator(task_id="upsert_canal_de_venta_libranza", postgres_conn_id=CONN,
                                 sql=path+"tm003_upsert_canal_de_venta_libranza.sql")
        a3 = PostgresOperator(task_id="upsert_canal_de_venta_meta_ejecutivo", postgres_conn_id=CONN,
                                 sql=path+"tm004_upsert_canal_de_venta_meta_ejecutivo.sql")
        a4 = PostgresOperator(task_id="upsert_canal_de_venta_meta_area", postgres_conn_id=CONN,
                                 sql=path+"tm005_upsert_canal_de_venta_meta_area.sql")
        a5 = PostgresOperator(task_id="upsert_canal_de_venta_on_archivo_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tm006_upsert_canal_de_venta_on_archivo_gestor_general.sql")
        a6 = PostgresOperator(task_id="upsert_canal_de_venta_on_archivo_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tm007_upsert_canal_de_venta_on_archivo_libranzas.sql")
        a7 = PostgresOperator(task_id="upsert_canal_de_venta_on_archivo_metas_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tm008_upsert_canal_de_venta_on_archivo_metas_ejecutivos.sql")
        a8 = PostgresOperator(task_id="upsert_canal_de_venta_on_archivo_metas_area", postgres_conn_id=CONN,
                                 sql=path+"tm009_upsert_canal_de_venta_on_archivo_metas_area.sql")
        a9 = PostgresOperator(task_id="upsert_articulo_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tm010_upsert_articulo_gestor_general.sql")
        a10 = PostgresOperator(task_id="upsert_articulo_libranza", postgres_conn_id=CONN,
                                 sql=path+"tm011_upsert_articulo_libranza.sql")
        a11 = PostgresOperator(task_id="upsert_articulo_meta_ejecutivo", postgres_conn_id=CONN,
                                 sql=path+"tm012_upsert_articulo_meta_ejecutivo.sql")
        a12 = PostgresOperator(task_id="upsert_articulo_meta_area", postgres_conn_id=CONN,
                                 sql=path+"tm013_upsert_articulo_meta_area.sql")
        a13 = PostgresOperator(task_id="upsert_articulo_on_archivo_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tm014_upsert_articulo_on_archivo_gestor_general.sql")
        a14 = PostgresOperator(task_id="upsert_articulo_on_archivo_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tm015_upsert_articulo_on_archivo_libranzas.sql")
        a15 = PostgresOperator(task_id="upsert_articulo_on_archivo_metas_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tm016_upsert_articulo_on_archivo_metas_ejecutivos.sql")
        a16 = PostgresOperator(task_id="upsert_articulo_on_archivo_metas_area", postgres_conn_id=CONN,
                                 sql=path+"tm017_upsert_articulo_on_archivo_metas_area.sql")
        a17 = PostgresOperator(task_id="update_alias_de_vendedor", postgres_conn_id=CONN,
                                 sql=path+"tm018_update_alias_de_vendedor.sql")
        a18 = PostgresOperator(task_id="upsert_vendedor_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tm019_upsert_vendedor_gestor_general.sql")
        a19 = PostgresOperator(task_id="upsert_vendedor_libranza", postgres_conn_id=CONN,
                                 sql=path+"tm020_upsert_vendedor_libranza.sql")
        a20 = PostgresOperator(task_id="upsert_vendedor_meta_ejecutivo", postgres_conn_id=CONN,
                                 sql=path+"tm021_upsert_vendedor_meta_ejecutivo.sql")
        a21 = PostgresOperator(task_id="upsert_vendedor_on_archivo_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tm022_upsert_vendedor_on_archivo_gestor_general.sql")
        a22 = PostgresOperator(task_id="upsert_vendedor_on_archivo_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tm023_upsert_vendedor_on_archivo_libranzas.sql")
        a23 = PostgresOperator(task_id="upsert_vendedor_on_archivo_metas_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tm024_upsert_vendedor_on_archivo_metas_ejecutivos.sql")
        a24 = PostgresOperator(task_id="upsert_vendedor_on_fecha_de_ingreso_gestor", postgres_conn_id=CONN,
                                 sql=path+"tm025_upsert_vendedor_on_fecha_de_ingreso_gestor.sql")
        a25 = PostgresOperator(task_id="upsert_vendedor_on_fecha_de_ingreso_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tm028_upsert_vendedor_on_fecha_de_ingreso_libranzas.sql")
        a26 = PostgresOperator(task_id="upsert_g2s_feriados", postgres_conn_id=CONN,
                                 sql=path+"tm029_upsert_g2s_feriados.sql")
        a27 = PostgresOperator(task_id="auxiliar_temporadas_truncate_insert", postgres_conn_id=CONN,
                                 sql=path+"tm030_auxiliar_temporadas_truncate_insert.sql")
        a28 = PostgresOperator(task_id="auxiliar_temporadas_update_archivo_cierres", postgres_conn_id=CONN,
                                 sql=path+"tm031_auxiliar_temporadas_update_archivo_cierres.sql")
        a29 = PostgresOperator(task_id="auxiliar_temporadas_update_rango", postgres_conn_id=CONN,
                                 sql=path+"tm032_auxiliar_temporadas_update_rango.sql")
        a30 = PostgresOperator(task_id="auxiliar_temporadas_update_dias", postgres_conn_id=CONN,
                                 sql=path+"tm033_auxiliar_temporadas_update_dias.sql")

        a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> a6 >> a7 >> a8 >> a9
        a9 >> a10 >> a11 >> a12 >> a13 >> a14 >> a15 >> a16 >> a17 >> a18
        a18 >> a19 >> a20 >> a21 >> a22 >> a23 >> a24 >> a25 >> a26 >> a27
        a27 >> a28 >> a29 >> a30
   # End Task Group definition

    # Start Task Group Definition
    path = 'bac-personas-sql/sql/1_tablas_transaccionales/'
    with TaskGroup(group_id='1_tablas_transaccionales') as tg2:
        b0 = PostgresOperator(task_id="auxiliar_metricas_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt001_auxiliar_metricas_truncate.sql")
        b1 = PostgresOperator(task_id="auxiliar_metricas_insert_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tt002_auxiliar_metricas_insert_gestor_general.sql")
        b2 = PostgresOperator(task_id="auxiliar_metricas_insert_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt003_auxiliar_metricas_insert_libranzas.sql")
        b3 = PostgresOperator(task_id="auxiliar_metricas_delete_inactivos", postgres_conn_id=CONN,
                                 sql=path+"tt004_auxiliar_metricas_delete_inactivos.sql")
        b4 = PostgresOperator(task_id="auxiliar_metricas_delete_cambio_de_area_postumo", postgres_conn_id=CONN,
                                 sql=path+"tt005_auxiliar_metricas_delete_cambio_de_area_postumo.sql")
        b5 = PostgresOperator(task_id="auxiliar_metricas_delete_cambio_de_area_previo", postgres_conn_id=CONN,
                                 sql=path+"tt006_auxiliar_metricas_delete_cambio_de_area_previo.sql")
        b6 = PostgresOperator(task_id="auxiliar_metricas_insert_areas_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tt007_auxiliar_metricas_insert_areas_gestor_general.sql")
        b7 = PostgresOperator(task_id="auxiliar_metricas_insert_areas_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt008_auxiliar_metricas_insert_areas_libranzas.sql")
        b8 = PostgresOperator(task_id="auxiliar_metricas_update_metricas_siembra_gestor_general_area_1_2", postgres_conn_id=CONN,
                                 sql=path+"tt009_auxiliar_metricas_update_metricas_siembra_gestor_general_area_1_2.sql")
        b9 = PostgresOperator(task_id="auxiliar_metricas_update_metricas_siembra_gestor_general_areas", postgres_conn_id=CONN,
                                 sql=path+"tt010_auxiliar_metricas_update_metricas_siembra_gestor_general_areas.sql")
        b10 = PostgresOperator(task_id="auxiliar_metricas_update_metricas_siembra_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt011_auxiliar_metricas_update_metricas_siembra_libranzas.sql")
        b11 = PostgresOperator(task_id="auxiliar_metricas_update_metricas_mes_gestor_general_area", postgres_conn_id=CONN,
                                 sql=path+"tt012_auxiliar_metricas_update_metricas_mes_gestor_general_area.sql")
        b12 = PostgresOperator(task_id="auxiliar_metricas_update_metricas_mes_libranzas_area", postgres_conn_id=CONN,
                                 sql=path+"tt013_auxiliar_metricas_update_metricas_mes_libranzas_area.sql")
        b13 = PostgresOperator(task_id="auxiliar_metricas_update_medianas", postgres_conn_id=CONN,
                                 sql=path+"tt014_auxiliar_metricas_update_medianas.sql")
        b14 = PostgresOperator(task_id="auxiliar_metricas_update_propiedades_en_meses_iniciales", postgres_conn_id=CONN,
                                 sql=path+"tt015_auxiliar_metricas_update_propiedades_en_meses_iniciales.sql")
        b15 = PostgresOperator(task_id="auxiliar_metricas_update_propiedades_metricas_del_area", postgres_conn_id=CONN,
                                 sql=path+"tt016_auxiliar_metricas_update_propiedades_metricas_del_area.sql")
        b16 = PostgresOperator(task_id="auxiliar_metricas_update_metas_desembolso_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tt017_auxiliar_metricas_update_metas_desembolso_ejecutivos.sql")
        b17 = PostgresOperator(task_id="auxiliar_metricas_update_metas_desembolso_area", postgres_conn_id=CONN,
                                 sql=path+"tt018_auxiliar_metricas_update_metas_desembolso_area.sql")
        b18 = PostgresOperator(task_id="auxiliar_metricas_update_metas_de_ingreso_y_prospeccion", postgres_conn_id=CONN,
                                 sql=path+"tt019_auxiliar_metricas_update_metas_de_ingreso_y_prospeccion.sql")
        b19 = PostgresOperator(task_id="auxiliar_metricas_update_mediana_metricas_mensuales", postgres_conn_id=CONN,
                                 sql=path+"tt020_auxiliar_metricas_update_mediana_metricas_mensuales.sql")
        b20 = PostgresOperator(task_id="auxiliar_metricas_insert_balanceador", postgres_conn_id=CONN,
                                 sql=path+"tt021_auxiliar_metricas_insert_balanceador.sql")
        b21 = PostgresOperator(task_id="auxiliar_metricas_update_meta_anual", postgres_conn_id=CONN,
                                 sql=path+"tt022_auxiliar_metricas_update_meta_anual.sql")
        b22 = PostgresOperator(task_id="auxiliar_metricas_update_desembolsos_anual_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tt023_auxiliar_metricas_update_desembolsos_anual_gestor_general.sql")
        b23 = PostgresOperator(task_id="auxiliar_metricas_update_desembolsos_anual_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt024_auxiliar_metricas_update_desembolsos_anual_libranzas.sql")
        b24 = PostgresOperator(task_id="auxiliar_metricas_update_alcances_gestor_general_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tt026_auxiliar_metricas_update_alcances_gestor_general_ejecutivos.sql")
        b25 = PostgresOperator(task_id="auxiliar_metricas_update_alcances_gestor_general_ejecutivos_areas_1_2", postgres_conn_id=CONN,
                                 sql=path+"tt027_auxiliar_metricas_update_alcances_gestor_general_ejecutivos_areas_1_2.sql")
        b26 = PostgresOperator(task_id="auxiliar_metricas_update_alcances_gestor_general_area", postgres_conn_id=CONN,
                                 sql=path+"tt028_auxiliar_metricas_update_alcances_gestor_general_area.sql")
        b27 = PostgresOperator(task_id="auxiliar_metricas_update_alcances_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt029_auxiliar_metricas_update_alcances_libranzas.sql")
        b28 = PostgresOperator(task_id="auxiliar_metricas_update_alcances_libranzas_area", postgres_conn_id=CONN,
                                 sql=path+"tt030_auxiliar_metricas_update_alcances_libranzas_area.sql")
        b29 = PostgresOperator(task_id="auxiliar_metricas_update_ranks", postgres_conn_id=CONN,
                                 sql=path+"tt031_auxiliar_metricas_update_ranks.sql")
        b30 = PostgresOperator(task_id="auxiliar_registro_de_etapas_insert_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tt035_auxiliar_registro_de_etapas_insert_gestor_general.sql")
        b31 = PostgresOperator(task_id="auxiliar_registro_de_etapas_insert_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt036_auxiliar_registro_de_etapas_insert_libranzas.sql")
        b32 = PostgresOperator(task_id="auxiliar_registro_de_etapas_delete_orden_mayor", postgres_conn_id=CONN,
                                 sql=path+"tt037_auxiliar_registro_de_etapas_delete_orden_mayor.sql")
        b33 = PostgresOperator(task_id="auxiliar_registro_de_etapas_orden", postgres_conn_id=CONN,
                                 sql=path+"tt038_auxiliar_registro_de_etapas_orden.sql")
        b34 = PostgresOperator(task_id="auxiliar_registro_de_etapas_delete_descontinuados", postgres_conn_id=CONN,
                                 sql=path+"tt039_auxiliar_registro_de_etapas_delete_descontinuados.sql")
        b35 = PostgresOperator(task_id="auxiliar_metricas_diarias_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt041_auxiliar_metricas_diarias_truncate.sql")
        b36 = PostgresOperator(task_id="auxiliar_metricas_diarias_insert_auxiliar_metricas", postgres_conn_id=CONN,
                                 sql=path+"tt042_auxiliar_metricas_diarias_insert_auxiliar_metricas.sql")
        b37 = PostgresOperator(task_id="auxiliar_metricas_diarias_delete_inactivos", postgres_conn_id=CONN,
                                 sql=path+"tt043_auxiliar_metricas_diarias_delete_inactivos.sql")
        b38 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_siembra_gestor_general_areas_1_2", postgres_conn_id=CONN,
                                 sql=path+"tt044_auxiliar_metricas_diarias_update_metricas_siembra_gestor_general_areas_1_2.sql")
        b39 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_siembra_gestor_general_demas_areas", postgres_conn_id=CONN,
                                 sql=path+"tt045_auxiliar_metricas_diarias_update_metricas_siembra_gestor_general_demas_areas.sql")
        b40 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_siembra_area_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tt046_auxiliar_metricas_diarias_update_metricas_siembra_area_gestor_general.sql")
        b41 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_siembra_area_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt047_auxiliar_metricas_diarias_update_metricas_siembra_area_libranzas.sql")
        b42 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_siembra_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt048_auxiliar_metricas_diarias_update_metricas_siembra_libranzas.sql")
        b43 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_desembolso_gestor_general_area_1_2", postgres_conn_id=CONN,
                                 sql=path+"tt049_auxiliar_metricas_diarias_update_metricas_desembolso_gestor_general_area_1_2.sql")
        b44 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_desembolso_gestor_general_demas_areas", postgres_conn_id=CONN,
                                 sql=path+"tt050_auxiliar_metricas_diarias_update_metricas_desembolso_gestor_general_demas_areas.sql")
        b45 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_desembolso_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt051_auxiliar_metricas_diarias_update_metricas_desembolso_libranzas.sql")
        b46 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_desembolso_area_gestor_general", postgres_conn_id=CONN,
                                 sql=path+"tt052_auxiliar_metricas_diarias_update_metricas_desembolso_area_gestor_general.sql")
        b47 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_desembolso_area_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt053_auxiliar_metricas_diarias_update_metricas_desembolso_area_libranzas.sql")
        b48 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metas_ejecutivo", postgres_conn_id=CONN,
                                 sql=path+"tt054_auxiliar_metricas_diarias_update_metas_ejecutivo.sql")
        b49 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metas_area", postgres_conn_id=CONN,
                                 sql=path+"tt055_auxiliar_metricas_diarias_update_metas_area.sql")
        b50 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_mes_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tt056_auxiliar_metricas_diarias_update_mes_ejecutivos.sql")
        b51 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_dia_areas", postgres_conn_id=CONN,
                                 sql=path+"tt059_auxiliar_metricas_diarias_update_dia_areas.sql")
        b52 = PostgresOperator(task_id="auxiliar_metricas_diarias_update_metricas_anteriores", postgres_conn_id=CONN,
                                 sql=path+"tt060_auxiliar_metricas_diarias_update_metricas_anteriores.sql")
        b53 = PostgresOperator(task_id="auxiliar_usuarios_truncate", postgres_conn_id=CONN,
                                 sql=path+"tt061_auxiliar_usuarios_truncate.sql")
        b54 = PostgresOperator(task_id="auxiliar_usuarios_insert_metabase", postgres_conn_id=CONN,
                                 sql=path+"tt062_auxiliar_usuarios_insert_metabase.sql")
        b55 = PostgresOperator(task_id="auxiliar_usuarios_insert_kontainer", postgres_conn_id=CONN,
                                 sql=path+"tt063_auxiliar_usuarios_insert_kontainer.sql")
        b56 = PostgresOperator(task_id="auxiliar_usuarios_update_name", postgres_conn_id=CONN,
                                 sql=path+"tt064_auxiliar_usuarios_update_name.sql")
        b57 = PostgresOperator(task_id="auxiliar_usuarios_update_estrategia", postgres_conn_id=CONN,
                                 sql=path+"tt065_auxiliar_usuarios_update_estrategia.sql")
        b58 = PostgresOperator(task_id="auxiliar_usuarios_update_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tt066_auxiliar_usuarios_update_ejecutivos.sql")
        b59 = PostgresOperator(task_id="auxiliar_usuarios_update_televentas_agencias", postgres_conn_id=CONN,
                                 sql=path+"tt067_auxiliar_usuarios_update_televentas_agencias.sql")
        b60 = PostgresOperator(task_id="auxiliar_usuarios_update_gerentes", postgres_conn_id=CONN,
                                 sql=path+"tt068_auxiliar_usuarios_update_gerentes.sql")
        b61 = PostgresOperator(task_id="auxiliar_usuarios_update_lideres_prendas_e_hipotecas", postgres_conn_id=CONN,
                                 sql=path+"tt069_auxiliar_usuarios_update_lideres_prendas_e_hipotecas.sql")
        b62 = PostgresOperator(task_id="auxiliar_usuarios_update_lideres_libranzas", postgres_conn_id=CONN,
                                 sql=path+"tt070_auxiliar_usuarios_update_lideres_libranzas.sql")
        b63 = PostgresOperator(task_id="auxiliar_usuarios_update_externos", postgres_conn_id=CONN,
                                 sql=path+"tt071_auxiliar_usuarios_update_externos.sql")
        b64 = PostgresOperator(task_id="update_vendedor_email", postgres_conn_id=CONN,
                                 sql=path+"tt072_update_vendedor_email.sql")

        b0 >> b1 >> b2 >> b3 >> b4 >> b5 >> b6 >> b7 >> b8 >> b9
        b9 >> b10 >> b11 >> b12 >> b13 >> b14 >> b15 >> b16 >> b17 >> b18
        b18 >> b19 >> b20 >> b21 >> b22 >> b23 >> b24 >> b25 >> b26 >> b27
        b27 >> b28 >> b29 >> b30 >> b31 >> b32 >> b33 >> b34 >> b35 >> b36
        b36 >> b37 >> b38 >> b39 >> b40 >> b41 >> b42 >> b43 >> b44 >> b45
        b45 >> b46 >> b47 >> b48 >> b49 >> b50 >> b51 >> b52 >> b53 >> b54
        b54 >> b55 >> b56 >> b57 >> b58 >> b59 >> b60 >> b61 >> b62 >> b63
        b63 >> b64
   # End Task Group definition

    # Start Task Group Definition
    path = 'bac-personas-sql/sql/2_tablas_resumidas/'
    with TaskGroup(group_id='2_tablas_resumidas') as tg3:
        c0 = PostgresOperator(task_id="resumen_metricas_truncate", postgres_conn_id=CONN,
                                 sql=path+"tr001_resumen_metricas_truncate.sql")
        c1 = PostgresOperator(task_id="resumen_metricas_insert_metricas_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tr002_resumen_metricas_insert_metricas_ejecutivos.sql")
        c2 = PostgresOperator(task_id="resumen_metricas_update_metricas_ejecutivos_de_agencias_y_canales_en_mes_calendario", postgres_conn_id=CONN,
                                 sql=path+"tr003_resumen_metricas_update_metricas_ejecutivos_de_agencias_y_canales_en_mes_calendario.sql")
        c3 = PostgresOperator(task_id="resumen_metricas_update_metricas_ejecutivos_de_prendas_y_proyectos_en_mes_calendario", postgres_conn_id=CONN,
                                 sql=path+"tr004_resumen_metricas_update_metricas_ejecutivos_de_prendas_y_proyectos_en_mes_calendario.sql")
        c4 = PostgresOperator(task_id="resumen_metricas_update_metricas_ejecutivos_de_libranzas_en_mes_calendario", postgres_conn_id=CONN,
                                 sql=path+"tr005_resumen_metricas_update_metricas_ejecutivos_de_libranzas_en_mes_calendario.sql")
        c5 = PostgresOperator(task_id="resumen_metricas_update_metricas_ejecutivos_de_agencias_y_canales_en_mes_ejecutivo", postgres_conn_id=CONN,
                                 sql=path+"tr006_resumen_metricas_update_metricas_ejecutivos_de_agencias_y_canales_en_mes_ejecutivo.sql")
        c6 = PostgresOperator(task_id="resumen_metricas_update_metricas_ejecutivos_de_prendas_y_proyectos_en_mes_ejecutivo", postgres_conn_id=CONN,
                                 sql=path+"tr007_resumen_metricas_update_metricas_ejecutivos_de_prendas_y_proyectos_en_mes_ejecutivo.sql")
        c7 = PostgresOperator(task_id="resumen_metricas_update_metricas_ejecutivos_de_libranzas_en_mes_ejecutivos", postgres_conn_id=CONN,
                                 sql=path+"tr008_resumen_metricas_update_metricas_ejecutivos_de_libranzas_en_mes_ejecutivos.sql")
        c8 = PostgresOperator(task_id="resumen_metricas_update_metricas_ejecutivos_anuales", postgres_conn_id=CONN,
                                 sql=path+"tr009_resumen_metricas_update_metricas_ejecutivos_anuales.sql")
        c9 = PostgresOperator(task_id="resumen_metricas_insert_metricas_area", postgres_conn_id=CONN,
                                 sql=path+"tr010_resumen_metricas_insert_metricas_area.sql")
        c10 = PostgresOperator(task_id="resumen_metricas_update_metricas_area_de_agencias_canales_prendas_y_proyectos_en_mes_calendario", postgres_conn_id=CONN,
                                 sql=path+"tr011_resumen_metricas_update_metricas_area_de_agencias_canales_prendas_y_proyectos_en_mes_calendario.sql")
        c11 = PostgresOperator(task_id="resumen_metricas_update_metricas_area_de_libranzas_en_mes_calendario", postgres_conn_id=CONN,
                                 sql=path+"tr012_resumen_metricas_update_metricas_area_de_libranzas_en_mes_calendario.sql")
        c12 = PostgresOperator(task_id="resumen_metricas_update_metricas_area_compensacion", postgres_conn_id=CONN,
                                 sql=path+"tr013_resumen_metricas_update_metricas_area_compensacion.sql")

        c0 >> c1 >> c2 >> c3 >> c4 >> c5 >> c6 >> c7 >> c8 >> c9
        c9 >> c10 >> c11 >> c12
   # End Task Group definition

    # Start Task Group Definition
    path = 'bac-personas-sql/sql/3_vista_materializadas/'
    with TaskGroup(group_id='3_vista_materializadas') as tg4:
        d0 = PostgresOperator(task_id="refresh___notificaciones__", postgres_conn_id=CONN,
                                 sql=path+"vm0015_refresh___notificaciones__.sql")
        d1 = PostgresOperator(task_id="refresh_vista_prendas_hipotecas_metricas", postgres_conn_id=CONN,
                                 sql=path+"vm001_refresh_vista_prendas_hipotecas_metricas.sql")
        d2 = PostgresOperator(task_id="refresh_vista__prendas_hipotecas_detalle", postgres_conn_id=CONN,
                                 sql=path+"vm002_refresh_vista__prendas_hipotecas_detalle.sql")
        d3 = PostgresOperator(task_id="refresh_vista_prendas_hipotecas_productividad", postgres_conn_id=CONN,
                                 sql=path+"vm003_refresh_vista_prendas_hipotecas_productividad.sql")
        d4 = PostgresOperator(task_id="refresh_vista_prendas_hipotecas_diario", postgres_conn_id=CONN,
                                 sql=path+"vm004_refresh_vista_prendas_hipotecas_diario.sql")
        d5 = PostgresOperator(task_id="refresh_vista_libranzas_metricas", postgres_conn_id=CONN,
                                 sql=path+"vm005_refresh_vista_libranzas_metricas.sql")
        d6 = PostgresOperator(task_id="refresh_vista_libranzas_detalle", postgres_conn_id=CONN,
                                 sql=path+"vm006_refresh_vista_libranzas_detalle.sql")
        d7 = PostgresOperator(task_id="refresh_vista_libranzas_productividad", postgres_conn_id=CONN,
                                 sql=path+"vm007_refresh_vista_libranzas_productividad.sql")
        d8 = PostgresOperator(task_id="refresh_vista_libranzas_diario", postgres_conn_id=CONN,
                                 sql=path+"vm008_refresh_vista_libranzas_diario.sql")
        d9 = PostgresOperator(task_id="refresh_colocacion_metricas_rrip", postgres_conn_id=CONN,
                                 sql=path+"vm009_refresh_colocacion_metricas_rrip.sql")
        d10 = PostgresOperator(task_id="refresh_colocacion_metricas_departamento_cartera", postgres_conn_id=CONN,
                                 sql=path+"vm010_refresh_colocacion_metricas_departamento_cartera.sql")

        d0 >> d1 >> d2 >> d3 >> d4 >> d5 >> d6 >> d7 >> d8 >> d9
        d9 >> d10
   # End Task Group definition

    tg1 >> tg2 >> tg3 >> tg4