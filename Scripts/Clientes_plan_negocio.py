from Lectura_insumos import base_biblia_seleccionada, base_plantilla7
from General_Functions import exportar_a_excel
from Transformation_functions import (
    Filtrar_por_valores_excluidos,
    Eliminar_columnas,
    filtrar_por_valores,
    Eliminar_filas_con_cadena,
    Agregar_columna_constante,
    Group_by_and_sum_cols,
    Renombrar_columnas_con_diccionario,
    rellenar_columnas_nulas,
    establecer_periodo,
    concatenar_dataframes,
    Cambiar_tipo_dato_multiples_columnas,
    Duplicar_muliples_columnas,
    Reemplazar_valores_misma_columna,
)

import config_constans as cc

MES = cc.MES
SUBCANAL = cc.SUB_CANAL
REEMPLAZOS_BIBLIA_SUBCANAL = cc.REEMPLAZOS_BIBLIA
CLIENTE = cc.CLIENTE
NIT = cc.NIT
ACUERDO = cc.ACUERDO
ESTADO_FINAL = cc.ESTADO_FINAL
META = cc.META
INVERSION = cc.INVERSION
BENEFICIO = cc.BENEFICIO
COD_LOC = cc.COD_LOC
COD_CLI = cc.COD_CLI
COD_CLI_ORIG = cc.COD_CLI_ORIG
config = cc.config
RESULTADOS_P7 = cc.RESULTADOS_P7
dict_mcp7 = config["MODS_COLS_PLANT_7"]
DICT_MESES = config["dict_meses"]
dc_ptlls = config["Plantillas"]
a_a_a_a = "Año"

# Borrar duplicados de la base_biblia.
base_biblia_filtrada = base_biblia_seleccionada.drop_duplicates()

# Extraemos la columna subcanal
subcanal_biblia = base_biblia_filtrada[SUBCANAL]

# Hacemos los reemplazos correspondientes en la columna subcanal.
subcanal_biblia = subcanal_biblia.replace(REEMPLAZOS_BIBLIA_SUBCANAL)

# Actualizamos la columna socios en el dataframe.
base_biblia_filtrada.loc[:, SUBCANAL] = subcanal_biblia


# Base_plantilla7
# Crear los subconjuntos de datos según las necesidades.
# Eliminar de ambos col Subcanl el segmento "Tiendas"
for key, value in dict_mcp7["VAL_EXCLUIR_SUBC_P7_META_INVER"].items():
    base_plantilla7_filtro1 = Filtrar_por_valores_excluidos(
        df=base_plantilla7, columna=key, valores_excluir=value
    )

# Filtrar por meses "Ene a Dic"
base_plantilla7_filtro2 = Reemplazar_valores_misma_columna(
    df=base_plantilla7_filtro1,
    columna=MES,
    diccionario_mapeo=dict_mcp7["RENOMBRAR_MESES_P7"],
)

# Eliminar los registros con subcanal en blanco.
base_plantilla7_filtro3 = base_plantilla7_filtro2.dropna(subset=SUBCANAL)

# Filtrar Estado final mantieniendo clientes activos.
#base_plantilla7_filtro4 = filtrar_por_valores(
#    df=base_plantilla7_filtro3, columna=ESTADO_FINAL, valores_filtrar="Activo"
#)

# Tomar los segmentos de meta e inversión a partir de aquí.

base_plantilla7_meta = Eliminar_columnas(
    df=base_plantilla7_filtro3, columnas_a_eliminar=INVERSION
)
base_plantilla7_inver = Eliminar_columnas(
    df=base_plantilla7_filtro3, columnas_a_eliminar=META
)

# Filtraciones en la columna Acuerdo para cada subconjunto.
base_plantilla7_meta_filtrada = filtrar_por_valores(
    df=base_plantilla7_meta,
    columna=dict_mcp7["VAL_MANTENER_ACUERDO_P7_META"]["columna"],
    valores_filtrar=dict_mcp7["VAL_MANTENER_ACUERDO_P7_META"]["valor"],
)

base_plantilla7_inver_filtrada = Eliminar_filas_con_cadena(
    df=base_plantilla7_inver,
    columna=dict_mcp7["VAL_EXCLUIR_ACUERTO_P7_INV"]["columna"],
    cadena=dict_mcp7["VAL_EXCLUIR_ACUERTO_P7_INV"]["valor"],
)

# Agregar columnas constantes eliminadas anteirormente.
base_plantilla7_meta_filtrada_2 = Agregar_columna_constante(
    dataframe=base_plantilla7_meta_filtrada,
    nombre_col_cols=INVERSION,
    valor_constante="0",
)
base_plantilla7_inver_filtrada_2 = Agregar_columna_constante(
    dataframe=base_plantilla7_inver_filtrada, nombre_col_cols=META, valor_constante="0"
)

base_plantilla7_meta_periodo = Renombrar_columnas_con_diccionario(
    base=base_plantilla7_meta_filtrada_2, cols_to_rename=dict_mcp7["RENOMBRAR_COLS_P7"]
)

base_plantilla7_inv_periodo = Renombrar_columnas_con_diccionario(
    base=base_plantilla7_inver_filtrada_2, cols_to_rename=dict_mcp7["RENOMBRAR_COLS_P7"]
)


# Establecer el valor para el año completo
base_plantilla7_meta_ano = base_plantilla7_meta_periodo.copy()
base_plantilla7_inv_ano = base_plantilla7_inv_periodo.copy()

# Renombrar las columnas de las bases año.
base_plantilla7_meta_ano = Renombrar_columnas_con_diccionario(
    base=base_plantilla7_meta_ano, cols_to_rename=dict_mcp7["RENOMBRAR_COLS_AÑO_P7"]
)

base_plantilla7_inv_ano = Renombrar_columnas_con_diccionario(
    base=base_plantilla7_inv_ano, cols_to_rename=dict_mcp7["RENOMBRAR_COLS_AÑO_P7"]
)

base_plantilla7_meta_ano.loc[:, MES] = a_a_a_a
base_plantilla7_inv_ano.loc[:, MES] = a_a_a_a

# Establecer el periodo para las bases de peiriodo ( Trim , Acum , Act )
base_plantilla7_meta_periodo = establecer_periodo(
    df=base_plantilla7_meta_periodo, dict_meses=DICT_MESES, hay_trim=config["hay_trim"],mes_act=config["mes_act"] 
)
base_plantilla7_inv_periodo = establecer_periodo(
    df=base_plantilla7_inv_periodo, dict_meses=DICT_MESES, hay_trim=config["hay_trim"], mes_act=config["mes_act"]
)


# Reordenar columnas ambas bases para concatenar posteriormente.
base_plantilla7_meta_def = base_plantilla7_meta_periodo[dict_mcp7["ORDEN_COLS_P7"]]
base_plantilla7_inv_def = base_plantilla7_inv_periodo[dict_mcp7["ORDEN_COLS_P7"]]
base_plantilla7_meta_ano = base_plantilla7_meta_ano[dict_mcp7["ORDEN_COLS_P7_AÑO"]]
base_plantilla7_inv_ano = base_plantilla7_inv_ano[dict_mcp7["ORDEN_COLS_P7_AÑO"]]

# Concatenar ambas bases
base_plantilla7_concat_ac_trm_mes = concatenar_dataframes(
    df_list=[
        base_plantilla7_meta_def,
        base_plantilla7_inv_def,
    ]
)
base_plantilla7_concat_ano = concatenar_dataframes(
    df_list=[
        base_plantilla7_meta_ano,
        base_plantilla7_inv_ano,
    ]
)

# Renombrar las columnas  (meta y beneficio) a (meta anual y beneficio anual) para la parte de la plantilla que contiene el año.

base_plantilla7_concat_ac_trm_mes = Agregar_columna_constante(
    dataframe=base_plantilla7_concat_ac_trm_mes,
    nombre_col_cols=dict_mcp7["COLS_SUM_P7"][2:],
    valor_constante=0,
)

base_plantilla7_concat_ano = Agregar_columna_constante(
    dataframe=base_plantilla7_concat_ano,
    nombre_col_cols=dict_mcp7["COLS_SUM_P7"][:2],
    valor_constante=0,
)

base_plantilla7_concat_ano = Cambiar_tipo_dato_multiples_columnas(
    base=base_plantilla7_concat_ano,
    list_columns=dict_mcp7["COLS_SUM_P7"],
    type_data=float,
)
base_plantilla7_concat_ac_trm_mes = Cambiar_tipo_dato_multiples_columnas(
    base=base_plantilla7_concat_ac_trm_mes,
    list_columns=dict_mcp7["COLS_SUM_P7"],
    type_data=float,
)

base_plantilla7_concat_ano = rellenar_columnas_nulas(
    df=base_plantilla7_concat_ano, columna=dict_mcp7["COLS_SUM_P7"], valor=0
)
base_plantilla7_concat_ac_trm_mes = rellenar_columnas_nulas(
    df=base_plantilla7_concat_ac_trm_mes, columna=dict_mcp7["COLS_SUM_P7"], valor=0
)

base_plantilla7_concat_ano.loc[:, COD_LOC] = base_plantilla7_concat_ano[COD_CLI_ORIG]
base_plantilla7_concat_ac_trm_mes.loc[:, COD_LOC] = base_plantilla7_concat_ac_trm_mes[
    COD_CLI_ORIG
]
# Eliminar columnas innecesarias

base_plantilla7_agrup_ano = Group_by_and_sum_cols(
    df=base_plantilla7_concat_ano,
    group_col=dict_mcp7["COLS_AGRUP_P7_META_INVER"],
    sum_col=dict_mcp7["COLS_SUM_P7"],
)

base_plantilla7_agrup_ac_trm_mes = Group_by_and_sum_cols(
    df=base_plantilla7_concat_ac_trm_mes,
    group_col=dict_mcp7["COLS_AGRUP_P7_META_INVER"],
    sum_col=dict_mcp7["COLS_SUM_P7"],
)

base_plantilla7_insumo_def = concatenar_dataframes(
    df_list=[
        base_plantilla7_agrup_ano,
        base_plantilla7_agrup_ac_trm_mes,
    ]
)
print("Exportación resultados plajtilla N°7 ... ")
exportar_a_excel(
   df=base_plantilla7_insumo_def,
   ruta_guardado=RESULTADOS_P7,
   nom_hoja=dc_ptlls["Plantilla7"]["noms"][0],
)
