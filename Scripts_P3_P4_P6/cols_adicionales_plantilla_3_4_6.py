# Columnas adicionales. calculadas cols 3,4,6
import pandas as pd
from numpy import array
from loguru import logger
from General_Functions import Lectura_insumos_excel, exportar_a_excel
from Transformation_functions import (
    pd_left_merge,
    pd_left_merge_two_keys,
    reemplazar_indices,
    rellenar_columnas_nulas,
    Filtrar_por_valores_excluidos,
    crear_pivot_table,
    # filtrar_por_valores,
    # concatenar_dataframes,
    # Unidecode_solo_cols_df,
    Renombrar_columnas_con_diccionario,
    Agregar_columna_constante,
    Group_by_and_sum_cols,
    Eliminar_columnas,
    obtener_valores_unicos_sin_nulos,
    Cambiar_tipo_dato_multiples_columnas,
    establecer_periodo,
    Eliminar_filas_duplicadas_por_columnas,
    seleccionar_columnas,
    Duplicar_muliples_columnas,
)
from limpieza_plantilla_3_4_6 import (
    base_pto_inver_x_cliente_copy,
    driver_plantilla_2,
    driver_dcto_envio_directo,
    p3_historico_act_sin_dup,
    bd_plantilla6_rename,
    NOM_COL_P6,
)

import config_constans as cc

config = cc.config

MES = cc.MES
ESCALAS = cc.ESCALAS
ESCALA = cc.ESCALA
SECTOR = cc.SECTOR
COD_OV = cc.COD_OV
OFICINA_VENTAS = cc.OFICINA_VENTAS
COD_CLI = cc.COD_CLI
COD_MT = cc.COD_MT
ENVIO_DIRECTO = cc.ENVIO_DIRECTO
COD_CLI_ORG = cc.COD_CLI_ORIG
FIL_NUMERAL = cc.FIL_NUMERAL
FIL_SIN_ASIG = cc.FIL_SIN_ASIG
SUB_CANAL_TIP = cc.SUB_CANAL_TIP


COLS_PARA_MERGE_PLANTILLA_2_FINAL = cc.COLS_PARA_MERGE_PLANTILLA_2_FINAL
COLS_AGRUP_PLANTILLA_3_4_TD_NOTAS_CREDITO = cc.COLS_AGRUP_PLANTILLA_3_4_TD_NOTAS_CREDITO
COLS_AGRUP_PLANTILLA_3_4_TD_NOTAS_CREDITO_AGRUP_CONCEP = (
    cc.COLS_AGRUP_PLANTILLA_3_4_TD_NOTAS_CREDITO_AGRUP_CONCEP
)
COLS_FOR_AGRUP_PLN4 = cc.COLS_FOR_AGRUP_PLN4
COLS_FOR_SUM_PLN4 = cc.COLS_FOR_SUM_PLN4
VALS_FILTRAR_PLAN4 = cc.VALS_FILTRAR_PLAN4
COLS_RENOMBRAR_PLANTILLA_3_ACT = cc.COLS_RENOMBRAR_PLANTILLA_3_ACT

CONCEPTO = config["CONCEPTO"]
VALOR = config["VALOR"]
CANAL_INV = config["CANAL_INV"]
VENTA_CONTADO = cc.VENTA_CONTADO
DEV_MALAS = cc.DEV_MALAS
DICT_MESES = config["dict_meses"]
AGRUPACION_CONCEPTO = config["AGRUPACION_CONCEPTO"]
CODIGO = cc.CODIGO
PORCENTAJE_VENTAS_CONTADO_P6 = cc.PORCENTAJE_VENTAS_CONTADO_P6
MODS_COLS_PLANT_7 = config["MODS_COLS_PLANT_7"]
COLS_FINALES_PLN4 = config["COLS_FINALES_PLN4"]

RUTAS_INSUMOS = config["RUTAS_INSUMOS"]
RESULTADOS_P3 = cc.RESULTADOS_P3
RESULTADOS_P4 = cc.RESULTADOS_P4
RESULTADOS_P6 = cc.RESULTADOS_P6
RESULTADOS_P7 = cc.RESULTADOS_P7
dc_cols_duplicar_pln4 = cc.COLS_DUPLICAR_PLN4
dc_ptlls = config["Plantillas"]
cols_fillna_plantillas = config["cols_fillna_plantillas"]

# Procesamos la plantilla N°7 para completarla.
base_plantilla7_def = Lectura_insumos_excel(
    path=f"{RESULTADOS_P7}",
    nom_insumo=dc_ptlls["Plantilla7"]["noms"][0] + ".xlsx",
    nom_Hoja=dc_ptlls["Plantilla7"]["noms"][0],
    cols=dc_ptlls["Plantilla7"]["cols"][0],
)

# Eliminamos duplicados.
driver_plantilla_2 = Eliminar_filas_duplicadas_por_columnas(
    dataframe=driver_plantilla_2, columnas=COD_CLI
)
# Traer la columna código de cliente identico a la forma de la plantilla2, para las plantillas 3 y 4

contador = 0
bases_merge_P3_P4_P6_P7 = [
    p3_historico_act_sin_dup,
    base_pto_inver_x_cliente_copy,
    bd_plantilla6_rename,
    base_plantilla7_def,
]
for cada_plantilla in bases_merge_P3_P4_P6_P7:
    # COLUMNA COD_CLIENTE

    # 1.) Traemos el segundo grupo por la llave COD_CLIENTE
    cada_plantilla = pd_left_merge_two_keys(
        base_left=cada_plantilla,
        base_right=driver_plantilla_2[COD_CLI],
        left_key=COD_CLI_ORG,
        right_key=COD_CLI,
    )

    # 2.) Renombrar la columna COD_ORIG en Plantilla_2_P2, para poder traerla nuevamente por otra llave con un merge.
    driver_plantilla_2 = Renombrar_columnas_con_diccionario(
        base=driver_plantilla_2, cols_to_rename={COD_CLI_ORG: f"{COD_CLI_ORG}_1"}
    )

    # 3.) Traemos el segundo grupo por la llave COD_CLIENTE_ORIG
    cada_plantilla = pd_left_merge_two_keys(
        base_left=cada_plantilla,
        base_right=driver_plantilla_2[f"{COD_CLI_ORG}_1"],
        left_key=COD_CLI_ORG,
        right_key=f"{COD_CLI_ORG}_1",
    )

    # 4.) Seleccionamos los indices nulos.
    indices_nulos_cod_cli = cada_plantilla[cada_plantilla[COD_CLI].isnull()].index

    # 5.) Reemplazar los indices nulos del primer merge con los indices del primer merge para esta columna.
    cada_plantilla = reemplazar_indices(
        df=cada_plantilla,
        columna_a_reemplazar=COD_CLI,
        columna_con_valores=f"{COD_CLI_ORG}_1",
        indices=indices_nulos_cod_cli,
    )

    # 6.) Reemplazar los indices aún nulos.
    indices_nulos_restantes = cada_plantilla[cada_plantilla[COD_CLI].isnull()].index

    cada_plantilla = reemplazar_indices(
        df=cada_plantilla,
        columna_a_reemplazar=COD_CLI,
        columna_con_valores=COD_CLI_ORG,
        indices=indices_nulos_restantes,
    )

    # Traer el resto de las columnas.
    # COLUMNAS N.I.F , RAZON_SOCIAL , NOMBRE_COMERCIAL, COD_JV, JEFE DE VENTAS, COD_RV, VENDEDOR, COD_OV, OFICINA DE VENTAS, REGION, CANAL EST, SUBCANAL EST, CANAL TIP, SUBCANAL TIP, SEGMENTO , AGRUPACION CLIENTES, FORMATO, PLAN DE NEGOCIO, SOCIOS, ESCALAS.

    Driver_plantilla_2_seleccionado = driver_plantilla_2[
        COLS_PARA_MERGE_PLANTILLA_2_FINAL
    ]

    if contador == 1:
        cada_plantilla = cada_plantilla.drop(columns=[COD_OV, OFICINA_VENTAS])

    cada_plantilla = pd_left_merge(
        base_left=cada_plantilla,
        base_right=Driver_plantilla_2_seleccionado,
        key=COD_CLI,
    )

    if contador != 2:
        cada_plantilla = Agregar_columna_constante(
            dataframe=cada_plantilla,
            nombre_col_cols=SECTOR,
            valor_constante="Compañia",
        )

    cada_plantilla[COLS_PARA_MERGE_PLANTILLA_2_FINAL] = cada_plantilla[
        COLS_PARA_MERGE_PLANTILLA_2_FINAL
    ].fillna(FIL_NUMERAL)

    bases_merge_P3_P4_P6_P7[contador] = cada_plantilla
    contador = contador + 1

bd_notas_credito = bases_merge_P3_P4_P6_P7[0]
bd_vtas_dctos = bases_merge_P3_P4_P6_P7[1]
bd_plt6_ed_dm_cont = bases_merge_P3_P4_P6_P7[2]
bd_plt7_met_inv = bases_merge_P3_P4_P6_P7[3]

# Eliminar columnas innecesarias. Plantillas 3 y 6
bd_notas_credito_select = Eliminar_columnas(
    df=bd_notas_credito, columnas_a_eliminar=[f"{COD_CLI_ORG}_1"]
)
bd_vtas_dctos_select = Eliminar_columnas(
    df=bd_vtas_dctos, columnas_a_eliminar=[f"{COD_CLI_ORG}_1"]
)
bd_p6_ed_dm_cont_select = Eliminar_columnas(
    df=bd_plt6_ed_dm_cont, columnas_a_eliminar=[f"{COD_CLI_ORG}_1"]
)
bd_plt7_met_inv_select = Eliminar_columnas(
    df=bd_plt7_met_inv, columnas_a_eliminar=[f"{COD_CLI_ORG}_1"]
)


# Trasponer los valores de la columna columna  AGRUPACION_CONCEPTO. PASOS
# Usaremos la columna valor que contiene los el valor asociado a cada fila de AGRUPACION_ CONCEPTO

# PASO: 0
# Trasformar columna valor en flotante.
bd_notas_credito_select = Cambiar_tipo_dato_multiples_columnas(
    base=bd_notas_credito_select, list_columns=[VALOR], type_data=float
)
# PASO: 1
lista_cols_p3 = COLS_AGRUP_PLANTILLA_3_4_TD_NOTAS_CREDITO

# Asignar el valor de escala, a la última columna
lista_cols_p3[-1] = ESCALA

# Creación de la tabla dinámica
bd_notas_credito_select = bd_notas_credito_select.rename(columns={ESCALAS: ESCALA})

# Tratamos con "sin asignar" los conceptos que estan vacios luego del merge 
bd_notas_credito_select[[CONCEPTO, AGRUPACION_CONCEPTO]] = bd_notas_credito_select[[CONCEPTO, AGRUPACION_CONCEPTO]].fillna(FIL_SIN_ASIG)

# Replicamos la tabla dinamica de excel necesaria.
pivot_notas_credito = crear_pivot_table(
    df=bd_notas_credito_select,
    lista_columnas=lista_cols_p3,
    columna_agrupacion=AGRUPACION_CONCEPTO,
    columna_valor=VALOR,
    fill_value = 0
)
# Eliminar el indice ya tomado.
base_td_notas_credito = pivot_notas_credito.reset_index(drop=True)

# Rellenar los valores nulos con 0s.
# Extraemos las columnas de los conceptos necesarios
lista_conceptos = obtener_valores_unicos_sin_nulos(
    columna=bd_notas_credito_select[AGRUPACION_CONCEPTO]
)

base_td_notas_credito.loc[:, lista_conceptos] = base_td_notas_credito.loc[
    :, lista_conceptos
].fillna(0)


## Agregar a la plantilla6 las columnas Envio directo y contado.
bd_p6_ed_dm_cont_select = pd_left_merge(
    base_left=bd_p6_ed_dm_cont_select,
    base_right=driver_dcto_envio_directo[[COD_MT, "% Dcto"]],
    key=COD_MT,
)

# COLUMNA ENVIO_DIRECTO. (Plantilla6)
bd_p6_ed_dm_cont_select[ENVIO_DIRECTO] = bd_p6_ed_dm_cont_select[
    "Venta Env. Dir."
].astype(float) * bd_p6_ed_dm_cont_select["% Dcto"].infer_objects(copy=False).fillna(0)

# Cambiar tipo de dato de la columna Venta Contado para hacer la operación.
bd_p6_ed_dm_cont_select = Cambiar_tipo_dato_multiples_columnas(
    base=bd_p6_ed_dm_cont_select, list_columns=[VENTA_CONTADO], type_data=float
)

# COLUMNA CONTADO (Plantilla6)
bd_p6_ed_dm_cont_select["Contado"] = (
    bd_p6_ed_dm_cont_select[VENTA_CONTADO].astype(float) * PORCENTAJE_VENTAS_CONTADO_P6
)

# Filtrar tiendas del subcanal tip. Plantilla6
# bd_p6_ed_dm_cont_filtrada = Filtrar_por_valores_excluidos(
#    df=bd_p6_ed_dm_cont_select,
#    columna=SUB_CANAL_TIP,
#    valores_excluir=VALS_FILTRAR_PLAN4,
# )
# Vamos a establecer el periodo correspondiente, para las plantillas 3, 4, y 6.

# td_notas_credito_def_filtrado = Filtrar_por_valores_excluidos(
#    df=td_notas_credito_def, columna=SUB_CANAL_TIP, valores_excluir=VALS_FILTRAR_PLAN4
# )

base_td_notas_credito_rename = Renombrar_columnas_con_diccionario(
    base=base_td_notas_credito, cols_to_rename={ESCALAS: ESCALA, ESCALA: ESCALAS}
)
base_td_notas_credito_rename = establecer_periodo(
    df=base_td_notas_credito_rename, dict_meses=DICT_MESES, hay_trim=config["hay_trim"], mes_act=config["mes_act"]
)

# Plantilla 4 ( no contiene meses necesarios contiene información de Año (únicamente))
# td_vtas_dctos_mod_filtrado = Filtrar_por_valores_excluidos(
#    df=bd_vtas_dctos_select, columna=SUB_CANAL_TIP, valores_excluir=VALS_FILTRAR_PLAN4
# )

# Cambiar tipo de dato de las columnas para sumar.
td_vtas_dctos_mod_filtrado = Cambiar_tipo_dato_multiples_columnas(
    base=bd_vtas_dctos_select, list_columns=COLS_FOR_SUM_PLN4, type_data=float
)
td_vtas_dctos_agrup = Group_by_and_sum_cols(
    df=td_vtas_dctos_mod_filtrado,
    group_col=COLS_FOR_AGRUP_PLN4,
    sum_col=COLS_FOR_SUM_PLN4,
)
# Plantilla4 (Duplicar columnas "Ppto $$"" y "Ppto Dctos" como cols => "Ppto $$ Anual" y "Ppto Dctos Anual")
td_vtas_dctos_agrup = Duplicar_muliples_columnas(
    df=td_vtas_dctos_agrup,
    columnas_a_duplicar=dc_cols_duplicar_pln4,
)
# Plantilla4 (Agregar col acumulado.)
td_vtas_dctos_def = Agregar_columna_constante(
    dataframe=td_vtas_dctos_agrup, nombre_col_cols=MES, valor_constante="Acum"
)
td_vtas_dctos_def_reorder = seleccionar_columnas(
    dataframe=td_vtas_dctos_def, cols_elegidas=COLS_FINALES_PLN4
)
td_vtas_dctos_def_reorder.loc[:,MES] = "Año"

# Plantilla6 Establecer periodo (Acum, Trim, Mes act)
bd_p6_ed_dm_cont_periodo = establecer_periodo(
    df=bd_p6_ed_dm_cont_select, dict_meses=DICT_MESES, hay_trim=config["hay_trim"], mes_act=config["mes_act"]
)
# Agrupar la plantilla6.
bd_p6_ed_dm_cont_periodo[[ENVIO_DIRECTO, DEV_MALAS, "Contado"]] = bd_p6_ed_dm_cont_periodo[[ENVIO_DIRECTO, DEV_MALAS, "Contado"]].astype(float)

bd_p6_ed_dm_cont_agrup = Group_by_and_sum_cols(
    df=bd_p6_ed_dm_cont_periodo,
    group_col=NOM_COL_P6["COLS_AGRUP_PLANTILLA_6_TD_ED_D_M"],
    sum_col=[ENVIO_DIRECTO, DEV_MALAS, "Contado"],
)


# Plantilla 7 tratamiento de datos nulls.
bd_plt7_met_inv_mods = rellenar_columnas_nulas(
    df=bd_plt7_met_inv_select,
    columna=cols_fillna_plantillas["cols_fillna_plantilla7"]["cols_numeral"],
    valor=FIL_NUMERAL,
)

# Bases de la plantilla N°4
exportar_a_excel(
    df=td_vtas_dctos_def_reorder,
    ruta_guardado=RESULTADOS_P4,
    nom_hoja=dc_ptlls["Plantilla4"]["noms"][1],
    index=False
)

## Bases de la plantilla N°3
exportar_a_excel(
    df=base_td_notas_credito_rename,
    ruta_guardado=RESULTADOS_P3,
    nom_hoja=dc_ptlls["Plantilla3"]["noms"][1],
    index=False
)

exportar_a_excel(
    df=bd_p6_ed_dm_cont_agrup,
    ruta_guardado=RESULTADOS_P6,
    nom_hoja=dc_ptlls["Plantilla6"]["noms"][1],
    index=False
)
# Bases de la plantilla N°7
exportar_a_excel(
    df=bd_plt7_met_inv_mods,
    ruta_guardado=RESULTADOS_P7,
    nom_hoja=dc_ptlls["Plantilla7"]["noms"][1],
    index=False
)
