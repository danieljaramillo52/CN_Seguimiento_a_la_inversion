# Tomamos las bases generadas en Plantilla_2_P1 Y Plantilla2_2_P2 y construimos la Parte_3
from General_Functions import Lectura_insumos_excel, exportar_a_excel
from Plantilla_2_p1 import base_plant_2_def
from Plantilla_2_p2 import BD_Gastos_Def

from Transformation_functions import (
    Unidecode_solo_cols_df,
    pd_left_merge,
    pd_left_merge_two_keys,
    reemplazar_indices,
    Renombrar_columnas_con_diccionario,
    rellenar_columnas_nulas,
    Reemplazar_columna_en_funcion_de_otra,
    Agregar_columna_constante,
    Eliminar_acentos,
    Eliminar_filas_duplicadas_por_columnas,
    Eliminar_columnas,
)
import config_constans as cc

config = cc.config
dict_meses = config["dict_meses"]
cols_fillna_plantillas = config["cols_fillna_plantillas"]

MES = cc.MES
COD_CLI_ORG = cc.COD_CLI_ORIG
COD_CLI = cc.COD_CLI
CANAL_TIP = cc.CANAL_TIP
RAZON_SOCIAL = cc.RAZON_SOCIAL
SUB_CANAL_TIP = cc.SUB_CANAL_TIP
FIL_NUMERAL = cc.FIL_NUMERAL
FIL_GUION = cc.FIL_GUION
FIL_NO = cc.FIL_NO
FIL_SIN_ASIG = cc.FIL_SIN_ASIG
RESULTADOS_P2 = cc.RESULTADOS_P2
SECTOR = cc.SECTOR
SEGMENTO = cc.SEGMENTO
SUB_CANAL_TIP_ORIG = cc.SUB_CANAL_TIP_ORIG
COLS_PARA_MERGE_PLANTILLA_2_FINAL = cc.COLS_PARA_MERGE_PLANTILLA_2_FINAL


driver_de_segmento = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=cc.RUTA_DRIVERS,
        nom_insumo=cc.NOM_DRIVER_USUARIO,
        nom_Hoja=cc.DRIVERS_USUARIO[7],
        cols=4,
    )
)

# Lectura Driver_plantilla_2 == Archivo generado en Plantilla_2_p2
driver_plantilla_2 = Unidecode_solo_cols_df(Lectura_insumos_excel(
    path=config["path"]["Resultados"]["RESULTADOS_P1"],
    nom_insumo=config["Plantillas"]["Plantilla1"]["noms"][0] + ".xlsx",
    nom_Hoja=config["Plantillas"]["Plantilla1"]["noms"][0],
    cols=22,
))
# Eliminamos duplicados.
driver_plantilla_2 = Eliminar_filas_duplicadas_por_columnas(
    dataframe=driver_plantilla_2, columnas=COD_CLI
)
# Crear copias para no alterar las bases originales.
base_plant_2_def_copy = base_plant_2_def.copy()
bd_gastos_def_copy = BD_Gastos_Def.copy()

# Vamos a agregar las columnas para construir el insumo final de la plantilla_2.

# COLUMNA COD_CLIENTE

# 1.) Traemos el segundo grupo por la llave COD_CLIENTE
base_plant_2_def_copy_merge1 = pd_left_merge_two_keys(
    base_left=base_plant_2_def_copy,
    base_right=bd_gastos_def_copy[COD_CLI],
    left_key=COD_CLI_ORG,
    right_key=COD_CLI,
)
Contador = 0
for cada_col in ['Reintegro Logistico', 'OIPV + Mercaderismo', 'Trade', 'PAC']:
    Contador +=  base_plant_2_def_copy[cada_col].astype(float).sum()
    
# 2.) Renombrar la columna COD_ORIG en Plantilla_2_P2, para poder traerla nuevamente por otra llave con un merge.
bd_gastos_def_copy = Renombrar_columnas_con_diccionario(
    base=bd_gastos_def_copy, cols_to_rename={COD_CLI_ORG: f"{COD_CLI_ORG}_1"}
)

# 3.) Traemos el segundo grupo por la llave COD_CLIENTE_ORIG
base_plant_2_def_copy_merge2 = pd_left_merge_two_keys(
    base_left=base_plant_2_def_copy_merge1,
    base_right=bd_gastos_def_copy[f"{COD_CLI_ORG}_1"],
    left_key=COD_CLI_ORG,
    right_key=f"{COD_CLI_ORG}_1",
)

# 4.) Seleccionamos los indices nulos.
indices_nulos_cod_cli = base_plant_2_def_copy_merge2[
    base_plant_2_def_copy_merge2[COD_CLI].isnull()
].index

# 5.) Reemplazar los indices nulos del primer merge con los indices del primer merge para esta columna.
base_plant_2_def_copy_merge2 = reemplazar_indices(
    df=base_plant_2_def_copy_merge2,
    columna_a_reemplazar=COD_CLI,
    columna_con_valores=f"{COD_CLI_ORG}_1",
    indices=indices_nulos_cod_cli,
)

# 6.) Reemplazar los indices aún nulos.
indices_nulos_restantes = base_plant_2_def_copy_merge2[
    base_plant_2_def_copy_merge2[COD_CLI].isnull()
].index

base_plant_2_def_copy_merge2 = reemplazar_indices(
    df=base_plant_2_def_copy_merge2,
    columna_a_reemplazar=COD_CLI,
    columna_con_valores=COD_CLI_ORG,
    indices=indices_nulos_restantes,
)

#Eliminar columna ya innecesaria
#Eliminar columnas adicionales innecesarias
base_plant_2_def_copy_merge2 = Eliminar_columnas(
    df=base_plant_2_def_copy_merge2, columnas_a_eliminar=[f"{COD_CLI_ORG}_1"]
)
# COLUMNAS N.I.F , RAZON_SOCIAL , NOMBRE_COMERCIAL, COD_JV, JEFE DE VENTAS, COD_RV, VENDEDOR, COD_OV, OFICINA DE VENTAS, REGION, CANAL EST, SUBCANAL EST, *CANAL TIP, *SUBCANAL TIP, *SEGMENTO , AGRUPACION CLIENTES, FORMATO, PLAN DE NEGOCIO, SOCIOS, ESCALAS.

# * COLS => *CANAL TIP, *SUBCANAL TIP, *SEGMENTO (Solo en el caso de estas columnas traemos el primer grupo, luego existen segundos grupos para reemplazar los datos restantes)

# Todas las columnas anteriores están guardadas en la lista COLS_PARA_MERGE_PLANTILLA_2_FINAL
bd_gastos_def_copy_filtrada = bd_gastos_def_copy[COLS_PARA_MERGE_PLANTILLA_2_FINAL]

# Eliminamos duplicados previo al merge.
base_plant_2_def_copy_sin_duplicados = base_plant_2_def_copy_merge2.drop_duplicates()

# bd_gastos_def_copy_filtrada.drop_duplicates(subset="Cod. Cl")
base_plant_2_def_agrup = pd_left_merge(
    base_left=base_plant_2_def_copy_sin_duplicados,
    base_right=bd_gastos_def_copy_filtrada.drop_duplicates(subset=COD_CLI),
    key=COD_CLI,
)

# Reemplazar los valores.
# * COLS => *CANAL TIP, *SUBCANAL TIP, *SEGMENTO
# Tomemos un diccionario de valores de referencia.

DICT_CANAL = driver_de_segmento.set_index(SUB_CANAL_TIP_ORIG)[CANAL_TIP].to_dict()
DICT_SUBCANAL = driver_de_segmento.set_index(SUB_CANAL_TIP_ORIG)[
    SUB_CANAL_TIP
].to_dict()
DICT_SEGMENTO = driver_de_segmento.set_index(SUB_CANAL_TIP_ORIG)[SEGMENTO].to_dict()

base_plant_2_def_agrup = Reemplazar_columna_en_funcion_de_otra(
    df=base_plant_2_def_agrup,
    nom_columna_a_reemplazar=CANAL_TIP,
    nom_columna_de_referencia=SUB_CANAL_TIP_ORIG,
    mapeo=DICT_CANAL,
)
base_plant_2_def_agrup = Reemplazar_columna_en_funcion_de_otra(
    df=base_plant_2_def_agrup,
    nom_columna_a_reemplazar=SUB_CANAL_TIP,
    nom_columna_de_referencia=SUB_CANAL_TIP_ORIG,
    mapeo=DICT_SUBCANAL,
)
base_plant_2_def_agrup = Reemplazar_columna_en_funcion_de_otra(
    df=base_plant_2_def_agrup,
    nom_columna_a_reemplazar=SEGMENTO,
    nom_columna_de_referencia=SUB_CANAL_TIP_ORIG,
    mapeo=DICT_SEGMENTO,
)

base_plant_2_def_agrup = Agregar_columna_constante(
    dataframe=base_plant_2_def_agrup, nombre_col_cols=SECTOR, valor_constante="Compañia"
)

# Rellenar datos faltantes columnas.

# cols para rellenar con "sin asignar".
base_plant_2_def_agrup_mod1 = rellenar_columnas_nulas(
    df=base_plant_2_def_agrup,
    columna=cols_fillna_plantillas["cols_fillna_plantilla2"]["cols_sin_Asig"],
    valor=FIL_SIN_ASIG,
)
# cols para rellenar con "#".
base_plant_2_def_agrup_mod1 = rellenar_columnas_nulas(
    df=base_plant_2_def_agrup,
    columna=cols_fillna_plantillas["cols_fillna_plantilla2"]["cols_numeral"],
    valor=FIL_NUMERAL,
)
# cols para rellenar con "-".
base_plant_2_def_agrup_mod2 = rellenar_columnas_nulas(
    df=base_plant_2_def_agrup_mod1,
    columna=cols_fillna_plantillas["cols_fillna_plantilla2"]["cols_guion"],
    valor=FIL_GUION,
)
# cols para rellenar con "No".
base_plant_2_def_agrup_mod3 = rellenar_columnas_nulas(
    df=base_plant_2_def_agrup_mod2,
    columna=cols_fillna_plantillas["cols_fillna_plantilla2"]["cols_No"],
    valor=FIL_NO,
)
base_plant_2_def_agrup_def = Eliminar_acentos(base_plant_2_def_agrup_mod3)

# Exportar resultados.
exportar_a_excel(
    ruta_guardado=RESULTADOS_P2,
    df=base_plant_2_def_agrup_def,
    nom_hoja=config["Plantillas"]["Plantilla2"]["noms"][1],
)
