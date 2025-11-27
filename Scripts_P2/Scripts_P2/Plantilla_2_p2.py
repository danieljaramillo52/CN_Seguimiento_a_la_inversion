# Lectrua del insumo para parte de la plantilla 2.
from Transformation_functions import (
    Agregar_filas_no_existentes,
    Agregar_columna_constante,
    Unidecode_solo_cols_df,
    Reemplazar_columna_con_valor_constante
)
from General_Functions import Lectura_insumos_excel,exportar_a_excel
import pandas as pd
import config_constans as cc

config = cc.config

COD_CLI = cc.COD_CLI
RUTA_DRIVERS = cc.RUTA_DRIVERS
RUTA_INSUMOS_P2 = cc.RUTA_INSUMOS_P2
RESULTADOS_P1 = cc.RESULTADOS_P1
RESULTADOS_P2 = cc.RESULTADOS_P2


# Lectura Anexos.
# Nombre de la base
BASE_ANEXOS = cc.BASE_ANEXOS
NOM_BASE_ANEXOS = cc.NOM_BASE_ANEXOS
NOM_BASE_MES_ANTERIOR = cc.NOM_BASE_MES_ANTERIOR

# Nombres de las hojas
NOM_HOJA_BASE_ANEXOS = cc.NOM_HOJA_BASE_ANEXOS
NOM_HOJA_BASE_MES_ANTERIOR = cc.NOM_HOJA_BASE_MES_ANTERIOR

# Numero de columnas
COLS_ANEXOS = cc.COLS_ANEXOS
COLS_BASE_MES_ANTERIOR = cc.COLS_BASE_MES_ANTERIOR

REORDENAR_COLUMNAS_ANEXOS = cc.REORDENAR_COLUMNAS_ANEXOS
# orden columnas anexo.

# Base Bo Mes anterior = (Insumo de la automatización/actualizada al final)
base_bo_ant = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_INSUMOS_P2,
        nom_insumo=NOM_BASE_MES_ANTERIOR,
        nom_Hoja=NOM_HOJA_BASE_MES_ANTERIOR,
        cols=COLS_BASE_MES_ANTERIOR,
    )
)


# Base Bo Mes actual = (Importada de base_bo)
# Base Bo Mes anterior = (Insumo de la automatización)
# Insumo de clientes actualizado
base_bo_act = Unidecode_solo_cols_df(pd.read_excel(RESULTADOS_P1 + "TD - Datos Cliente.xlsx", dtype=str))

base_bo_act = Agregar_columna_constante(
    dataframe=base_bo_act, nombre_col_cols="Vigencia", valor_constante="Si"
)
base_bo_ant = Agregar_columna_constante(
    dataframe=base_bo_ant, nombre_col_cols="Vigencia", valor_constante="No"
)

base_ant_act_mod = Agregar_filas_no_existentes(
    df1=base_bo_act, df2=base_bo_ant, columna_id=COD_CLI
)

# Agregar anexo a la base completa.
BD_Gastos_Def = base_ant_act_mod 

#Exportar base_ant_act actualizada. 
exportar_a_excel(
    ruta_guardado=RUTA_INSUMOS_P2,
    df=base_ant_act_mod,
    nom_hoja=config["Plantillas"]["Plantilla2"]["noms"][3],
)