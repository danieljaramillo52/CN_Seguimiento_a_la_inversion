# Creacion de la parte2 de la plantilla2.
from General_Functions import Lectura_insumos_excel, exportar_a_excel
import pandas as pd
from Transformation_functions import (
    Renombrar_columnas_con_diccionario,
    Unidecode_solo_cols_df,
    Eliminar_primeros_n_caracteres,
    Cambiar_tipo_dato_multiples_columnas,
    conservar_primeros_n_caracteres,
    Formatear_primera_letra_mayuscula,
    pd_left_merge,
    Group_by_and_sum_cols,
    filtrar_por_valores,
    concatenar_dataframes,
    establecer_periodo,
    Lista_unidecode,
)

import config_constans as cc

config = cc.config
dict_meses = config["dict_meses"]
RESULTADOS_P2 = cc.RESULTADOS_P2
RUTA_INSUMOS_P2 = cc.RUTA_INSUMOS_P2
RUTA_DRIVERS = cc.RUTA_DRIVERS
NOM_DRIVER_USUARIO = cc.NOM_DRIVER_USUARIO
DRIVERS_USUARIO = cc.DRIVERS_USUARIO
RENAME_COLS_AFO_CONSUL_INV = cc.RENAME_COLS_AFO_CONSUL_INV
NOM_AFO_CONSUL_INV = cc.NOM_AFO_CONSUL_INV
NOM_HISTORI_BD_GASTOS = cc.NOM_HISTORI_BD_GASTOS

NOM_HOJA_AFO_CONSUL_INV = cc.NOM_HOJA_AFO_CONSUL_INV
NOM_HOJA_HISTORI_BD_GASTOS = cc.NOM_HOJA_HISTORI_BD_GASTOS

COLS_AFO_CONSUL_INV = cc.COLS_AFO_CONSUL_INV
COLS_HISTORI_BD_GASTOS = cc.COLS_HISTORI_BD_GASTOS

MES = cc.MES
COD_CLI_ORG = cc.COD_CLI_ORIG
CONCEPTO = cc.CONCEPTO
REAL_ACT = cc.REAL_ACT
COD_CECO = cc.COD_CECO
COLS_AGRUP_CONS_GASTOS = cc.COLS_AGRUP_CONS_GASTOS
COLS_AGRUP_CONS_GASTOS_CONCEPTO = cc.COLS_AGRUP_CONS_GASTOS_CONCEPTO
COLS_AGRUP_PLANT2_P1 = cc.COLS_AGRUP_PLANT2_P1
COLS_SUM_PLANT2_P1 = cc.COLS_SUM_PLANT2_P1

config_insumos2 = config["Insumos_P2"]

afo_consul_inver_cli = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=config["path"]["Insumos_P2"],
        nom_insumo=config_insumos2["Afo_consulta_inversion_cliente"]["file_name"],
        nom_Hoja=config_insumos2["Afo_consulta_inversion_cliente"]["sheet"],
        cols=config_insumos2["Afo_consulta_inversion_cliente"]["cols"],
    )
)

driver_cecos_conceptos = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[6],
        cols=2,
    )
)

histo_bd_gast = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_INSUMOS_P2,
        nom_insumo=NOM_HISTORI_BD_GASTOS,
        nom_Hoja=NOM_HOJA_HISTORI_BD_GASTOS,
        cols=COLS_HISTORI_BD_GASTOS,
    )
)

afo_consul_inver_cli_rename = Renombrar_columnas_con_diccionario(
    base=afo_consul_inver_cli, cols_to_rename=RENAME_COLS_AFO_CONSUL_INV
)
afo_consul_inver_cli_rename = afo_consul_inver_cli_rename.drop(
    afo_consul_inver_cli_rename.index[:1]
)

afo_consul_inver_cli_rename = Eliminar_primeros_n_caracteres(
    df=afo_consul_inver_cli_rename, columna=COD_CECO, n=5
)

afo_consul_inver_cli_concepto = pd_left_merge(
    base_left=afo_consul_inver_cli_rename,
    base_right=driver_cecos_conceptos,
    key=COD_CECO,
)

# Transformar columna Real en floatante.
afo_consul_inver_cli_concepto[REAL_ACT] = afo_consul_inver_cli_concepto[
    REAL_ACT
].astype(float)

# Eliminar valores nulos.
afo_consul_inver_cli_concepto_filtrada = afo_consul_inver_cli_concepto.dropna(
    subset=[CONCEPTO]
)
lista_conceptos = list(afo_consul_inver_cli_concepto_filtrada[CONCEPTO].unique())

# Generar la nueva consulta.
consul_gasto_cli = afo_consul_inver_cli_concepto.groupby(
    COLS_AGRUP_CONS_GASTOS_CONCEPTO,
    as_index=False,
)[REAL_ACT].sum()

bases_concepto = []
for i in list(lista_conceptos):
    base_por_concepto = filtrar_por_valores(consul_gasto_cli, CONCEPTO, [i])
    base_por_concepto_rename = base_por_concepto.rename(columns={REAL_ACT: i})
    base_por_concepto_drop = base_por_concepto_rename.drop(columns=[CONCEPTO])
    bases_concepto.append(base_por_concepto_drop)

consul_gasto_cli = afo_consul_inver_cli_concepto.groupby(
    COLS_AGRUP_CONS_GASTOS,
    as_index=False,
)[REAL_ACT].sum()

for cada_base in bases_concepto:
    consul_gasto_cli = pd.merge(
        consul_gasto_cli,
        cada_base,
        on=COLS_AGRUP_CONS_GASTOS,
        how="left",
    )

consul_gasto_cli[lista_conceptos] = consul_gasto_cli[lista_conceptos].fillna(0)

# Modificar las columana "Mes"
consul_gasto_cli = conservar_primeros_n_caracteres(
    df=consul_gasto_cli, columna=MES, n=3
)
consul_gasto_cli = Formatear_primera_letra_mayuscula(
    dataframe=consul_gasto_cli, columna=MES
)
# Pasar a unicode el formato de las posibles nuevas cadenas.
consul_gasto_cli = Unidecode_solo_cols_df(consul_gasto_cli)

# Seleccionar y reordenar cols según el historico.
consul_gasto_cli = consul_gasto_cli[list(histo_bd_gast.columns)]

# Concatenar base final con el historico
base_plant_2_concat = concatenar_dataframes([histo_bd_gast, consul_gasto_cli])

# Agrupar y sumar conceptos.
cols_para_sum = Lista_unidecode(lista_conceptos)

base_plant_2_concat_mod = Cambiar_tipo_dato_multiples_columnas(
    base=base_plant_2_concat, list_columns=cols_para_sum, type_data=float
)

# Vamos a establecer los peridos (Acum, Mes, Trimestre)
base_plant_2_periodos = establecer_periodo(
    df=base_plant_2_concat_mod, dict_meses=dict_meses, hay_trim=config["hay_trim"], mes_act=config["mes_act"]
)
base_plant_2_def = Group_by_and_sum_cols(
    df=base_plant_2_periodos, group_col=COLS_AGRUP_PLANT2_P1, sum_col=cols_para_sum
)

# Exportar nuevo historico de gastos.
exportar_a_excel(
    ruta_guardado=RESULTADOS_P2,
    df=base_plant_2_concat_mod,
    nom_hoja=config["Plantillas"]["Plantilla2"]["noms"][2],
)

