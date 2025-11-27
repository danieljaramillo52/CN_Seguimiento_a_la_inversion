import sys
import pandas as pd
from os import path, getcwd, chdir


def agregar_columnas_faltantes(df, columnas_deseadas, dict_columnas):
    # Identificar las columnas faltantes
    columnas_faltantes = [col for col in columnas_deseadas if col not in df.columns]

    # Agregar las columnas faltantes al DataFrame con el valor constante definido en el diccionario
    for col in columnas_faltantes:
        df[col] = dict_columnas[col]

    return df


# Cambiar ruta de ejecución para python idle. (Comentar las siguinetes dos lineas si ejecuta desde visual)
current_dir = getcwd()
chdir(path.dirname(current_dir))

# Agregar rutas necesarias a la configuración.
parent_dir = path.dirname(path.dirname(path.abspath(__file__)))
sys.path.extend([f"{parent_dir}\\Utils", f"{parent_dir}\\Scripts"])

# Agregar rutas de carpetas de resultados.
for i in range(1, 8, 1):
    sys.path.extend([f"{parent_dir}\\Resultados/Plantilla_{i}/"])

# Importar librerias adicionales.
import config_constans as cc
import General_Functions as gf
import Transformation_functions as tf


# Cargar el archivo de configuración.
config = gf.Procesar_configuracion("config.yml")

# Extraer subconjuntos de información.
dict_noms_pltll = config["Plantillas"]
path_plantillas = config["path"]["Resultados"]


p1_td_dts_cli = tf.Unidecode_solo_cols_df(
    gf.Lectura_insumos_excel(
        path=path_plantillas["RESULTADOS_P1"],
        nom_insumo=dict_noms_pltll["Plantilla1"]["noms"][0] + ".xlsx",
        nom_Hoja=dict_noms_pltll["Plantilla1"]["noms"][0],
        cols=dict_noms_pltll["Plantilla1"]["cols"][0],
    )
)
p1_td_vts_dctos = gf.Lectura_insumos_excel(
    path=path_plantillas["RESULTADOS_P1"],
    nom_insumo=dict_noms_pltll["Plantilla1"]["noms"][1] + ".xlsx",
    nom_Hoja=dict_noms_pltll["Plantilla1"]["noms"][1],
    cols=dict_noms_pltll["Plantilla1"]["cols"][1],
)

p2_gtos_def = gf.Lectura_insumos_excel(
    path=path_plantillas["RESULTADOS_P2"],
    nom_insumo=dict_noms_pltll["Plantilla2"]["noms"][1] + ".xlsx",
    nom_Hoja=dict_noms_pltll["Plantilla2"]["noms"][1],
    cols=dict_noms_pltll["Plantilla2"]["cols"][1],
)

p3_td_ntas_cred = gf.Lectura_insumos_excel(
    path=path_plantillas["RESULTADOS_P3"],
    nom_insumo=dict_noms_pltll["Plantilla3"]["noms"][1] + ".xlsx",
    nom_Hoja=dict_noms_pltll["Plantilla3"]["noms"][1],
    cols=33,
)

p4_td_vtas_dctos = gf.Lectura_insumos_excel(
    path=path_plantillas["RESULTADOS_P4"],
    nom_insumo=dict_noms_pltll["Plantilla4"]["noms"][1] + ".xlsx",
    nom_Hoja=dict_noms_pltll["Plantilla4"]["noms"][1],
    cols=dict_noms_pltll["Plantilla4"]["cols"][1],
)

p6_td_ed_dm_cont = gf.Lectura_insumos_excel(
    path=path_plantillas["RESULTADOS_P6"],
    nom_insumo=dict_noms_pltll["Plantilla6"]["noms"][1] + ".xlsx",
    nom_Hoja=dict_noms_pltll["Plantilla6"]["noms"][1],
    cols=dict_noms_pltll["Plantilla6"]["cols"][1],
)
p7_met_inv_mods = gf.Lectura_insumos_excel(
    path=path_plantillas["RESULTADOS_P7"],
    nom_insumo=dict_noms_pltll["Plantilla7"]["noms"][1] + ".xlsx",
    nom_Hoja=dict_noms_pltll["Plantilla7"]["noms"][1],
    cols=dict_noms_pltll["Plantilla7"]["cols"][1],
)
p7_met_inv_mods_fil = tf.Filtrar_por_valores_excluidos(
    df=p7_met_inv_mods, columna="Canal inv", valores_excluir="Minimercados"
)

dict_nombres_plantillas = {
    "TD_P1": p1_td_vts_dctos,
    "TD_P2": p2_gtos_def,
    "TD_P3": p3_td_ntas_cred,
    "TD_P4": p4_td_vtas_dctos,
    "TD_P6": p6_td_ed_dm_cont,
    "TD_P7": p7_met_inv_mods_fil,
}

dict_plantillas_mod = dict_nombres_plantillas.copy()


# Lista de columnas deseadas
columnas_completas = config["cols_basicas_plantilla_final"].keys()
dict_columnas_finales = config["cols_basicas_plantilla_final"]

# Eliminar columna Dcto Merc Act. ( Plantilla 1 para evitar repetición.)
dict_plantillas_mod["TD_P1"] = tf.Eliminar_columnas(
    df=dict_plantillas_mod["TD_P1"], columnas_a_eliminar=["Dcto Merc. Act."]
)
# Agregar columna constante.
for nombre, plantilla in dict_plantillas_mod.items():
    dict_plantillas_mod[nombre] = tf.Agregar_columna_constante(
        dataframe=plantilla, nombre_col_cols="Plantilla", valor_constante=nombre
    )
    dict_plantillas_mod[nombre] = agregar_columnas_faltantes(
        df=dict_plantillas_mod[nombre],
        columnas_deseadas=columnas_completas,
        dict_columnas=dict_columnas_finales,
    )

    dict_plantillas_mod[nombre] = tf.Filtrar_por_valores_excluidos(
        df=dict_plantillas_mod[nombre],
        columna=cc.SUB_CANAL_TIP,
        valores_excluir=config["VALS_FILTRAR_PLAN4"],
    )
    dict_plantillas_mod[nombre] = tf.seleccionar_columnas(
        dataframe=dict_plantillas_mod[nombre],
        cols_elegidas=list(config["cols_basicas_plantilla_final"].keys()),
    )

base_completa = tf.concatenar_dataframes(df_list=dict_plantillas_mod.values())

# Cambiar tipo de dato columnas numericas.
base_completa = tf.Cambiar_tipo_dato_multiples_columnas(
    base=base_completa, list_columns=list(columnas_completas)[-39:], type_data=float
)

# Extraer bases
base_acum = tf.filtrar_por_valores(
    df=base_completa, columna=cc.MES, valores_filtrar=["Acum", "Año"]
)
base_mes = tf.filtrar_por_valores(
    df=base_completa, columna=cc.MES, valores_filtrar=[config["mes_act"], "Año"]
)
if "Trim" not in base_completa[cc.MES].unique():
    dict_final_dfs = {cc.MES: base_mes, "Acum": base_acum}
else:
    base_trim = tf.filtrar_por_valores(
        df=base_completa, columna=cc.MES, valores_filtrar=["Trim", "Año"]
    )
    dict_final_dfs = {cc.MES: base_mes, "Acum": base_acum, "Trim": base_trim}


print("Exportación de resultados finales...")
for sheet_name, dataframe in dict_final_dfs.items():
    gf.exportar_a_excel(
        ruta_guardado="Resultados/Plantilla_8/", df=dataframe, nom_hoja=f"_{sheet_name}"
    )
    print("Consolidado plantilla 8 de: ", sheet_name, " exportado exitosamente.")


print("Proceso finalizado.")
